#!/usr/bin/env python3
"""
将 PostgreSQL EXPLAIN (ANALYZE, FORMAT JSON) 输出转为前端 ExecutionPlanTree 格式，
并构建 cardinality_map（alias_key → {true_card, est_card}）。

逻辑参照 online_evaluation.py 中的:
  - extract_actual_rows_from_plan  (plan 节点 → frozenset(aliases))
  - build_asm_estimate_map          (sub_plan_queries → frozenset(aliases) → est)

用法:
  # 单独转换一个 plan JSON
  python build_response_from_plan.py plan.json --est-file result.1a --true-file true_cardinality.1a

  # 批量生成 response JSON（需要 pkl 文件）
  python build_response_from_plan.py plan.json \\
      --sub-plan-pkl all_sub_plan_queries_str.pkl \\
      --q-name 1a \\
      --est-file Job_CE/result.1a \\
      --true-file Job_CE/true_cardinality.1a \\
      -o Server/demo/response/0/1.json
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
from typing import Optional


def _get_alias(node: dict) -> Optional[str]:
    return node.get("Alias") or node.get("Relation Name")


def pg_plan_to_tree(pg_node: dict) -> tuple[dict, set[str]]:
    """
    Recursively convert a PG EXPLAIN ANALYZE JSON node into the simplified
    frontend format: { "NodeName": { time, alias_key, children: {...} } }.

    Returns (simplified_tree_dict, set_of_aliases_in_subtree).
    The returned dict has exactly one key (the node display name).
    """
    node_type = pg_node.get("Node Type", "Unknown")
    actual_time = pg_node.get("Actual Total Time")
    time_ns = int(actual_time * 1_000_000) if actual_time is not None else None

    children_plans = pg_node.get("Plans", [])

    if "Scan" in node_type:
        alias = _get_alias(pg_node)
        alias_set = {alias} if alias else set()
        alias_key = alias if alias else None

        rel = pg_node.get("Relation Name", "")
        alias_label = _get_alias(pg_node) or ""
        display = f"{node_type} [{alias_label}]" if alias_label else node_type

        result_node: dict = {"alias_key": alias_key, "children": {}}
        if time_ns is not None:
            result_node["time"] = time_ns
        return {display: result_node}, alias_set

    if len(children_plans) == 1:
        child_tree, child_aliases = pg_plan_to_tree(children_plans[0])

        alias_key = ",".join(sorted(child_aliases)) if child_aliases else None
        display = node_type

        result_node = {"alias_key": alias_key, "children": child_tree}
        if time_ns is not None:
            result_node["time"] = time_ns
        return {display: result_node}, child_aliases

    if len(children_plans) == 2:
        left_tree, left_aliases = pg_plan_to_tree(children_plans[0])
        right_tree, right_aliases = pg_plan_to_tree(children_plans[1])
        combined = left_aliases | right_aliases

        alias_key = ",".join(sorted(combined)) if combined else None
        join_type = pg_node.get("Join Type", "")
        label_suffix = f" [{alias_key}]" if alias_key else ""
        display = f"{node_type}{label_suffix}"

        merged_children = {**left_tree, **right_tree}
        result_node = {"alias_key": alias_key, "children": merged_children}
        if time_ns is not None:
            result_node["time"] = time_ns
        return {display: result_node}, combined

    all_aliases: set[str] = set()
    merged_children: dict = {}
    for cp in children_plans:
        ct, ca = pg_plan_to_tree(cp)
        merged_children.update(ct)
        all_aliases |= ca

    alias_key = ",".join(sorted(all_aliases)) if all_aliases else None
    display = node_type

    result_node = {"alias_key": alias_key, "children": merged_children}
    if time_ns is not None:
        result_node["time"] = time_ns
    return {display: result_node}, all_aliases


def build_cardinality_map_from_files(
    sub_plan_queries: list,
    est_file: Path,
    true_file: Path,
) -> dict[str, dict]:
    """
    Build alias_key -> {true_card, est_card} from:
      - sub_plan_queries: list of (left_t, right_t) tuples
      - est_file: one float per line (estimated cardinalities)
      - true_file: one float per line (true cardinalities)
    """
    with open(est_file, "r") as f:
        est_cards = [float(line.strip()) for line in f if line.strip()]
    with open(true_file, "r") as f:
        true_cards = [float(line.strip()) for line in f if line.strip()]

    if len(est_cards) != len(sub_plan_queries):
        raise ValueError(
            f"est_file has {len(est_cards)} lines but sub_plan_queries has {len(sub_plan_queries)} entries"
        )
    if len(true_cards) != len(sub_plan_queries):
        raise ValueError(
            f"true_file has {len(true_cards)} lines but sub_plan_queries has {len(sub_plan_queries)} entries"
        )

    cmap: dict[str, dict] = {}
    for (left_t, right_t), est, true_c in zip(sub_plan_queries, est_cards, true_cards):
        aliases = sorted(right_t.strip().split(" ") + [left_t.strip()])
        key = ",".join(aliases)
        cmap[key] = {"true_card": true_c, "est_card": round(est, 2)}
    return cmap


def build_cardinality_map_simple(
    est_file: Path, true_file: Path
) -> dict[str, dict]:
    """
    Fallback when sub_plan_queries pkl is not available.
    Returns a map keyed by line index ("0", "1", ...).
    """
    with open(est_file, "r") as f:
        est_cards = [float(line.strip()) for line in f if line.strip()]
    with open(true_file, "r") as f:
        true_cards = [float(line.strip()) for line in f if line.strip()]

    cmap: dict[str, dict] = {}
    for i, (est, true_c) in enumerate(zip(est_cards, true_cards)):
        cmap[str(i)] = {"true_card": true_c, "est_card": round(est, 2)}
    return cmap


def build_full_response(
    pg_plan_json: dict,
    cardinality_map: dict[str, dict],
    latency: Optional[float] = None,
) -> dict:
    """Build the complete response JSON matching JoinQueryResponse."""
    raw_json = pg_plan_json[0] if isinstance(pg_plan_json, list) else pg_plan_json
    plan_node = raw_json.get("Plan", raw_json)

    tree, _ = pg_plan_to_tree(plan_node)

    exec_time = raw_json.get("Execution Time")
    if latency is None and exec_time is not None:
        latency = exec_time

    return {
        "latency": round(latency, 4) if latency is not None else 0.0,
        "pgm_detail_rows": [],
        "res_table": {"col_type": [], "rows": []},
        "tree": tree,
        "cardinality_map": cardinality_map,
    }


def _run_single(args) -> int:
    with open(args.plan_json, "r") as f:
        pg_plan = json.load(f)

    if args.sub_plan_pkl and args.q_name:
        with open(args.sub_plan_pkl, "rb") as f:
            all_sub = pickle.load(f)
        sub_plans = all_sub[args.q_name]
        cmap = build_cardinality_map_from_files(sub_plans, args.est_file, args.true_file)
    else:
        cmap = build_cardinality_map_simple(args.est_file, args.true_file)

    response = build_full_response(pg_plan, cmap, args.latency)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(response, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"Wrote {args.output}")
    else:
        print(json.dumps(response, indent=2, ensure_ascii=False))
    return 0


def _run_batch(args) -> int:
    """
    批量模式：从原始数据目录一次性生成所有 response JSON。

    目录结构约定:
      {raw_dir}/
        plans/                          — PG EXPLAIN (ANALYZE, FORMAT JSON) 输出
          {method_id}/{q_name}.json     — 每个 method、每条 query 一个文件
        estimates/                      — 基数估计文件
          {method_id}/result.{q_name}   — 每个 method 的预测基数
        true_cards/                     — 真实基数
          true_cardinality.{q_name}
        all_sub_plan_queries_str.pkl    — (可选) sub-plan queries 映射

    输出到 {output_dir}/{method_id}/{job_id}.json
    """
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.output) if args.output else raw_dir / "response"

    plans_dir = raw_dir / "plans"
    estimates_dir = raw_dir / "estimates"
    true_dir = raw_dir / "true_cards"
    sub_pkl = raw_dir / "all_sub_plan_queries_str.pkl"

    all_sub = None
    if sub_pkl.is_file():
        with open(sub_pkl, "rb") as f:
            all_sub = pickle.load(f)
        print(f"Loaded sub_plan_queries from {sub_pkl}")

    if not plans_dir.is_dir():
        print(f"Error: plans directory not found: {plans_dir}")
        return 1

    count = 0
    for method_dir in sorted(plans_dir.iterdir()):
        if not method_dir.is_dir():
            continue
        method_id = method_dir.name

        for plan_file in sorted(method_dir.glob("*.json")):
            q_name = plan_file.stem

            est_file = estimates_dir / method_id / f"result.{q_name}"
            true_file = true_dir / f"true_cardinality.{q_name}"

            if not est_file.is_file():
                print(f"  Skip {method_id}/{q_name}: est file missing ({est_file})")
                continue
            if not true_file.is_file():
                print(f"  Skip {method_id}/{q_name}: true file missing ({true_file})")
                continue

            with open(plan_file, "r") as f:
                pg_plan = json.load(f)

            if all_sub and q_name in all_sub:
                cmap = build_cardinality_map_from_files(
                    all_sub[q_name], est_file, true_file
                )
            else:
                cmap = build_cardinality_map_simple(est_file, true_file)

            response = build_full_response(pg_plan, cmap)

            out_path = out_dir / method_id / f"{q_name}.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(response, f, ensure_ascii=False, indent=2)
                f.write("\n")
            count += 1

    print(f"Generated {count} response files under {out_dir}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest="command")

    # single
    p_s = sub.add_parser("single", help="转换单个 plan JSON")
    p_s.add_argument("plan_json", type=Path, help="PG EXPLAIN (FORMAT JSON) 输出文件")
    p_s.add_argument("--est-file", type=Path, required=True)
    p_s.add_argument("--true-file", type=Path, required=True)
    p_s.add_argument("--sub-plan-pkl", type=Path, default=None)
    p_s.add_argument("--q-name", type=str, default=None)
    p_s.add_argument("--latency", type=float, default=None)
    p_s.add_argument("-o", "--output", type=Path, default=None)

    # batch
    p_b = sub.add_parser("batch", help="批量转换：从原始数据目录生成所有 response")
    p_b.add_argument("raw_dir", type=str, help="原始数据根目录")
    p_b.add_argument("-o", "--output", type=str, default=None, help="输出目录")

    args = ap.parse_args()

    if args.command == "single":
        return _run_single(args)
    elif args.command == "batch":
        return _run_batch(args)
    else:
        ap.print_help()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
