#!/usr/bin/env python3
"""
从 ASM/datasets/imdb/<表名>.csv 读入**全部行**（不采样），生成 demo/tables/<表名>.json。
表名列表与 Web/src/api/mock.ts 中 AVAILABLE_TABLES 一致。

另生成 demo/response/<m>/<job_id>.json（与 views.py 约定一致）。

用法（在 Server/demo 下）:
  python3 scripts/generate_demo_data.py
  python3 scripts/generate_demo_data.py --max-rows 1000   # 仅调试时限制行数
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

DEMO_ROOT = Path(__file__).resolve().parent.parent
TABLES_DIR = DEMO_ROOT / "tables"
RESPONSE_ROOT = DEMO_ROOT / "response"
IMDB_ROOT = DEMO_ROOT.parent.parent / "ASM" / "datasets" / "imdb"

# 与 Web/src/api/mock.ts AVAILABLE_TABLES 完全一致（顺序一致便于对照）
IMDB_TABLES = [
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "person_info",
    "role_type",
    "title",
]


def _parse_cell(raw: str):
    if raw is None or raw == "":
        return None
    s = raw.strip()
    if s == "":
        return None
    try:
        if "." in s or "e" in s.lower():
            return float(s)
        return int(s)
    except ValueError:
        return raw


def _infer_col_types(rows: list[list], ncols: int) -> list[str]:
    types: list[str] = []
    for i in range(ncols):
        col_vals: list = []
        for r in rows:
            if i >= len(r):
                continue
            v = r[i]
            if v is None or v == "":
                continue
            col_vals.append(v)
        if not col_vals:
            types.append("str")
            continue
        if all(isinstance(x, int) for x in col_vals):
            types.append("int")
        elif all(isinstance(x, (int, float)) for x in col_vals):
            has_float = any(isinstance(x, float) for x in col_vals)
            types.append("double" if has_float else "int")
        else:
            types.append("str")
    return types


def read_imdb_table_csv(table: str, max_rows: int | None) -> dict:
    path = IMDB_ROOT / f"{table}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"找不到 CSV: {path}")

    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        header = [h.strip() for h in header]
        ncols = len(header)
        raw_rows: list[list] = []
        for i, row in enumerate(reader):
            if max_rows is not None and i >= max_rows:
                break
            padded = (row + [""] * ncols)[:ncols]
            raw_rows.append([_parse_cell(c) for c in padded])

    col_type = _infer_col_types(raw_rows, ncols)
    return {
        "col_name": header,
        "col_type": col_type,
        "rows": raw_rows,
    }


def write_imdb_tables(max_rows: int | None) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    for name in IMDB_TABLES:
        spec = read_imdb_table_csv(name, max_rows)
        path = TABLES_DIR / f"{name}.json"
        payload = {"table": spec}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.write("\n")
        n = len(spec["rows"])
        print(f"Wrote {path} ({n} rows)")


import random as _random

DEMO_ALIASES = ["t", "mi", "mk", "mc", "cn", "k"]


def _demo_true_cards(job_id: int) -> dict[str, int]:
    """Method-independent true cardinalities keyed by alias_key."""
    rng = _random.Random(job_id * 31)
    singles = {a: rng.randint(500, 5_000_000) for a in DEMO_ALIASES}
    pairs = {}
    for i in range(len(DEMO_ALIASES)):
        for j in range(i + 1, len(DEMO_ALIASES)):
            key = ",".join(sorted([DEMO_ALIASES[i], DEMO_ALIASES[j]]))
            pairs[key] = rng.randint(100, 2_000_000)
    triples = {
        ",".join(sorted(DEMO_ALIASES[:3])): rng.randint(50, 500_000),
        ",".join(sorted(DEMO_ALIASES[:4])): rng.randint(10, 100_000),
        ",".join(sorted(DEMO_ALIASES[:5])): rng.randint(5, 50_000),
        ",".join(sorted(DEMO_ALIASES)): rng.randint(1, 10_000),
    }
    return {**singles, **pairs, **triples}


def _demo_est_cards(true_cards: dict[str, int], method: int, job_id: int) -> dict[str, float]:
    """Per-method estimated cardinalities, with deliberate under/over-estimation."""
    rng = _random.Random(method * 10007 + job_id)
    result = {}
    for key, tc in true_cards.items():
        bias = 0.3 + method * 0.15 + rng.random() * 1.2
        result[key] = round(tc * bias, 2)
    return result


def demo_cardinality_map(
    true_cards: dict[str, int], est_cards: dict[str, float]
) -> dict[str, dict]:
    cmap: dict[str, dict] = {}
    for key in true_cards:
        cmap[key] = {
            "true_card": true_cards[key],
            "est_card": est_cards.get(key, true_cards[key]),
        }
    return cmap


def demo_tree(method: int, job_id: int) -> dict:
    base_ns = 500_000 + method * 100_000 + job_id * 1_000
    a, b, c, d = DEMO_ALIASES[0], DEMO_ALIASES[1], DEMO_ALIASES[2], DEMO_ALIASES[3]
    ab = ",".join(sorted([a, b]))
    abc = ",".join(sorted([a, b, c]))
    abcd = ",".join(sorted([a, b, c, d]))
    return {
        "Plan": {
            "time": int(base_ns * 1000),
            "alias_key": None,
            "children": {
                f"Hash Join [{abcd}]": {
                    "time": int(base_ns * 600),
                    "alias_key": abcd,
                    "children": {
                        f"Hash Join [{abc}]": {
                            "time": int(base_ns * 400),
                            "alias_key": abc,
                            "children": {
                                f"Hash Join [{ab}]": {
                                    "time": int(base_ns * 200),
                                    "alias_key": ab,
                                    "children": {
                                        f"Seq Scan [{a}]": {
                                            "time": int(base_ns * 80),
                                            "alias_key": a,
                                            "children": {},
                                        },
                                        f"Seq Scan [{b}]": {
                                            "time": int(base_ns * 70),
                                            "alias_key": b,
                                            "children": {},
                                        },
                                    },
                                },
                                f"Seq Scan [{c}]": {
                                    "time": int(base_ns * 60),
                                    "alias_key": c,
                                    "children": {},
                                },
                            },
                        },
                        f"Index Scan [{d}]": {
                            "time": int(base_ns * 50),
                            "alias_key": d,
                            "children": {},
                        },
                    },
                },
                "Aggregate": {
                    "time": int(base_ns * 100),
                    "alias_key": None,
                    "children": {},
                },
            },
        }
    }


def demo_job_payload(method: int, job_id: int) -> dict:
    latency_ms = 1.5 + method * 0.3 + (job_id % 17) * 0.02
    true_cards = _demo_true_cards(job_id)
    est_cards = _demo_est_cards(true_cards, method, job_id)
    return {
        "latency": round(latency_ms, 4),
        "pgm_detail_rows": [],
        "res_table": {
            "col_type": ["double", "str"],
            "rows": [
                [float(job_id), f"demo_row_m{method}_j{job_id}"],
            ],
        },
        "tree": demo_tree(method, job_id),
        "cardinality_map": demo_cardinality_map(true_cards, est_cards),
    }


def parse_range(s: str, default_lo: int, default_hi: int) -> tuple[int, int]:
    if not s:
        return default_lo, default_hi
    if "-" in s:
        a, b = s.split("-", 1)
        return int(a.strip()), int(b.strip())
    v = int(s.strip())
    return v, v


def write_job_responses(method_lo: int, method_hi: int, job_lo: int, job_hi: int) -> int:
    count = 0
    for m in range(method_lo, method_hi + 1):
        d = RESPONSE_ROOT / str(m)
        d.mkdir(parents=True, exist_ok=True)
        for j in range(job_lo, job_hi + 1):
            path = d / f"{j}.json"
            payload = demo_job_payload(m, j)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                f.write("\n")
            count += 1
    print(f"Wrote {count} files under {RESPONSE_ROOT}")
    return count


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--max-rows",
        type=int,
        default=None,
        metavar="N",
        help="仅读前 N 行（调试用）；不传则读入 CSV 全部行",
    )
    ap.add_argument("--sample", action="store_true", help="仅少量 JOB 文件")
    ap.add_argument("--methods", type=str, default="", help="如 0-4")
    ap.add_argument("--jobs", type=str, default="", help="如 1-113")
    args = ap.parse_args()

    if not IMDB_ROOT.is_dir():
        print(f"错误: IMDB 目录不存在: {IMDB_ROOT}")
        return 1

    if args.sample:
        ml, mh = 0, 2
        jl, jh = 1, 3
    else:
        ml, mh = parse_range(args.methods, 0, 4)
        jl, jh = parse_range(args.jobs, 1, 113)

    os.chdir(DEMO_ROOT)
    write_imdb_tables(args.max_rows)
    write_job_responses(ml, mh, jl, jh)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
