#!/usr/bin/env python3
"""
Convert RepariRegressionFramework pickle files to JSON for decision-tree visualization.

Logic references:
  - compare_decision_trees.py : _extract_tree_structure
  - RegressionFramework.py    : format_action_detail, print_tree

Usage:
  # 1) Convert offline model
  python convert_framework_to_json.py single framework.pkl \
      --model-id offline --model-name "Offline model" \
      -o ../decision_trees/offline.json

  # 2) Convert online snapshot (set query name / training samples via flags)
  python convert_framework_to_json.py single online_fw_after_1a.pkl \
      --model-id online_1a --model-name "1a" \
      --training-samples 35 \
      -o ../decision_trees/online_1a.json

  # 3) Batch: put multiple pkl files in one directory, auto-generate models.json
  python convert_framework_to_json.py batch ./snapshots/ \
      -o ../decision_trees/

batch_dir naming convention:
  offline.pkl               -> offline model
  online_{query_name}.pkl   -> online snapshot after {query_name}
"""

from __future__ import annotations

import argparse
import json
import os
import pickle
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
# print(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("CUDA_VISIBLE_DEVICES", "1")


def _lazy_imports():
    """Lazy-import heavy dependencies only when needed."""
    import ASM.AR.common
    sys.modules["common"] = ASM.AR.common

    from RegressionFramework.RegressionFramework import RepariRegressionFramework
    from RegressionFramework.NonShiftedModel.AdaptivaGroupTree import TreeNode
    return RepariRegressionFramework, TreeNode


def extract_tree_node(framework, node, depth=0) -> dict:
    """Recursively extract a TreeNode into the frontend JSON shape."""
    min_r, mean_r, max_r = node.confidence_range()
    var = node.variance()

    if node.is_leaf():
        candidate_splits = []
        for action in node.actions:
            try:
                left_n, right_n = action.fake_split(node)
                cond = framework.format_action_detail(action)
                candidate_splits.append({
                    "condition": cond,
                    "left_size": left_n.size(),
                    "right_size": right_n.size(),
                    "score": round(action.score, 4),
                })
            except Exception:
                pass

        return {
            "is_leaf": True,
            "size": node.size(),
            "mean": round(mean_r, 4),
            "min": round(min_r, 4),
            "max": round(max_r, 4),
            "variance": round(var, 4),
            "candidate_splits": candidate_splits,
        }

    cond = framework.format_action_detail(node.split_action)
    split_type = type(node.split_action).__name__

    result = {
        "is_leaf": False,
        "split_condition": cond,
        "split_type": split_type,
        "size": node.size(),
        "mean": round(mean_r, 4),
        "variance": round(var, 4),
    }

    if node.left_child:
        result["left"] = extract_tree_node(framework, node.left_child, depth + 1)
    if node.right_child:
        result["right"] = extract_tree_node(framework, node.right_child, depth + 1)

    return result


def framework_to_dict(
    framework,
    model_id: str,
    model_name: str,
    description: str = "",
    training_samples: int | None = None,
) -> dict:
    """Serialize RepariRegressionFramework to frontend DTDataResponse-shaped dict."""
    trees = {}
    for iod_model in framework.iod_models:
        for static_key, root in iod_model.key_to_static_root.items():
            trees[static_key] = extract_tree_node(framework, root)

    if training_samples is None:
        total = 0
        for iod_model in framework.iod_models:
            for root in iod_model.key_to_static_root.values():
                total = total + root.size()
        training_samples = total

    return {
        "model_id": model_id,
        "model_name": model_name,
        "description": description,
        "training_samples": training_samples,
        "trees": trees,
    }


def convert_single(args):
    _lazy_imports()

    pkl_path = Path(args.pkl_file)
    with open(pkl_path, "rb") as f:
        framework = pickle.load(f)

    data = framework_to_dict(
        framework,
        model_id=args.model_id,
        model_name=args.model_name or args.model_id,
        description=args.description or "",
        training_samples=args.training_samples,
    )

    out = Path(args.output) if args.output else pkl_path.with_suffix(".json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Wrote {out}  ({len(data['trees'])} trees)")
    return data


def convert_batch(args):
    _lazy_imports()

    batch_dir = Path(args.batch_dir)
    out_dir = Path(args.output) if args.output else batch_dir / "json"
    out_dir.mkdir(parents=True, exist_ok=True)

    pkl_files = sorted(batch_dir.glob("*.pkl"))
    if not pkl_files:
        print(f"No .pkl files found in {batch_dir}")
        return

    models_meta = []

    for pkl_path in pkl_files:
        stem = pkl_path.stem
        with open(pkl_path, "rb") as f:
            framework = pickle.load(f)

        if stem == "offline":
            mid = "offline"
            mtype = "offline"
            mname = "Offline model"
            desc = "Trained on offline training dataset"
            qname = None
        elif stem.startswith("online_"):
            qname = stem[len("online_"):]
            mid = stem
            mtype = "online"
            mname = qname
            desc = f"Online incremental training — snapshot after processing query {qname}"
        else:
            mid = stem
            mtype = "offline"
            mname = stem
            desc = ""
            qname = None

        data = framework_to_dict(framework, mid, mname, desc)
        out_path = out_dir / f"{mid}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        print(f"Wrote {out_path}  ({len(data['trees'])} trees)")

        meta: dict = {
            "id": mid,
            "type": mtype,
            "name": mname,
            "description": desc,
            "training_samples": data["training_samples"],
        }
        if qname:
            meta["query_name"] = qname
        models_meta.append(meta)

    models_path = out_dir / "models.json"
    with open(models_path, "w", encoding="utf-8") as f:
        json.dump(models_meta, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Wrote {models_path}  ({len(models_meta)} models)")


def main():
    ap = argparse.ArgumentParser(
        description="Convert RepariRegressionFramework pkl files to frontend JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    sub = ap.add_subparsers(dest="command")

    # single
    p_single = sub.add_parser("single", help="Convert a single pkl file")
    p_single.add_argument("pkl_file", type=str, help="Path to pkl file")
    p_single.add_argument("--model-id", required=True, help="Model id (e.g. offline, online_1a)")
    p_single.add_argument("--model-name", default=None, help="Display name")
    p_single.add_argument("--description", default="", help="Model description")
    p_single.add_argument("--training-samples", type=int, default=None)
    p_single.add_argument("-o", "--output", default=None, help="Output JSON path")

    # batch
    p_batch = sub.add_parser("batch", help="Convert all pkl files in a directory")
    p_batch.add_argument("batch_dir", type=str, help="Directory containing pkl files")
    p_batch.add_argument("-o", "--output", default=None, help="Output directory")

    args = ap.parse_args()

    if args.command == "single":
        convert_single(args)
    elif args.command == "batch":
        convert_batch(args)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
