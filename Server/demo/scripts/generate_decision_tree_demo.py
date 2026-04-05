#!/usr/bin/env python3
"""
生成决策树可视化 demo 数据。

生成的文件放在 demo/decision_trees/ 目录:
  models.json                — 可用模型列表（offline + online 按 query 名称）
  offline.json               — 离线模型
  online_{query_name}.json   — 在线模型快照（以触发训练的 query 命名）

用法:
  python3 scripts/generate_decision_tree_demo.py
"""

from __future__ import annotations

import json
import random
from pathlib import Path

DEMO_ROOT = Path(__file__).resolve().parent.parent
DT_DIR = DEMO_ROOT / "decision_trees"

SPLIT_CONDITIONS = {
    "FilterCol": [
        "[过滤列] 含列 cn.name 的过滤条件",
        "[过滤列] 含列 t.production_year 的过滤条件",
        "[过滤列] 含列 mi.info 的过滤条件",
        "[过滤列] 含列 k.keyword 的过滤条件",
        "[过滤列] 含列 n.name 的过滤条件",
    ],
    "FilterValue": [
        "[过滤值] 含谓词 cn.country_code = [us]",
        "[过滤值] 含谓词 cn.country_code = [jp]",
        "[过滤值] 含谓词 t.production_year > 2004.0",
        "[过滤值] 含谓词 t.production_year > 2008.0",
        "[过滤值] 含谓词 t.production_year >= 1978.0",
        "[过滤值] 含谓词 t.production_year <= 2013.0",
        "[过滤值] 含谓词 cn.name = Warner Bros. Entertainment",
        "[过滤值] 含谓词 cn.name = Sony Pictures Releasing",
        "[过滤值] 含谓词 k.keyword = marvel-cinematic-universe",
        "[过滤值] 含谓词 mi.info = USA",
        "[过滤值] 含谓词 it.info = release dates",
        "[过滤值] 含谓词 cn.name = Paramount Pictures",
    ],
    "FilterOp": [
        "[过滤操作] 含谓词 t.production_year >",
        "[过滤操作] 含谓词 t.production_year >=",
        "[过滤操作] 含谓词 t.production_year <=",
        "[过滤操作] 含谓词 cn.country_code =",
        "[过滤操作] 含谓词 cn.country_code <>",
    ],
    "JoinKey": [
        "[连接键] Join Key = _",
        "[连接键] Join Key = cn.id_mc.company_id",
        "[连接键] Join Key = t.id_mi.movie_id",
        "[连接键] Join Key = t.id_mk.movie_id",
        "[连接键] Join Key = n.id_ci.person_id",
    ],
    "JoinType": [
        "[连接类型] Join Type = Hash Join",
        "[连接类型] Join Type = Nested Loop",
        "[连接类型] Join Type = Merge Join",
    ],
    "ScanType": [
        "[扫描类型] Scan Type = Seq Scan",
        "[扫描类型] Scan Type = Index Scan",
        "[扫描类型] Scan Type = Bitmap Index Scan",
    ],
    "NumRange": [
        "[数值范围] 列 t.production_year <= 2005.0",
        "[数值范围] 列 t.production_year <= 1998.0",
        "[数值范围] 列 mi_idx.info <= 5.3",
    ],
}

CANDIDATE_SPLITS_POOL = [
    "[过滤值] 含谓词 cn.name = Fox",
    "[过滤值] 含谓词 cn.name = Miramax Films",
    "[过滤值] 含谓词 cn.name = Disney Channel",
    "[过滤值] 含谓词 cn.name = Paramount Pictures",
    "[过滤值] 含谓词 cn.name = Warner Home Video",
    "[过滤值] 含谓词 cn.name = Sony Pictures Releasing",
    "[过滤值] 含谓词 cn.country_code = [us]",
    "[过滤值] 含谓词 cn.country_code = [jp]",
    "[过滤值] 含谓词 cn.country_code = [gb]",
    "[过滤值] 含谓词 t.production_year > 2001.0",
    "[过滤值] 含谓词 t.production_year > 2008.0",
    "[过滤值] 含谓词 t.production_year <= 2005.0",
    "[过滤值] 含谓词 t.production_year >= 2007.0",
    "[过滤操作] 含谓词 t.production_year >",
    "[过滤操作] 含谓词 cn.country_code =",
    "[连接键] Join Key = cn.id_mc.company_id",
    "[连接键] Join Key = t.id_mi.movie_id",
    "[过滤列] 含列 t.production_year 的过滤条件",
    "[过滤列] 含列 k.keyword 的过滤条件",
]

STATIC_KEY_CONFIGS = {
    "company_name movie_companies": {
        "depth_range": (3, 5),
        "root_size": 69,
        "split_types": ["FilterCol", "FilterValue", "JoinKey", "FilterOp"],
    },
    "company_name movie_companies title": {
        "depth_range": (3, 5),
        "root_size": 151,
        "split_types": ["JoinKey", "FilterCol", "FilterOp", "FilterValue", "NumRange"],
    },
    "keyword movie_keyword title": {
        "depth_range": (2, 4),
        "root_size": 93,
        "split_types": ["FilterCol", "FilterValue", "JoinKey"],
    },
    "info_type movie_info title": {
        "depth_range": (2, 4),
        "root_size": 47,
        "split_types": ["FilterValue", "FilterCol", "FilterOp"],
    },
    "cast_info": {
        "depth_range": (1, 2),
        "root_size": 22,
        "split_types": ["ScanType", "FilterCol"],
    },
    "keyword movie_keyword": {
        "depth_range": (2, 3),
        "root_size": 58,
        "split_types": ["FilterCol", "FilterValue", "JoinKey", "FilterOp"],
    },
    "cast_info info_type movie_info title": {
        "depth_range": (2, 3),
        "root_size": 31,
        "split_types": ["FilterValue", "FilterCol", "JoinKey"],
    },
    "company_name company_type movie_companies title": {
        "depth_range": (2, 3),
        "root_size": 28,
        "split_types": ["JoinType", "FilterValue", "FilterOp"],
    },
}

# JOB benchmark query names used for online snapshots
ONLINE_QUERIES = [
    {"query_name": "1a", "training_samples": 28,  "keys_count": 3, "depth_adj": -1, "seed": 101},
    {"query_name": "5b", "training_samples": 62,  "keys_count": 4, "depth_adj": -1, "seed": 205},
    {"query_name": "8a", "training_samples": 95,  "keys_count": 5, "depth_adj":  0, "seed": 308},
    {"query_name": "13b","training_samples": 138, "keys_count": 6, "depth_adj":  0, "seed": 413},
    {"query_name": "17a","training_samples": 186, "keys_count": 7, "depth_adj":  0, "seed": 517},
    {"query_name": "21a","training_samples": 241, "keys_count": 8, "depth_adj":  0, "seed": 621},
]


def _gen_candidate_splits(rng: random.Random, total_size: int, n: int) -> list:
    pool = list(CANDIDATE_SPLITS_POOL)
    rng.shuffle(pool)
    splits = []
    for i in range(min(n, len(pool))):
        left = rng.randint(1, max(1, total_size - 1))
        right = total_size - left
        splits.append({
            "condition": pool[i],
            "left_size": left,
            "right_size": right,
            "score": round(left / total_size, 4),
        })
    return splits


def _gen_tree(rng: random.Random, size: int, depth: int, max_depth: int,
              split_types: list, variance_base: float) -> dict:
    mean = round(rng.gauss(2.0, 1.5), 4)
    variance = round(abs(rng.gauss(variance_base, variance_base * 0.4)), 4)
    half_range = max(0.3, abs(mean) * 0.8)
    min_r = round(mean - half_range + rng.uniform(-0.5, 0.0), 4)
    max_r = round(mean + half_range + rng.uniform(0.0, 0.5), 4)

    if depth >= max_depth or size < 8:
        n_cands = rng.randint(3, min(12, max(3, size)))
        return {
            "is_leaf": True,
            "size": size,
            "mean": mean,
            "min": min_r,
            "max": max_r,
            "variance": variance,
            "candidate_splits": _gen_candidate_splits(rng, size, n_cands),
        }

    st = rng.choice(split_types)
    conds = SPLIT_CONDITIONS[st]
    cond = rng.choice(conds)

    left_ratio = rng.uniform(0.25, 0.75)
    left_size = max(3, int(size * left_ratio))
    right_size = max(3, size - left_size)

    return {
        "is_leaf": False,
        "split_condition": cond,
        "split_type": st,
        "size": size,
        "mean": mean,
        "variance": variance,
        "left": _gen_tree(rng, left_size, depth + 1, max_depth, split_types,
                          variance_base * 0.7),
        "right": _gen_tree(rng, right_size, depth + 1, max_depth, split_types,
                           variance_base * 0.8),
    }


def gen_model(model_id: str, model_name: str, description: str,
              training_samples: int, seed: int,
              key_subset: list[str] | None = None,
              depth_adjust: int = 0) -> dict:
    rng = random.Random(seed)
    trees = {}

    keys = key_subset if key_subset else list(STATIC_KEY_CONFIGS.keys())
    for sk in keys:
        cfg = STATIC_KEY_CONFIGS.get(sk)
        if cfg is None:
            continue
        d_lo, d_hi = cfg["depth_range"]
        d_lo = max(1, d_lo + depth_adjust)
        d_hi = max(d_lo, d_hi + depth_adjust)
        max_depth = rng.randint(d_lo, d_hi)
        size_scale = training_samples / 495
        root_size = max(5, int(cfg["root_size"] * size_scale))
        trees[sk] = _gen_tree(rng, root_size, 0, max_depth,
                              cfg["split_types"], 3.0)

    return {
        "model_id": model_id,
        "model_name": model_name,
        "description": description,
        "training_samples": training_samples,
        "trees": trees,
    }


def write_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    print(f"Wrote {path}")


def main():
    DT_DIR.mkdir(parents=True, exist_ok=True)

    all_keys = list(STATIC_KEY_CONFIGS.keys())

    # --- models.json ---
    models_meta = [
        {
            "id": "offline",
            "type": "offline",
            "name": "Offline 模型",
            "description": "从离线训练数据集训练（495 条训练数据）",
            "training_samples": 495,
        },
    ]
    for q in ONLINE_QUERIES:
        models_meta.append({
            "id": f"online_{q['query_name']}",
            "type": "online",
            "query_name": q["query_name"],
            "name": q["query_name"],
            "description": f"在线增量训练 — 处理查询 {q['query_name']} 后的快照"
                           f"（累计 {q['training_samples']} 条训练数据）",
            "training_samples": q["training_samples"],
        })
    write_json(DT_DIR / "models.json", models_meta)

    # --- offline.json ---
    offline = gen_model("offline", "Offline 模型",
                        "从离线训练数据集训练（495 条训练数据）",
                        495, seed=42)
    write_json(DT_DIR / "offline.json", offline)

    # --- online_{query_name}.json ---
    for q in ONLINE_QUERIES:
        key_sub = all_keys[:q["keys_count"]]
        data = gen_model(
            f"online_{q['query_name']}",
            q["query_name"],
            f"在线增量训练 — 处理查询 {q['query_name']} 后的快照"
            f"（累计 {q['training_samples']} 条训练数据）",
            q["training_samples"],
            seed=q["seed"],
            key_subset=key_sub,
            depth_adjust=q["depth_adj"],
        )
        write_json(DT_DIR / f"online_{q['query_name']}.json", data)

    print("Decision tree demo data generated.")


if __name__ == "__main__":
    main()
