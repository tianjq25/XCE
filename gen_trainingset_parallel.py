import os, pickle, traceback, multiprocessing
os.environ['CUDA_VISIBLE_DEVICES'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'

from sql_parser import SQLParser
from sql_executor import SQLExecutor
from cache_db import SQLCacheDB
from RegressionFramework.utils import read_sqls
from reason_analyze import reason_sql_parallel
from gen_all_reason import gen_reason_sql
from run_ASM import load_bound_model, run_ASM_one, parse_sql_to_dict
from cardinality_extract import process_log_file
from docker_management import DockerFileHandler
from plan_handler import extract_join_order

from concurrent.futures import ProcessPoolExecutor

import sys
import ASM.AR.common 
sys.modules['common'] = ASM.AR.common

executor_config_dict = {
    "dataset": "imdb",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "30010",
    "timeout_seconds": 3600
}

docker_config_dict = {
    "container_name_or_id": "ce-benchmark"
}

sql_cache_config = {
    "db_name": "imdb_cache_dataset.db"
}

db_pool_configs = [
    {
        "executor_config": {
            "dataset": "imdb",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "30010",
            "timeout_seconds": 1800
        },
        "docker_config": {"container_name_or_id": "ce-benchmark"},
        "workers": 4,
    },
    {
        "executor_config": {
            "dataset": "imdb",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "30011",
            "timeout_seconds": 1800
        },
        "docker_config": {"container_name_or_id": "ce-benchmark-2"},
        "workers": 4,
    },
    {
        "executor_config": {
            "dataset": "imdb",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": "30012",
            "timeout_seconds": 1800
        },
        "docker_config": {"container_name_or_id": "ce-benchmark-3"},
        "workers": 4,
    },
]

import psutil
import gc
import torch

def print_memory_usage(prefix=""):
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / (1024 * 1024)
    print(f"[{prefix}] Current process memory usage: {mem_mb:.2f} MB", flush=True)

_worker_reason = None

def worker_init(executor_config, docker_config):
    global _worker_reason
    local_executor = SQLExecutor(**executor_config, docker_file_handler=DockerFileHandler(**docker_config))
    local_cache = SQLCacheDB(**sql_cache_config)
    local_parser = SQLParser()
    _worker_reason = reason_sql_parallel(local_parser, local_executor, local_cache)

def process_single_sql(cleaned_sql, all_sub_queries, cardinality, join_estimates, join_steps):
    try:
        _worker_reason.analyze_single_query(cleaned_sql, all_sub_queries, cardinality, join_estimates, join_steps)
    except Exception as e:
        print(f"Error analyzing query {cleaned_sql}: {e}")
        traceback.print_exc()
        # raise

if __name__ == "__main__":
    multiprocessing.set_start_method('forkserver')

    original_sqls_path = "Data/job4.txt"
    original_sqls = read_sqls(original_sqls_path)

    docker_handler = DockerFileHandler(**docker_config_dict)
    parser = SQLParser()
    executor = SQLExecutor(**executor_config_dict, docker_file_handler=docker_handler)
    cache = SQLCacheDB(**sql_cache_config)

    bound_ensemble_nc = load_bound_model(
        model_path="/home/tianjiaqi/data/Workspace/CardinalityEstimation/SQL_analyze/ASM/meta_models/model_imdb.pkl",
        ar_path="/home/tianjiaqi/data/Workspace/CardinalityEstimation/SQL_analyze/ASM/AR_models/{}-single-{}.tar",
        config_path="{}-single-{}_infer",
        sample_size=2048,
        dataset="imdb"
    )

    generator = gen_reason_sql(parser, executor, cache, bound_ensemble_nc)
    
    cleaned_sqls = []
    for original_sql in original_sqls:
        tables_dict, filters, joins = parser.parse(original_sql)
        cleaned_sqls.append(parser.reconstruct_sql(tables_dict, filters, joins))

    del original_sqls

    executor.send_subquery(cleaned_sqls)
    all_sub_queries = executor.read_subquery()

    out_file = "estimate_P.txt"
    join_estimates_dict = {}
    join_steps_dict = {}
    
    for idx, cleaned_sql in enumerate(cleaned_sqls):
        qname = f"query_{idx}"

        with open(out_file, 'a') as f:
            # print(f"evaluate_one_tree: {alias}, {table}", file=f)
            print(f'query: {qname}', file=f)

        query_predicate = parse_sql_to_dict(cleaned_sql)

        print(cleaned_sql)
        print(query_predicate)
        # print(all_sub_queries[qname])

        try:
            join_estimates = run_ASM_one(
                query=cleaned_sql,
                sub_plan=all_sub_queries[qname],
                query_predicate=query_predicate,
                bound_ensemble_nc=bound_ensemble_nc,
                get_Probability=out_file
            )
        except Exception as e:
            print(e)
            traceback.print_exc()
            continue

        write_str = ""
        method_name = "imdb_est.txt"
        for est in join_estimates:
            write_str += str(est) + "\n"
        docker_handler.write_file(method_name, write_str)
        executor.set_optimize(method_name)
        true_plan = executor.convert_sql_to_plan_without_execute(cleaned_sql)
        join_steps = extract_join_order(true_plan[0]['Plan'])
        executor.reset_optimize()
        print(join_steps)

        join_estimates_dict[qname] = join_estimates
        join_steps_dict[qname] = join_steps

        cardinality = process_log_file(out_file)
        q_record = None
        for record in cardinality:
            if record['query_id'] == qname:
                q_record = record
                break

        # try:
        #     generator.analyze_single_query(cleaned_sql, all_sub_queries[qname], q_record, join_estimates, join_steps)
        # except Exception as e:
        #     print(f"Error analyzing query {qname}: {e}")
        #     continue
    
    cardinality_list = process_log_file(out_file)
    cardinality_dict = {}
    if cardinality_list:
        cardinality_dict = {record['query_id']: record for record in cardinality_list}
    del cardinality_list

    executor.close()
    print_memory_usage("Before model destruction")

    print("Releasing ASM model resources...", flush=True)
    del bound_ensemble_nc
    del generator
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    print_memory_usage("After model destruction")

    total_workers = sum(cfg["workers"] for cfg in db_pool_configs)
    print(f"Starting parallel processing: {len(db_pool_configs)} databases, {total_workers} workers total...")

    task_args = []
    for idx, cleaned_sql in enumerate(cleaned_sqls):
        qname = f"query_{idx}"
        q_record = cardinality_dict.get(qname)
        # q_record = None
        # for record in cardinality:
        #     if record['query_id'] == qname:
        #         q_record = record
        #         break
        je = join_estimates_dict.get(qname, [])
        js = join_steps_dict.get(qname, [])
        task_args.append((cleaned_sql, all_sub_queries[qname], q_record, je, js))

    del cleaned_sqls, cardinality_dict, join_estimates_dict, join_steps_dict, all_sub_queries
    del parser, docker_handler, cache, executor
    gc.collect()
    print_memory_usage("After releasing intermediate data")

    # Assign tasks to each database in a round-robin manner
    # so that the workload is balanced across all databases
    db_task_buckets = [[] for _ in db_pool_configs]
    for i, args in enumerate(task_args):
        db_task_buckets[i % len(db_pool_configs)].append(args)

    futures = []
    executor_pools = []
    for db_idx, db_cfg in enumerate(db_pool_configs):
        pool = ProcessPoolExecutor(
            max_workers=db_cfg["workers"],
            initializer=worker_init,
            initargs=(db_cfg["executor_config"], db_cfg["docker_config"]),
        )
        executor_pools.append(pool)
        for args in db_task_buckets[db_idx]:
            cleaned_sql, sub_queries, q_record, je, js = args
            futures.append(pool.submit(
                process_single_sql,
                cleaned_sql, sub_queries, q_record, je, js,
            ))
        print(f"  DB {db_idx} (port {db_cfg['executor_config']['port']}): "
              f"{len(db_task_buckets[db_idx])} tasks, {db_cfg['workers']} workers", flush=True)

    del task_args, db_task_buckets
    gc.collect()

    for future in futures:
        try:
            future.result()
        except Exception as e:
            print(f"Task failed: {e}")

    for pool in executor_pools:
        pool.shutdown(wait=True)
