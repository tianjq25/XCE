import os, pickle
os.environ['CUDA_VISIBLE_DEVICES'] = '1'
import sys

import math
import json
import numpy as np

from sql_executor import SQLExecutor
from RegressionFramework.Plan.Plan import json_str_to_json_obj
from RegressionFramework.utils import flat_depth2_list
from RegressionFramework.RegressionFramework import RepariRegressionFramework

import time
from sql_parser import SQLParser
from sql_executor import SQLExecutor
from run_ASM import load_bound_model, run_ASM_one
from docker_management import DockerFileHandler

import sys
import ASM.AR.common 
sys.modules['common'] = ASM.AR.common

def read_test_dataset(file_name):
    plans_for_queries = []
    with open(file_name, "r") as f:
        for line in f.readlines():
            plans = json.loads(line.strip().split("#####")[-1])
            # print(plans[0])
            plans = [json_str_to_json_obj(p) for p in plans]
            plans_for_queries.append(plans)
    return plans_for_queries

def read_train_dataset(file_name):
    pairs = []
    seen = set()
    with open(file_name, "r") as f:
        for line in f.readlines():
            sql, repair_value = line.strip().split("#####")
            if sql in seen:
                continue
            seen.add(sql)
            pairs.append((sql, float(repair_value)))
    sqls = [p[0] for p in pairs]
    repair_values = [p[1] for p in pairs]
    return sqls, repair_values

def convert_sql_to_plan(sqls, executor: SQLExecutor):
    plans_for_query = []
    for sql in sqls:
        # print(sql)
        plan = executor.convert_sql_to_plan_without_execute(sql)
        plans_for_query.append([json_str_to_json_obj(p) for p in plan])
    return plans_for_query

def _init_regression_framework(train_plans, plans_for_queries, train_sqls, db,
                                mode="static", config_dict=None, forest=100):
    return RepariRegressionFramework(train_plans, train_sqls, db, mode=mode,
                                    config_dict=config_dict, forest=forest, plans_for_queries=plans_for_queries)

def _add_plan_metric(plans_for_query, repair_values):
    for i, plans in enumerate(plans_for_query):
        rv = repair_values[i]
        for plan in plans:
            plan["metric"] = rv
            plan['predicate'] = rv

def train_tree(train_sqls, train_plan_for_query, db, config_dict=None, forest=100):
    # read training data
    train_plans = flat_depth2_list(train_plan_for_query)

    # building regression framework
    regression_framework = _init_regression_framework(train_plans, train_plan_for_query, train_sqls, db, config_dict=config_dict, forest=forest)
    regression_framework.build()

    return regression_framework

if __name__ == "__main__":
    train_sql_file = "training_dataset_imdb.txt"
    train_sqls, repair_values = read_train_dataset(train_sql_file)
    # print(train_sqls, repair_values)

    parser = SQLParser()
    
    executor = SQLExecutor(dataset = "imdb", user="postgres", password="postgres", host="localhost", port="30010")

    docker_handler = DockerFileHandler(container_name_or_id="ce-benchmark")
    train_plan_file = os.path.splitext(train_sql_file)[0] + "_plan.txt"
    if os.path.exists(train_plan_file):
        train_plan_for_query = []
        with open(train_plan_file, "r") as f:
            for line in f.readlines():
                plans = json.loads(line.strip())
                plans = [json_str_to_json_obj(p) for p in plans]
                train_plan_for_query.append(plans)
    else:
        train_plan_for_query = convert_sql_to_plan(train_sqls, executor)
        _add_plan_metric(train_plan_for_query, repair_values)
        with open(train_plan_file, "w") as f:
            for plan in train_plan_for_query:
                f.write(json.dumps(plan) + "\n")

    for plans in train_plan_for_query:
        for plan in plans:
            plan['metric'] = math.log(plan['metric'])  # Add small epsilon to avoid log(0)
            plan['predicate'] = math.log(plan['predicate'])  # Add small epsilon to avoid log(0)

    # print(train_plan_for_query[0])

    # print(train_plan_for_query)
    # op = ["=", "<", ">", "<=", ">=", "<>"]
    # ColHandler.set_executor(executor)
    # for query in train_sqls:
    #     tables, filters, joins = parser.parse(query)
    #     for table_alias in tables:
    #         if filters.get(table_alias):
    #             for filter_cond in filters[table_alias]:
    #                 if any(op_i in filter_cond for op_i in op):
    #                     col = filter_cond.split()[0].split('.')[-1]
    #                     table_name = tables[table_alias]
    #                     ColHandler.get_col_type(col, table_name, table_alias)

    # start_time = time.time()
    regression_framework = train_tree(train_sqls, train_plan_for_query, "imdb")
    # print(time.time()-start_time)
    # with open("offline.pkl", "wb") as f:
    #     pickle.dump(regression_framework, f)

    for static_key, root in regression_framework.iod_models[0].key_to_static_root.items():
        print(f"static_key: {static_key}, root size: {root.size()}, variance: {root.variance()}")
        regression_framework.print_tree(root)

    test_sql_file = "ASM/job_queries/all_queries.pkl"
    sub_plan_file="ASM/job_queries/all_sub_plan_queries_str.pkl"

    with open(test_sql_file, "rb") as f:
        all_queries = pickle.load(f)
    with open(sub_plan_file, "rb") as f:
        all_sub_plan_queries = pickle.load(f)

    query_predicate_location = "ASM/job_queries/predicate"
    bound_ensemble_nc = load_bound_model(
        model_path="/home/tianjiaqi/data/Workspace/CardinalityEstimation/SQL_analyze/ASM/meta_models/model_imdb.pkl",
        ar_path="/home/tianjiaqi/data/Workspace/CardinalityEstimation/SQL_analyze/ASM/AR_models/{}-single-{}.tar",
        config_path="{}-single-{}_infer",
        sample_size=2048,
        dataset="imdb"
    )

    for i, q_name in enumerate(all_queries):

        print(f'query: {q_name}, sub_query_len: {len(all_sub_plan_queries[q_name])}')

        if os.path.exists(query_predicate_location + '/' + q_name + '.pkl'):
            with open(query_predicate_location + '/' + q_name + '.pkl', "rb") as f:
                query_predicate = pickle.load(f)
        else:
            continue

        join_estimates = run_ASM_one(
            query=all_queries[q_name],
            sub_plan=all_sub_plan_queries[q_name],
            query_predicate=query_predicate,
            bound_ensemble_nc=bound_ensemble_nc,
            optimizer = (regression_framework, executor, parser)
        )
        print(f"len of join estimates: {len(join_estimates)}")

        with open(f'imdb_est.txt', "a") as f:
            for est in join_estimates:
                f.write(str(est) + "\n")
