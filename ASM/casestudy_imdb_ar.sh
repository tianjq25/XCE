#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONPATH="${SCRIPT_DIR}/..${PYTHONPATH:+:$PYTHONPATH}"

dir=job_CE

mkdir -p $dir

model_path=meta_models/model_imdb.pkl
ar_path=AR_models/{}-single-{}.tar
config_path={}-single-{}_infer

predicate=job_queries/predicate
query_file=job_queries/all_queries.pkl
sub_plan_file=job_queries/all_sub_plan_queries_str.pkl

save=$dir/result
log=$dir/evaluate.log

# python run_case_study.py --dataset imdb --evaluate --ar_path $ar_path --model_path $model_path --config_path $config_path --query_file $query_file  --query_sub_plan_file $sub_plan_file --save_folder $save --query_predicate_location $predicate
python -m debugpy --listen 5678 --wait-for-client run_case_study.py --dataset imdb --evaluate --ar_path $ar_path --model_path $model_path --config_path $config_path --query_file $query_file  --query_sub_plan_file $sub_plan_file --save_folder $save --query_predicate_location $predicate
