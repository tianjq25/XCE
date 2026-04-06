#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
export PYTHONPATH="${SCRIPT_DIR}/..${PYTHONPATH:+:$PYTHONPATH}"

dir=meta_models

mkdir -p $dir

log=$dir/train_imdb.log

python run_experiment.py --dataset imdb --generate_models --data_path datasets/imdb/{}.csv --model_path $dir > $log
