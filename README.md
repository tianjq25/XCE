# XCE

## Environment Setup

Ensure you have Anaconda or Miniconda installed on your linux machine.

1. Clone the repository

```Bash
git clone https://github.com/tianjq25/XCE.git
cd XCE
```

2. Create the Conda environment

We provide an `XCE.yml` file that handles the installation of all required pre-compiled binary packages, bypassing the need for local compilation. Run the following command th create the environment.

```Bash
conda env create -f XCE.yml
```

3. Activate the encironment

Once Conda has finshed downloading and extracting the packages, activate the environment:

```Bash
conda activate XCE
```

## Run the demo

To experience the full system, you need to start both the frontend interface and the backend API service separately. Please open two independent terminal windows and follow the steps below:

1. Start the Frontend Service

The frontend project requires Node.js. In your first terminal, navigate to the Web/ directory. We will use npm (Node Package Manager) to download all required frontend dependencies and then start the local development server:

```Bash
cd Web/
npm install
npm run dev
```

2. Start the Backend API Service

In your second terminal, navigate to the Server/backend/ directory. Make sure to activate the XCE Conda environment we configured earlier before starting the backend server:

```Bash
cd Server/backend/
conda activate XCE
python manage.py runserver 0.0.0.0:8010
```

3. Experience the Demo

Once both the frontend and backend services are successfully up and running, open your web browser and visit the following address to experience the demo: http://localhost:5173/

## Environment Setup for End-to-End Performance Measurement

> **Acknowledgment:** The instructions and configurations in this section are closely referenced and adapted from the [ASM](https://github.com/postechdblab/ASM). We sincerely thank the original authors for their foundational work and excellent documentation.

### Docker Setup for Hacked PostgreSQL

Please refer to https://github.com/Nathaniel-Han/End-to-End-CardEst-Benchmark for setting up the PostgreSQL v13.1 for measuring end-to-end performance. We've packaged all setups into a Docker image, including the PostgreSQL knob settings optimized for in-memory execution as mentioned in the paper.

```Bash
docker pull sigmod2024id403/pg13_hacked_for_ce_benchmark
docker run --name ce-benchmark -p 5432:5432 -v <path_to_asm>:/home -d sigmod2024id403/pg13_hacked_for_ce_benchmark
```

### Download and Import the Datasets

Original dataset link for IMDB-JOB: http://homepages.cwi.nl/~boncz/job/imdb.tgz

```Bash
cd <path_to_asm>/datasets
wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id=16Z35DYO-MfT_ipyNKSg6J21ZG40_LPgk' -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=16Z35DYO-MfT_ipyNKSg6J21ZG40_LPgk" -O imdb_dataset.zip && rm -rf /tmp/cookies.txt
unzip imdb_dataset.zip
rm imdb_dataset.zip
```

If you have any problem with downloading & unzipping the imdb_dataset.zip, please refer to the following Google drive link: https://drive.google.com/file/d/16Z35DYO-MfT_ipyNKSg6J21ZG40_LPgk/view

**Import IMDB-JOB into PostgreSQL Docker:**

This task should be run inside the ce-benchmark Docker container.

```Bash
psql -d postgres -U postgres
create database imdb;
\c imdb
\i /home/datasets/imdb/imdb_schema.sql
\i /home/datasets/imdb/imdb_load.sql
\i /home/datasets/imdb/imdb_index.sql
```

## Generate ASM Model 

> **Acknowledgment:** The instructions and configurations in this section are closely referenced and adapted from the [ASM](https://github.com/postechdblab/ASM). We sincerely thank the original authors for their foundational work and excellent documentation.

### Generate Meta Model

Use these scripts to generate a meta model for each dataset, which contains the schema information and global ordering of join keys. The meta models will be created in the `ASM/meta_models` directory (see inside the scripts). In addition, a directory will be created for each table in `ASM/datasets`, where each directory contains "table0.csv" that corresponds to the reordered table following the global order.

```Bash
bash ASM/generate_imdb_model.sh
```

### Train AR Models

Use these scripts to train the autoregressive (AR) models for each dataset. These models are trained over the reordered tables above. The AR models will be created in the `ASM/AR_models` directory (see inside the scripts). Furthermore, the AR models for the "*_type" tables of JOB and "site" table of Stack are dummies (not used in the estimation); following the implementation of FactorJoin (https://github.com/wuziniu/FactorJoin), we implement the per-table statistics estimation over the original table if the table has less than 1000 rows.

```Bash
bash ASM/safe_generate_imdb_ar.sh
```

### Estimate

Use these scripts to estimate the cardinalities of sub-queries of all queries for each dataset. Each script requires the directories for the meta model and AR models (see inside the scripts). The query-wise results will be stored in `ASM/job_CE/result.<query_name>` (e.g., job_CE/result.29b).

```Bash
bash ASM/evaluate_imdb_ar.sh
```

## Workload Augmentation

`gen_trainingset_parallel.py` generates training data for the calibrator by analyzing workloads. It compares ASM cardinality estimates against actual execution results and extracts (SQL, repair_value) pairs that reflect estimation errors.

### Usage

Before running, configure the database connection pools in the script (multiple PG Docker containers can be used for parallelism):

```Bash
python gen_trainingset_parallel.py
```

## Train Calibrator in Offline Scenario

`train_tree.py` trains the calibrator that learns to correct ASM cardinality estimation errors.

### Usage

```Bash
python train_tree.py
```

After training, the script prints each decision tree's structure (grouped by static join key), then runs ASM+Tree inference on test queries and writes the corrected estimates to `imdb_est.txt`.

## Online Evaluation

`online_evaluation.py` simulates a realistic online scenario where queries arrive sequentially, and the decision tree is incrementally trained from execution feedback.

### Usage

```Bash
python online_evaluation.py
```

## End-to-End Performance Measurement

The `evaluation/` directory contains scripts for measuring end-to-end query execution time on a separate PostgreSQL server, using the cardinality estimate files generated by the previous steps.

### Execution Scripts

All execution scripts inject cardinality estimates into the hacked PostgreSQL via Docker, then run queries with `EXPLAIN ANALYZE` to measure planning and execution time. Each script runs **5 repetitions** and reports the trimmed average (removing the best and worst runs).

| Script | Description |
|---|---|
| `send_query.py` | **Static benchmark**: Loads queries from a pickle file (`imdb_all_queries.pkl`) and executes them with a given estimate file. Used for methods like ASM, TrueCard, etc. |
| `send_online_query.py` | **Online scenario**: Reads queries from `job_test.txt`, shuffles them with seed 42 (matching `online_evaluation.py`'s order), and executes with online-generated estimates. |
| `send_online_query_get_plan.py` | **Plan collection**: Similar to `send_online_query.py`, but saves the full JSON execution plan for each query to `imdb_plans/<method_name>/<query_id>.json`. |
| `send_original_query.py` | **PostgreSQL baseline**: Runs queries without any estimate injection (using PostgreSQL's native optimizer) to establish the baseline execution time. |

### Log Processing and Visualization

| Script | Description |
|---|---|
| `handle_send_log.py` | Parses multi-run execution logs, computes trimmed-average runtime per query, and optionally compares two methods side-by-side with per-query improvement percentages. |
| `plot_eval_time.py` | Generates a grouped bar chart comparing end-to-end execution time across methods (ASM, ASM+XCE, TrueCard, Postgres) for the top-N slowest queries. Outputs PNG and PDF to `imdb_log/`. |
| `plot_cumulative_time.py` | Generates a cumulative runtime line chart comparing ASM vs. ASM+XCE over the sequence of online queries. Outputs PNG and PDF. |


