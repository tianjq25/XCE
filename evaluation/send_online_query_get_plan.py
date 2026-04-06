import pickle, time, os, json
from sql_executor import SQLExecutor
from docker_management import DockerFileHandler
import random

SEP = "#####"

def read_sqls(file_path):
    ids = []
    sqls = []
    with open(file_path) as f:
        line = f.readline()
        while line is not None and line != "":
            ids.append(line.split(SEP)[0])
            sqls.append(line.split(SEP)[1])
            line = f.readline()
    return ids, sqls

def shuffle_paired(ids, sqls):
    paired = list(zip(ids, sqls))
    rng = random.Random(42)
    rng.shuffle(paired)
    return [p[0] for p in paired], [p[1] for p in paired]

def actual_time_one(method_path, method_name, dataset):
    executor = SQLExecutor(dataset=dataset, user="postgres", password="postgres", host="localhost", port=30010)
    docker_handler = DockerFileHandler(container_name_or_id="ce-benchmark-plus")
    ids, sqls = read_sqls(f"job_test.txt")
    ids, sqls = shuffle_paired(ids, sqls)

    with open(method_path, "r") as f:
        write_str = f.read().strip()

    docker_handler.write_file(method_name, write_str)
    executor.set_optimize(method_name=method_name)

    for i, (q_id, query) in enumerate(zip(ids, sqls)):
        q_name = q_id.strip()
        print(f"Executing query {q_name}")
        try:
            executor.cursor.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + query)
            res = executor.cursor.fetchall()
            if not os.path.exists(f"{dataset}_plans/{method_name}"):
                os.makedirs(f"{dataset}_plans/{method_name}")
            with open(f"{dataset}_plans/{method_name}/{q_name}.json", "w") as f:
                f.write(json.dumps(res[0][0], indent=2))
        except Exception as e:
            print(e)

    executor.close()

if __name__ == "__main__":

    dataset = "imdb"

    method_name = "online_eval_XCE_estimates_test.txt"

    method_path = f"online_est_files/{method_name}"

    actual_time_one(method_path, method_name, dataset)