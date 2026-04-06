import pickle, time, os, json
from sql_executor import SQLExecutor
from docker_management import DockerFileHandler

def actual_time_one(method_path, method_name, dataset):
    executor = SQLExecutor(dataset=dataset, user="postgres", password="postgres", host="localhost", port=30010)
    docker_handler = DockerFileHandler(container_name_or_id="ce-benchmark-plus")
    test_sql_file = f"{dataset}_all_queries.pkl"
    with open(test_sql_file, "rb") as f:
        all_queries = pickle.load(f)

    with open(method_path, "r") as f:
        # write_str = f.readlines()
        write_str = f.read().strip()

    docker_handler.write_file(method_name, write_str)
    executor.set_optimize(method_name=method_name)

    planning_time = [] 
    execution_time = []

    cumulative_time = 0

    num_total_queries = len(all_queries)
    timeout_queries_no = []

    total_time_start = time.time()

    total_planning_time = 0.0
    total_execution_time = 0.0

    for i, q_name in enumerate(all_queries):

        # if q_name == "33c":
        #     continue

        query = all_queries[q_name]
        if dataset == "stack" and "q2" not in q_name and "q3" not in q_name:
            continue

        # print(q_name, query)

        print(f"Executing query {q_name}")
        start = time.time()

        res = None

        try:
            executor.cursor.execute("EXPLAIN ANALYZE " + query)
            res = executor.cursor.fetchall()
            planning_time.append(float(res[-2][0].split(":")[-1].split("ms")[0].strip()))
            execution_time.append(float(res[-1][0].split(":")[-1].split("ms")[0].strip()))
        except Exception as err:
            print(err)
            print("Type of error : ", type(err))
            timeout_queries_no.append(q_name)
            planning_time.append(0.0)
            execution_time.append(0.0)

        # true_plan = executor.convert_sql_to_plan_with_execute(query)
        # with open("our_plan_optimal.txt", "a") as f:
        #     print(json.dumps(true_plan), file = f)

        total_execution_time += execution_time[-1]
        total_planning_time += planning_time[-1]

        end = time.time()
        print(f"{q_name}-th query finished in {end-start} sec, with planning_time {planning_time[-1]} ms and execution_time {execution_time[-1]} ms" )
        # print expected time
        cumulative_time += (end - start)
        print("Total Cumulative Time (sec) :", cumulative_time)
        # print("Expected Remaining Time by Extrapolation (sec) :", (num_total_queries - i - 1) * (cumulative_time / (i + 1)))
        print("Timeout Queries Number:", timeout_queries_no)

    total_time_end = time.time()
    print(f"Total Time for executing {num_total_queries} queries: {cumulative_time} sec")
    print(f"Total Planning Time: {total_planning_time} ms")
    print(f"Total Execution Time: {total_execution_time} ms")

    executor.close()

    return cumulative_time, total_planning_time, total_execution_time

if __name__ == "__main__":
    dataset = "imdb"

    # method_name = "imdb_est_ASM.txt"
    method_name = "imdb_true_card.txt"

    method_path = f"{dataset}_est_files/{method_name}"

    cumulative_time_list = []
    for i in range(5):
        cumulative_time, _, _ = actual_time_one(method_path, method_name, dataset)
        cumulative_time_list.append(cumulative_time)
    print(f"Average Cumulative Time over 5 runs: {sum(sorted(cumulative_time_list)[1:-1]) / 3} sec")