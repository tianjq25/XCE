import pickle, time, os, json
from sql_executor import SQLExecutor
from docker_management import DockerFileHandler

def actual_time_one():
    executor = SQLExecutor(dataset="imdb", user="postgres", password="postgres", host="localhost", port=30010)
    docker_handler = DockerFileHandler(container_name_or_id="ce-benchmark")
    test_sql_file = "imdb_all_queries.pkl"
    with open(test_sql_file, "rb") as f:
        all_queries = pickle.load(f)

    # ce_folder = "ASM/job_CE"
    # method_name = "imdb_est_origin.txt"

    planning_time = [] 
    execution_time = []

    cumulative_time = 0

    num_total_queries = len(all_queries)
    timeout_queries_no = []

    total_planing_time = 0.0
    total_execution_time = 0.0

    total_time_start = time.time()

    for i, q_name in enumerate(all_queries):
        query = all_queries[q_name]

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

        total_planing_time += planning_time[-1]
        total_execution_time += execution_time[-1]

        end = time.time()
        print(f"{q_name}-th query finished in {end-start} sec, with planning_time {planning_time[i]} ms and execution_time {execution_time[i]} ms" )
        # print expected time
        cumulative_time += (end - start)
        print("Total Cumulative Time (sec) :", cumulative_time)
        print("Expected Remaining Time by Extrapolation (sec) :", (num_total_queries - i - 1) * (cumulative_time / (i + 1)))
        print("Timeout Queries Number:", timeout_queries_no)

        # executor.reset_optimize()

    total_time_end = time.time()
    print(f"Total Time for executing {num_total_queries} queries: {total_time_end - total_time_start} sec")
    print(f"Total Planning Time: {total_planing_time} ms")
    print(f"Total Execution Time: {total_execution_time} ms")

    return cumulative_time, planning_time, execution_time

if __name__ == "__main__":
    cumulative_time_list = []
    for i in range(5):
        cumulative_time, _, _ = actual_time_one()
        cumulative_time_list.append(cumulative_time)
    print(f"Average Cumulative Time over 5 runs: {sum(sorted(cumulative_time_list)[1:-1]) / 3} sec")