import psycopg2
import os
import argparse

def send_query(dataset, method_name, query_file, save_folder, iteration=None):
    conn = psycopg2.connect(database=dataset, user="postgres", password="postgres", host="localhost", port=30010)
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()

    with open(query_file, "r") as f:
        queries = f.readlines()

    # if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
    #     os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

    # cursor.execute('SET debug_card_est=true')
    cursor.execute('SET print_sub_queries=true')
    cursor.execute('SET print_single_tbl_queries=true')

    for no, query in enumerate(queries):
        cursor.execute("EXPLAIN (FORMAT JSON)" + query.split("||")[0])
        res = cursor.fetchall()
        cursor.execute("SET query_no=0")
        print("%d-th query finished." % no)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default='stats', help='Which dataset to be used')
    parser.add_argument('--method_name', default='stats_CEB_sub_queries_model_stats_greedy_50.txt', help='save estimates')
    parser.add_argument('--query_file', default='/home/ubuntu/data_CE/stats_CEB/stats_CEB.sql', help='Query file location')
    parser.add_argument('--save_folder', default='/home/ubuntu/data_CE/stats_CEB/', help='Query file location')
    parser.add_argument('--iteration', type=int, default=None, help='Number of iteration to read')

    args = parser.parse_args()

    if args.iteration:
        for i in range(args.iteration):
            send_query(args.dataset, args.method_name, args.query_file, args.save_folder, i+1)
    else:
        send_query(args.dataset, args.method_name, args.query_file, args.save_folder, None)