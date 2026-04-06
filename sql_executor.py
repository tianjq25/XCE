import psycopg2
import time
import re
import portalocker

from docker_management import DockerFileHandler

class SQLExecutor:
    def __init__(self, dataset, user = "postgres", password = "postgres", host = "localhost", port = "5432", docker_file_handler : DockerFileHandler= None, timeout_seconds = 600):
        self.dataset = dataset
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.timeout_seconds = timeout_seconds
        self.conn = None
        self.cursor = None

        if docker_file_handler is None:
            self.docker_file_handler = DockerFileHandler(container_name_or_id="ce-benchmark")
        else:
            self.docker_file_handler = docker_file_handler

        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(
            database=self.dataset,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            client_encoding='utf8'
        )
        self.cursor = self.conn.cursor()

    def execute_query(self, query, timeout=True, time_out_seconds=None):
        """
        :param query: SQL statement
        :param timeout_seconds: Timeout limit (in seconds)
        """
        # start_time = time.time()

        try:
            if timeout:
                self.cursor.execute(f"SET statement_timeout = '{self.timeout_seconds if time_out_seconds is None else time_out_seconds}s'")

            print(f"Query: {query}")
            
            self.cursor.execute(query)

            return self.cursor.fetchall()

        except psycopg2.errors.QueryCanceled:
            print(f"Error: SQL execution timed out ({self.timeout_seconds if time_out_seconds is None else time_out_seconds} seconds), and has been stopped.")
            print(f"Query: {query}")
            self.conn.rollback()
            return None
            
        except Exception as e:
            print(f"SQL Execution Error: {e}")
            print(f"Query: {query}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            raise e
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def set_optimize(self, method_name = "imdb_est.txt"):
        self.cursor.execute("SET statement_timeout = 0; commit;")

        ##### ML METHOD OPTIONS #####

        self.cursor.execute("SET ml_joinest_enabled=true;")
        self.cursor.execute("SET join_est_no=0;")
        self.cursor.execute(f"SET ml_joinest_fname='{method_name}';")
            

        self.cursor.execute("SET enable_indexscan=on;")
        self.cursor.execute("SET enable_bitmapscan=on;")
        
        self.cursor.execute("SET enable_material=off;")

        self.cursor.execute("SET enable_hashjoin = on;")

    def reset_optimize(self):
        self.cursor.execute("SET ml_joinest_enabled=false;")

    def send_subquery(self, queries, start = 0):
        # with portalocker.Lock('join_est_record_job.txt.lock', timeout=60) as fh:
        # if os.path.exists("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt"):
        #     os.remove("/Users/hanyuxing/pgsql/13.1/data/join_est_record_job.txt")

        # cursor.execute('SET debug_card_est=true')
        self.docker_file_handler.clear_file('join_est_record_job.txt')
        self.cursor.execute('SET print_sub_queries=true;')
        # cursor.execute('SET print_single_tbl_queries=true')

        for no, query in enumerate(queries):
            if no < start:
                continue
            self.cursor.execute("EXPLAIN (FORMAT JSON) " + query)
            res = self.cursor.fetchall()
            self.cursor.execute("SET query_no=0;")
            print("%d-th query finished." % no)

        self.cursor.execute('SET print_sub_queries=false;')

    def get_subquery_without_archive(self, query):
        with portalocker.Lock('join_est_record_job.txt.lock', timeout=60) as fh:
            archive = self.docker_file_handler.read_file('join_est_record_job.txt')
            self.docker_file_handler.clear_file('join_est_record_job.txt')
            self.cursor.execute('SET print_sub_queries=true;')
            self.cursor.execute("SET query_no=0;")
            self.cursor.execute("EXPLAIN (FORMAT JSON) " + query)
            self.cursor.execute('SET print_sub_queries=false;')
            res = self.read_subquery()['query_0']
            self.docker_file_handler.write_file('join_est_record_job.txt', archive)

            return res

    def read_subquery(self):
        # with portalocker.Lock('join_est_record_job.txt.lock', timeout=60) as fh:
        self.cursor.execute("SELECT pg_read_file('join_est_record_job.txt');")
        res = self.cursor.fetchall()
        text = res[0][0]

        results = {}
    
        major_blocks = re.split(r'query:\s*0\s+', text)
        major_blocks = [block for block in major_blocks if block.strip()]

        for idx, major_block in enumerate(major_blocks):
            results[f'query_{idx}'] = []
            minor_blocks = re.split(r'query:\s*\d+', major_block)
            
            for minor_block in minor_blocks:
                if not minor_block.strip():
                    continue
                
                matches = re.findall(r'RELOPTINFO\s+\(([^)]+)\)', minor_block)
                
                if len(matches) >= 2:
                    inner_content = matches[0]
                    outer_content = matches[1]
                    results[f'query_{idx}'].append((inner_content, outer_content))

        return results
    
    def convert_sql_to_plan_with_execute(self, sql, timeout = True, timeout_seconds=None):
        if timeout:
            self.cursor.execute(f"SET statement_timeout = '{self.timeout_seconds if timeout_seconds is None else timeout_seconds}s'")
        # try:
        #     self.cursor.execute("EXPLAIN (ANALYZE, TIMING, VERBOSE, COSTS, SUMMARY, FORMAT JSON) " + sql)
        # except:
        #     self.cursor.execute("EXPLAIN (VERBOSE, COSTS, SUMMARY, FORMAT JSON) " + sql)
        try:
            self.cursor.execute("EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) " + sql)
        except:
            self.cursor.execute("EXPLAIN (VERBOSE, FORMAT JSON) " + sql)
        res = self.cursor.fetchall()
        explain_json = res[0][0]
        return explain_json
    
    def convert_sql_to_plan_without_execute(self, sql):
        # self.set_optimize()
        self.cursor.execute("EXPLAIN (VERBOSE, FORMAT JSON) " + sql)
        res = self.cursor.fetchall()
        explain_json = res[0][0]
        return explain_json
    
    def get_col_type(self, col_name, table_name = None):
        if table_name is not None:
            query = f"SELECT data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{col_name}';"
        else:
            query = f"SELECT data_type FROM information_schema.columns WHERE column_name = '{col_name}';"
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        if len(res) == 0:
            print(col_name, table_name)
            raise ValueError(f"Column {col_name} not found.")
        return res[0][0]

if __name__ == "__main__":
    # executor = SQLExecutor(dataset = "imdb", user="postgres", password="postgres", host="166.111.121.55", port="20004")
    # executor.gen_subquery([])
    pass