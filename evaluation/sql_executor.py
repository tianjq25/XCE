import psycopg2
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
        self.docker_file_handler = DockerFileHandler(container_name_or_id="ce-benchmark-plus")

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
            # 1. Set the timeout duration
            # (PostgreSQL-specific syntax)
            # This tells the database that if the following statement runs
            # longer than timeout_seconds, it should be terminated
            # Note: the SET operation itself is very fast and is not counted toward the timeout
            if timeout:
                self.cursor.execute(f"SET statement_timeout = '{self.timeout_seconds if time_out_seconds is None else time_out_seconds}s'")

            # 2. Execute the actual query
            print(f"Query: {query}")
            
            self.cursor.execute(query)
            
            # 3. Compute the execution time
            # (only for logging purposes)
            # time_cost = time.time() - start_time
            # print(f"Query: {query}")
            # print(f"Query executed in {time_cost:.4f} seconds.")

            return self.cursor.fetchall()

        except psycopg2.errors.QueryCanceled:
            # This specifically catches timeout exceptions
            print(f"Error: SQL execution timed out ({self.timeout_seconds if time_out_seconds is None else time_out_seconds} seconds), and has been stopped.")
            print(f"Query: {query}")
            # You may raise a custom exception here or return None
            # raise TimeoutError(f"Query timed out after {timeout_seconds} seconds")
            return None
            
        except Exception as e:
            # Catch other exceptions such as SQL syntax errors
            print(f"SQL Execution Error: {e}")
            print(f"Query: {query}")
            raise e
        finally:
            # Optional: to avoid affecting subsequent queries,
            # it is better to reset the timeout setting
            # (or reset it before each query)
            # If a transaction rollback occurs after an error,
            # this step may need to be placed after rollback
            pass
    
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

if __name__ == "__main__":
    # executor = SQLExecutor(dataset = "imdb", user="postgres", password="postgres", host="166.111.121.55", port="20004")
    # executor.gen_subquery([])
    pass