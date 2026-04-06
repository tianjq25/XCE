'''
This file implements a SQLite-based SQL cache manager for storing, retrieving,
and updating SQL query metadata. It normalizes SQL statements through a parser,
generates hashes for deduplication, and maintains cached information such as
true cardinality, estimated cardinality, repair values, and selection/pruning flags.
The class also includes retry logic to handle temporary database locking issues.
'''
import sqlite3
import hashlib
import time
import os
from sql_parser import SQLParser

class SQLCacheDB:
    def __init__(self, db_folder="Cache_DB", db_name="repair_cache.db", max_retries=5):
        # 1. Automatically create the folder if it does not exist
        if not os.path.exists(db_folder):
            os.makedirs(db_folder)
            
        self.parser = SQLParser()
        self.db_path = os.path.join(db_folder, db_name)
        self.max_retries = max_retries
        self._prepare_db()

    def _prepare_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sql_cache (
                    sql_hash TEXT PRIMARY KEY,
                    full_sql TEXT,
                    true_cardinality REAL,
                    est_cardinality REAL,
                    repair_value REAL,
                    is_pruned INTEGER,
                    is_selected INTEGER
                )
            """)
            conn.execute("PRAGMA journal_mode=WAL;")

    def _clean_sql(self, sql):
        tables, filters, joins = self.parser.parse(sql)
        return self.parser.reconstruct_sql(tables, filters, joins)

    def _get_hash(self, sql):
        """Use a unified hash computation to reduce duplicate code."""
        # return hashlib.md5(sql.encode()).hexdigest()
        return hashlib.sha256(sql.encode()).hexdigest()

    def get_value(self, sql, value_name):
        sql_hash = self._get_hash(self._clean_sql(sql))
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(f"SELECT {value_name} FROM sql_cache WHERE sql_hash = ?", (sql_hash,))
            row = cursor.fetchone()
            if row is None:
                self.save_sql(sql)
                cursor = conn.execute(f"SELECT {value_name} FROM sql_cache WHERE sql_hash = ?", (sql_hash,))
                row = cursor.fetchone()
            return row[0]

    def save_sql(self, sql):
        """Store the initial SQL cache entry with limited retry logic."""
        sql_hash = self._get_hash(self._clean_sql(sql))
        
        # Use a loop instead of recursion to avoid stack overflow
        for attempt in range(self.max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30) as conn: # timeout
                    # 1. First check whether this hash is already occupied
                    cursor = conn.execute("SELECT full_sql FROM sql_cache WHERE sql_hash = ?", (sql_hash,))
                    row = cursor.fetchone()
                    
                    if row:
                        if row[0] == sql:
                            return 
                        else:
                            # An extremely rare collision occurred: same hash but different SQL
                            # Solution: append a suffix to the hash before storing, or log/report an error
                            print(f"⚠️ Warning: Hash collision detected! SQL: {sql}...")

                    conn.execute("""
                        INSERT OR IGNORE INTO sql_cache (sql_hash, full_sql, repair_value, true_cardinality, est_cardinality, is_pruned, is_selected)
                        VALUES (?, ?, -1, -1, -1, 0, 0)
                    """, (sql_hash, sql))
                return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < self.max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    print(f"Write failed: {e}")
                    break

    def update_sql_value(self, sql, true_cardinality=-1, est_cardinality=-1, repair_value=-1, is_pruned=None, is_selected=None):
        """
        Only update fields whose values differ from the defaults.
        is_pruned defaults to None so that it is possible to distinguish
        whether the user really intends to update it.
        is_selected defaults to None so that it is possible to distinguish
        whether the user really intends to update it.
        """
        sql_hash = self._get_hash(self._clean_sql(sql))

        # 1. Dynamically build the fields that need to be updated
        updates = []
        params = []

        # Handle true_cardinality
        if true_cardinality != -1:
            updates.append("true_cardinality = ?")
            params.append(float(true_cardinality))

        # Handle est_cardinality
        if est_cardinality != -1:
            updates.append("est_cardinality = ?")
            params.append(float(est_cardinality))

        # Handle repair_value
        if repair_value != -1:
            updates.append("repair_value = ?")
            params.append(float(repair_value))

        # Handle is_pruned (assuming 0 is the default, update only when a non-default value is passed in or explicitly specified)
        # Or if you want to update only when explicitly specified, you can use None for checking
        if is_pruned is not None:
            updates.append("is_pruned = ?")
            params.append(int(is_pruned))

        # Handle is_selected (assuming 0 is the default, update only when a non-default value is passed in or explicitly specified)
        if is_selected is not None:
            updates.append("is_selected = ?")
            params.append(int(is_selected))

        # Return directly if there are no fields to update
        if not updates:
            return

        # 2. Construct the SQL statement
        sql_update = f"UPDATE sql_cache SET {', '.join(updates)} WHERE sql_hash = ?"
        params.append(sql_hash)

        # 3. Execute the update with retry logic
        for attempt in range(self.max_retries):
            try:
                with sqlite3.connect(self.db_path, timeout=30) as conn:
                    conn.execute(sql_update, params)
                return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and attempt < self.max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    print(f"Update failed: {e}")
                    break

    def get_selected_sqls(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT full_sql, repair_value FROM sql_cache WHERE is_selected = 1 AND repair_value > 1")
            return [(row[0], row[1]) for row in cursor.fetchall()]

if __name__ == "__main__":
    pass
    # cache_db = SQLCacheDB(db_folder="Cache_DB", db_name="imdb_cache_dataset.db")
    # selected_sqls = cache_db.get_selected_sqls()
    # # print(selected_sqls)
    # with open("training_dataset_imdb.txt", "a") as f:
    #     for sql, repair_value in selected_sqls:
    #         f.write(f"{sql}#####{repair_value}\n")
