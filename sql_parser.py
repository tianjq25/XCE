"""
This file provides utilities for parsing, normalizing, and reconstructing SQL queries.
It uses sqlglot to analyze SQL syntax trees, separates table information, filter conditions,
and join conditions, and includes helper functions for extracting join relationships and
equivalent join-key groups. The module is designed to support SQL normalization and join
analysis in PostgreSQL-style queries.
"""
import sqlglot
from sqlglot import exp
import re
from run_ASM import preprocess_between

def normalize_expression(cond):
    """
    Normalize a single condition:
    1. Remove extra spaces and normalize case
    2. For equality joins such as A = B, ensure a consistent direction by sorting
    """
    # Normalize operators and basic formatting
    cond = cond.strip().replace('<>', '!=')
    
    # Handle the order of equality conditions (for A = B)
    if '=' in cond and '!=' not in cond and '>=' not in cond and '<=' not in cond:
        parts = cond.split('=')
        if len(parts) == 2:
            left, right = parts[0].strip(), parts[1].strip()
            # Ensure the lexicographically smaller one is on the left
            if left > right:
                cond = f"{right} = {left}"
            else:
                cond = f"{left} = {right}"
    return cond

class SQLParser:
    def __init__(self):
        pass

    def parse(self, query, read = "postgres"):
        # 1. Parse SQL into an AST (Abstract Syntax Tree)
        # read='postgres' means parsing with PostgreSQL syntax (to match your JOB dataset)

        # query = preprocess_between(query)
        parsed = sqlglot.parse_one(query, read='postgres')
        
        # result containers
        tables_dict = {}       # {alias: real_name}
        joins = []             # Store join condition strings
        filters = {}           # {alias: [condition strings]}

        # --- A. Extract tables and aliases ---
        # Traverse all Table nodes
        for table in parsed.find_all(exp.Table):
            real_name = table.name
            # sqlglot automatically recognizes aliases;
            # if there is no alias, the alias is the table name itself
            alias = table.alias if table.alias else real_name
            tables_dict[alias] = real_name
            
            # Initialize the filter list
            if alias not in filters:
                filters[alias] = []

        # Helper function: determine whether a condition is a Join or a Filter
        def classify_condition(node):
            # Get all column objects involved in this condition node
            cols = list(node.find_all(exp.Column))
            involved_tables = set()
            
            for col in cols:
                # col.table gets the table alias of the column (for example, t in t.id)
                if col.table:
                    involved_tables.add(col.table)
            
            # Convert to an SQL string for storage
            sql_str = node.sql()

            if len(involved_tables) > 1:
                # Involves multiple tables -> Join
                joins.append(sql_str)
            elif len(involved_tables) == 1:
                # Involves only one table -> Filter
                tbl = list(involved_tables)[0]
                if tbl in filters:
                    filters[tbl].append(sql_str)
                else:
                    pass
            else:
                pass

        # --- B. Handle conditions in the WHERE clause (implicit Join + Filter) ---
        where = parsed.find(exp.Where)
        if where:
            # In sqlglot, where.this is the root node of the condition
            # Use flatten() to flatten nested AND structures into a list
            # Example: A AND (B AND C) -> [A, B, C]
            for condition in where.this.flatten() if isinstance(where.this, exp.And) else [where.this]:
                classify_condition(condition)

        # --- C. Handle conditions in JOIN ... ON (explicit Join) ---
        for join in parsed.find_all(exp.Join):
            on_clause = join.args.get("on")
            if on_clause:
                for condition in on_clause.flatten() if isinstance(on_clause, exp.And) else [on_clause]:
                    classify_condition(condition)

        return tables_dict, filters, joins
    
    @staticmethod
    def _cast_to_pg_timestamp(sql):
        """Convert sqlglot output CAST('...' AS TIMESTAMP) back to PostgreSQL-native '...'::timestamp"""
        return re.sub(
            r"CAST\s*\(\s*'([^']+)'\s+AS\s+TIMESTAMP\s*\)",
            r"'\1'::timestamp",
            sql,
            flags=re.IGNORECASE
        )

    def reconstruct_sql(self, tables_dict, filters, joins):
        # --- Helper function: depth-based full-parentheses detection ---
        def is_fully_wrapped(s):
            """
            Determine whether a string is completely wrapped by the outermost parentheses.
            Example:
            "(A OR B)" -> True
            "(A) OR (B)" -> False (although it starts and ends with parentheses, it breaks in the middle)
            "A OR B" -> False
            """
            s = s.strip()
            if not (s.startswith("(") and s.endswith(")")):
                return False
            
            depth = 0
            # Traverse the string (excluding the last character)
            for i, char in enumerate(s[:-1]):
                if char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                
                # If depth returns to 0 before traversal is complete,
                # it means the first opening parenthesis has already closed and more content follows
                # For example, in "(A) OR...", depth becomes 0 at ')'
                if depth == 0:
                    return False
            
            return True

        # --- Helper function: safely process conditions ---
        def safe_condition(cond):
            """
            If a condition contains OR and is not fully wrapped in parentheses,
            forcibly add parentheses
            """

            cond = SQLParser._cast_to_pg_timestamp(cond)
            cond = cond.replace('<>', '!=')
            
            # 2. Normalize NOT LIKE
            # Match: NOT table.col LIKE
            # Replace with: table.col NOT LIKE
            not_like_pattern = r'\bNOT\s+([a-zA-Z0-9_.]+)\s+LIKE\b'
            cond = re.sub(not_like_pattern, r'\1 NOT LIKE', cond, flags=re.IGNORECASE)

            # 3. Normalize NOT IN
            # Match: NOT table.col IN
            # Replace with: table.col NOT IN
            not_in_pattern = r'\bNOT\s+([a-zA-Z0-9_.]+)\s+IN\b'
            cond = re.sub(not_in_pattern, r'\1 NOT IN', cond, flags=re.IGNORECASE)

            # 4. Normalize NOT NULL
            # Match: NOT table.col IS NULL
            # Replace with: table.col IS NOT NULL
            not_null_pattern = r'\bNOT\s+([a-zA-Z0-9_.]+)\s+IS\s+NULL\b'
            cond = re.sub(not_null_pattern, r'\1 IS NOT NULL', cond, flags=re.IGNORECASE)

            # 1. If it is already fully wrapped, return directly
            # (to avoid adding duplicate parentheses)
            if is_fully_wrapped(cond):
                return cond
                
            # 2. Use regex to detect a standalone OR keyword (case-insensitive)
            # \b means word boundary, which can match " OR ", "\nOR\t", ")OR(" and similar cases
            # This is a strong detection: if a top-level OR is found, add parentheses
            if re.search(r'\bOR\b', cond, re.IGNORECASE):
                return f"({cond})"
                
            return cond

        # 1. 构建 FROM 子句
        from_parts = []
        # 排序以保证确定性
        sorted_aliases = sorted(tables_dict.keys())
        
        for alias in sorted_aliases:
            real_name = tables_dict[alias]
            if alias == real_name:
                from_parts.append(real_name)
            else:
                from_parts.append(f"{real_name} AS {alias}")
                
        from_clause = ", ".join(from_parts)

        # 1. Build the FROM clause
        all_conditions = []

        # Sort to ensure determinism
        for alias in sorted_aliases:
            if alias in filters and filters[alias]:
                for cond in filters[alias]:
                    all_conditions.append(safe_condition(cond))
        
        # 2. Build the WHERE clause
        if joins:
            normalized_joins = sorted([safe_condition(j) for j in joins])
            all_conditions.extend(normalized_joins)
            # for j in joins:
            #     all_conditions.append(safe_condition(j))
                    
        # Join all conditions with AND
        if all_conditions:
            where_clause = " AND ".join(all_conditions)
        else:
            where_clause = ""

        # 3. Construct the final SQL
        if where_clause:
            sql = f"SELECT COUNT(*) FROM {from_clause} WHERE {where_clause};"
        else:
            sql = f"SELECT COUNT(*) FROM {from_clause};"
            
        return sql

def process_condition_join(cond, tables_all):
    start = None
    join = False
    join_keys = {}
    for i in range(len(cond)):
        s = cond[i]
        if s == "=":
            start = i
            if cond[i + 1] == "=":
                end = i + 2
            else:
                end = i + 1
            break

    if start is None:
        return None, None, False, None

    left = cond[:start].strip()
    ops = cond[start:end].strip()
    right = cond[end:].strip()
    table1 = left.split(".")[0].strip().lower()
    if table1 in tables_all:
        left = tables_all[table1] + "." + left.split(".")[-1].strip()
    else:
        return None, None, False, None
    if "." in right:
        table2 = right.split(".")[0].strip().lower()
        if table2 in tables_all:
            right = tables_all[table2] + "." + right.split(".")[-1].strip()
            join = True
            join_keys[table1] = left
            join_keys[table2] = right
            return table1 + " " + table2, cond, join, join_keys
    return None, None, False, None


def parse_query_all_join(query):
    """
    This function will parse out all join conditions from the query.
    """
    query = query.replace(" where ", " WHERE ")
    query = query.replace(" from ", " FROM ")
    # query = query.replace(" and ", " AND ")
    query = query.split(";")[0]
    query = query.strip()
    tables_all = {}
    join_cond = {}
    join_keys = {}
    tables_str = query.split(" WHERE ")[0].split(" FROM ")[-1]
    for table_str in tables_str.split(","):
        table_str = table_str.strip()
        if " as " in table_str:
            tables_all[table_str.split(" as ")[-1]] = table_str.split(" as ")[0]
        else:
            tables_all[table_str.split(" ")[-1]] = table_str.split(" ")[0]
    conditions = query.split(" WHERE ")[-1].split(" AND ")

    add_all_equi_join = True

    if add_all_equi_join:
        equi_group = dict()
        def add_edge(k1, k2):
            if k1 in equi_group:
                if k2 in equi_group:
                    temp = equi_group[k1].union(equi_group[k2])
                    equi_group[k1] = temp.copy()
                    equi_group[k2] = temp.copy()
                else:
                    equi_group[k2] = equi_group[k1].copy()
            else:
                if k2 in equi_group:
                    equi_group[k1] = equi_group[k2].copy()
                else:
                    equi_group[k1] = set()
                    equi_group[k2] = set()
            equi_group[k1].add(k2)
            equi_group[k2].add(k1)

    for cond in conditions:
        cond = cond.strip()
        if cond[0] == "(" and cond[-1] == ")":
            cond = cond[1:-1]
        table, cond, join, join_key = process_condition_join(cond, tables_all)

        if join:
            if add_all_equi_join:
                [key1, key2] = cond.split("=")
                key1 = key1.strip()
                key2 = key2.strip()
                add_edge(key1, key2)
            for tab in join_key:
                if tab in join_keys:
                    join_keys[tab].add(join_key[tab])
                    join_cond[tab].add(cond)
                else:
                    join_keys[tab] = set([join_key[tab]])
                    join_cond[tab] = set([cond])

    if add_all_equi_join:
        for t in join_keys:
            for k in join_keys[t]:
                [_, c] = k.split(".")
                k = t + "." + c
                for other_k in equi_group[k]:
                    if k == other_k:
                        continue
                    other_t = other_k.split(".")[0]
                    cond1 = k + " = " + other_k
                    cond2 = other_k + " = " + k
                    if cond1 not in join_cond[t] and cond2 not in join_cond[t]:
                        assert cond1 not in join_cond[other_t]
                        assert cond2 not in join_cond[other_t]
                        join_cond[t].add(cond1)
                        join_cond[other_t].add(cond1)

    return tables_all, join_cond, join_keys

def find_equivalent_groups(pairs):
    groups = []
    for pair in pairs:
        found_group = None
        for group in groups:
            if pair[0] in group or pair[1] in group:
                found_group = group
                break

        if found_group:
            found_group.add(pair[0])
            found_group.add(pair[1])
        else:
            groups.append(set(pair))

    return groups

if __name__ == "__main__":
    pass