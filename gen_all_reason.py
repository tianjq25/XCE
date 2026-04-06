import os, time, pickle, itertools
from sql_parser import SQLParser, parse_query_all_join
from sql_executor import SQLExecutor
from cache_db import SQLCacheDB
from check_join import UnionFind
from run_ASM import run_ASM_one, parse_sql_to_dict, get_table_P

def all_subsets(s, start=1, end=None):
    result = []
    if end is None:
        end = len(s)
    for r in range(start, end + 1):
        for combo in itertools.combinations(s, r):
            result.append(list(combo))
    return result

class gen_reason_sql:
    def __init__(self, parser: SQLParser, executor: SQLExecutor, cache: SQLCacheDB, bound_ensemble_nc):
        self.parser = parser
        self.executor = executor
        self.cache = cache
        self.bound_ensemble_nc = bound_ensemble_nc

    def analyze_tables(self, cardinality, filters):
        for table in cardinality['tables']:
            table_raw_sql = self.parser.reconstruct_sql({table['alias']: table['table_name']}, {}, {})
            table_total_num = self.cache.get_value(table_raw_sql, 'true_cardinality')
            if table_total_num == -1:
                res = self.executor.execute_query(table_raw_sql)
                if res is None:
                    continue
                table_total_num = res[0][0]
                self.cache.update_sql_value(table_raw_sql, true_cardinality=table_total_num, est_cardinality=table_total_num)

            table_alias = table['alias']
            table_filters = filters.get(table_alias, [])
            table_P = table['final_prob'] if table['final_prob'] is not None else 1.0

            if table_filters != []:
                table_sql = self.parser.reconstruct_sql({table['alias']: table['table_name']}, {table_alias: table_filters}, {})
                self.cache.save_sql(table_sql)
                if self.cache.get_value(table_sql, 'est_cardinality') == -1:
                    self.cache.update_sql_value(table_sql, est_cardinality=table_total_num * table_P)

                if len(table['merged_columns']) <= 1:
                    continue

                self.analyze_single_table_cols(table, table_alias, table_filters, table_total_num)

    def analyze_single_table_cols(self, table, table_alias, table_filters, table_total_num):
        col_table_dict = {table_alias: table['table_name']}
        col_filter_dict = {table_alias: []}
        prev_col_filters = []
        for col_name, col_prob in table['merged_columns'].items():
            curr_col_filters = [f for f in table_filters if col_name in f]
            if len(curr_col_filters) == 0:
                continue
            col_filter_dict[table_alias].extend(curr_col_filters)
            col_sql = self.parser.reconstruct_sql(col_table_dict, col_filter_dict, {})
            self.cache.save_sql(col_sql)
            if self.cache.get_value(col_sql, 'est_cardinality') == -1:
                self.cache.update_sql_value(col_sql, est_cardinality=table_total_num * col_prob)
                
            if len(curr_col_filters) > 1 or len(prev_col_filters) > 0:
                self.analyze_single_table_filters(table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num)
            
            prev_col_filters.extend(curr_col_filters)

    def analyze_single_table_filters(self, table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num):
        if len(prev_col_filters) == 0:
            curr_col_filters_all_subsets = all_subsets(curr_col_filters, start=1, end = len(curr_col_filters) - 1)
        else:
            curr_col_filters_all_subsets = all_subsets(curr_col_filters, start=0, end = len(curr_col_filters) - 1)
        for curr_subset in curr_col_filters_all_subsets:
            col_filter_subset = prev_col_filters + curr_subset
            sub_cond_sql = self.parser.reconstruct_sql(col_table_dict, {table_alias: col_filter_subset}, {})
            self.cache.save_sql(sub_cond_sql)

            if self.cache.get_value(sub_cond_sql, 'est_cardinality') == -1:
                self.cache.update_sql_value(sub_cond_sql, est_cardinality=table_total_num * get_table_P(sub_cond_sql, parse_sql_to_dict(sub_cond_sql), bound_ensemble_nc=self.bound_ensemble_nc))

    def analyze_multi_table_cols(self, cardinality, join_tables, join_filters, join_conditions):
        col_filter_exec_plan = {table_alias: [] for table_alias in join_tables}
        col_filter_dict = {table_alias: [] for table_alias in join_tables}
        for table in cardinality['tables']:
            if table['alias'] not in join_tables:
                continue

            for col_name in table['merged_columns'].keys():
                col_filter_exec_plan[table['alias']].append([f for f in join_filters[table['alias']] if col_name in f])
                col_filter_dict[table['alias']].extend([f for f in join_filters[table['alias']] if col_name in f])
        
        for table_alias, table in join_tables.items():
            col_filter_dict[table_alias].clear()
            for item in col_filter_exec_plan[table_alias]:
                col_filter_dict[table_alias].extend(item)

                if item == col_filter_exec_plan[table_alias][-1]:
                    continue

                col_sql = self.parser.reconstruct_sql(join_tables, col_filter_dict, join_conditions)
                self.cache.save_sql(col_sql)

                if self.cache.get_value(col_sql, 'est_cardinality') != -1:
                    continue

                sub_query = self.executor.get_subquery_without_archive(col_sql)
                # print(sub_query)
                ests = run_ASM_one(
                    query=col_sql,
                    sub_plan=sub_query,
                    query_predicate=parse_sql_to_dict(col_sql),
                    bound_ensemble_nc=self.bound_ensemble_nc,
                )

                self.cache.update_sql_value(col_sql, est_cardinality=ests[-1])

    def analyze_joins(self, sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters):
        tables_all, join_cond, join_keys = parse_query_all_join(sql_query)
        
        for idx, (left_table_alias, right_table_alias) in enumerate(join_steps):
            all_join_tables = []
            all_join_tables.extend(left_table_alias.split(' '))
            all_join_tables.extend(right_table_alias.split(' '))

            join_tables = {table_alias: tables[table_alias] for table_alias in all_join_tables}
            join_filters = {table_alias: filters.get(table_alias, []) for table_alias in all_join_tables}

            UnionFind_obj = UnionFind()

            tmp_join_condions = []
            # print(join_cond[all_join_tables[0]], join_cond[all_join_tables[-1]])
            for _idx, table in enumerate(all_join_tables):
                # print(table, join_cond[table])
                for cond in join_cond[table]:
                    left_cond, right_cond = cond.split('=')
                    left_cond = left_cond.strip()
                    right_cond = right_cond.strip()
                    left_table = left_cond.split('.')[0]
                    right_table = right_cond.split('.')[0]
                    if (left_table == table and right_table in all_join_tables[_idx + 1:]) or (right_table == table and left_table in all_join_tables[_idx + 1:]):
                        if UnionFind_obj.check_connected(left_cond, right_cond):
                            continue
                        UnionFind_obj.union(left_cond, right_cond)
                        tmp_join_condions.append(cond)

            join_sql = self.parser.reconstruct_sql(join_tables, join_filters, tmp_join_condions)
            self.cache.save_sql(join_sql)

            if self.cache.get_value(join_sql, 'est_cardinality') == -1:
                for query, ests in zip(all_sub_queries, join_estimates):
                    left_t, right_t = query
                    if sorted(left_t.split(' ') + right_t.split(' ')) == sorted(all_join_tables):
                        join_estimate = ests
                
                self.cache.update_sql_value(join_sql, est_cardinality=join_estimate)

            all_filters_count = sum([len(value) for value in join_filters.values()])
            if all_filters_count <= 1:
                continue
            self.analyze_multi_table_cols(cardinality, join_tables, join_filters, tmp_join_condions)

    def analyze_single_query(self, sql_query, all_sub_queries, cardinality, join_estimates, join_steps):
        tables, filters, joins = self.parser.parse(sql_query)
        reconstructed_sql = self.parser.reconstruct_sql(tables, filters, joins)
        self.cache.save_sql(reconstructed_sql)
        if self.cache.get_value(reconstructed_sql, 'est_cardinality') == -1:
            self.cache.update_sql_value(reconstructed_sql, est_cardinality=join_estimates[-1])

        if len(tables) == 1:
            table_sql = self.parser.reconstruct_sql({cardinality['tables'][0]['alias']: cardinality['tables'][0]['table_name']}, {}, {})
            table_total_num = self.cache.get_value(table_sql, 'true_cardinality')
            if table_total_num == -1:
                res = self.executor.execute_query(table_sql)
                if res is None:
                    return
                table_total_num = res[0][0]
                self.cache.update_sql_value(table_sql, true_cardinality=table_total_num, est_cardinality=table_total_num)
            if len(cardinality['tables'][0]['merged_columns']) == 0:
                return
            elif len(cardinality['tables'][0]['merged_columns']) == 1:
                filter = filters.get(cardinality['tables'][0]['alias'], [])
                if len(filter) <= 1:
                    return
                self.analyze_single_table_filters(cardinality['tables'][0]['alias'], tables, [], filter, table_total_num)
            else:
                self.analyze_single_table_cols(cardinality['tables'][0], cardinality['tables'][0]['alias'], filters.get(cardinality['tables'][0]['alias'], []), table_total_num)
        else:
            self.analyze_tables(cardinality, filters)
            self.analyze_joins(sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters)

if __name__ == "__main__":
    pass