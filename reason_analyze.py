import os, time, pickle, itertools
from sql_parser import SQLParser, parse_query_all_join
from sql_executor import SQLExecutor
from check_join import UnionFind
from cache_db import SQLCacheDB
from run_ASM import run_ASM_one, parse_sql_to_dict, get_table_P

def all_subsets(s, start=1, end=None):
    result = []
    if end is None:
        end = len(s)
    for r in range(start, end + 1):
        for combo in itertools.combinations(s, r):
            result.append(list(combo))
    return result

class reason_sql:
    def __init__(self, parser: SQLParser, executor: SQLExecutor, bound_ensemble_nc, out_file="training_dataset_imdb.txt", global_threshold = 1.2, global_threshold_le = 0.8):
        self.parser = parser
        self.executor = executor
        self.out_file = out_file
        self.bound_ensemble_nc = bound_ensemble_nc
        self.global_threshold = global_threshold
        self.global_threshold_le = global_threshold_le

    def analyze_tables(self, cardinality, filters, threshold):
        for table in cardinality['tables']:
            res = self.executor.execute_query(self.parser.reconstruct_sql({table['alias']: table['table_name']}, {}, {}))
            if res is None:
                continue
            table_total_num = res[0][0]

            table_alias = table['alias']
            table_filters = filters.get(table_alias, [])
            table_P = table['final_prob'] if table['final_prob'] is not None else 1.0

            if table_filters != []:
                table_sql = self.parser.reconstruct_sql({table['alias']: table['table_name']}, {table_alias: table_filters}, {})

                res = self.executor.execute_query(table_sql)
                if res is None:
                    continue
                table_true_sample = res[0][0]

                table_selectivity = table_true_sample / table_total_num

                try:
                    table_repair_value = table_selectivity / table_P
                except ZeroDivisionError:
                    table_repair_value = table_true_sample / (table_total_num * table_P)
            else:
                table_repair_value = 1.0

            if table_repair_value >= self.global_threshold:
                with open(self.out_file, 'a') as f:
                    print(f"{table_sql}#####{table_repair_value}", file=f)

                if len(table['merged_columns']) <= 1:
                    continue

                self.analyze_single_table_cols(table, table_alias, table_total_num, table_filters, table_repair_value ** (1 / len(table['merged_columns'])))

    def analyze_single_table_cols(self, table, table_alias, table_total_num, table_filters, threshold):
        col_table_dict = {table_alias: table['table_name']}
        col_filter_dict = {table_alias: []}
        prev_col_filters = []
        for col_name, col_prob in table['merged_columns'].items():
            curr_col_filters = [f for f in table_filters if col_name in f]
            if len(curr_col_filters) == 0:
                continue
            col_filter_dict[table_alias].extend(curr_col_filters)
            col_sql = self.parser.reconstruct_sql(col_table_dict, col_filter_dict, {})
            col_res = self.executor.execute_query(col_sql)
            if col_res is None:
                continue
            col_selectivity = col_res[0][0] / table_total_num
            try:
                col_factor = col_selectivity / col_prob
            except ZeroDivisionError:
                col_factor = col_res[0][0] / (table_total_num * col_prob)

            if col_factor >= self.global_threshold:
                with open(self.out_file, 'a') as f:
                    print(f"{col_sql}#####{col_factor}", file=f)
                
                if len(curr_col_filters) > 1 or len(prev_col_filters) != 0:
                    self.analyze_single_table_filters(table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num, col_factor ** (1 / len(col_filter_dict[table_alias])))
            
            prev_col_filters.extend(curr_col_filters)

    def analyze_single_table_filters(self, table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num, threshold):
        curr_col_filters_all_subsets = all_subsets(curr_col_filters, start=0, end = len(curr_col_filters) - 1)
        for curr_subset in curr_col_filters_all_subsets:
            col_filter_subset = prev_col_filters + curr_subset
            # if len(col_filter_subset) == len(prev_col_filters) + len(curr_col_filters):
            #     continue
            sub_cond_sql = self.parser.reconstruct_sql(col_table_dict, {table_alias: col_filter_subset}, {})
            sub_cond_res = self.executor.execute_query(sub_cond_sql)
            if sub_cond_res is None:
                continue
            sub_cond_selectivity = sub_cond_res[0][0] / table_total_num
            sub_cond_P = get_table_P(sub_cond_sql, parse_sql_to_dict(sub_cond_sql), bound_ensemble_nc=self.bound_ensemble_nc)

            try:
                sub_cond_factor = sub_cond_selectivity / sub_cond_P
            except ZeroDivisionError:
                sub_cond_factor = sub_cond_res[0][0] / (table_total_num * sub_cond_P)

            if sub_cond_factor >= self.global_threshold:
                with open(self.out_file, 'a') as f:
                    print(f"{sub_cond_sql}#####{sub_cond_factor}", file=f)

    def analyze_multi_table_cols(self, cardinality, join_tables, join_filters, join_conditions, join_steps, threshold):
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
                col_res = self.executor.execute_query(col_sql)
                if col_res is None:
                    continue

                sub_query = self.executor.get_subquery_without_archive(col_sql)
                print(sub_query)
                ests = run_ASM_one(
                    query=col_sql,
                    sub_plan=sub_query,
                    query_predicate=parse_sql_to_dict(col_sql),
                    bound_ensemble_nc=self.bound_ensemble_nc,
                )

                col_factor = col_res[0][0] / ests[-1]
                
                if col_factor >= self.global_threshold:
                    with open(self.out_file, 'a') as f:
                        print(f"{col_sql}#####{col_factor}", file=f)

    def analyze_joins(self, sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters, threshold):
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
            join_res = self.executor.execute_query(join_sql)
            if join_res is None:
                continue

            for query, ests in zip(all_sub_queries, join_estimates):
                left_t, right_t = query
                if sorted(left_t.split(' ') + right_t.split(' ')) == sorted(all_join_tables):
                    join_estimate = ests

            try:
                # join_repair_value = (join_res[0][0] / join_estimate) ** (1 / len(all_join_tables))
                join_repair_value = join_res[0][0] / join_estimate
            except:
                for query, ests in zip(all_sub_queries, join_estimates):
                    left_t, right_t = query
                    print(sorted(left_t.split(' ') + right_t.split(' ')), sorted(all_join_tables))
                raise

            if join_repair_value >= self.global_threshold:
                with open(self.out_file, 'a') as f:
                    # print(f"{join_sql}#####{join_repair_value ** (1 / len(all_join_tables))}", file=f)
                    print(f"{join_sql}#####{join_repair_value}", file=f)

                all_filters_count = sum([len(value) for value in join_filters.values()])
                if all_filters_count <= 1:
                    continue
                self.analyze_multi_table_cols(cardinality, join_tables, join_filters, tmp_join_condions, join_steps, threshold ** (1 / all_filters_count))

    def analyze_single_query(self, sql_query, all_sub_queries, cardinality, join_estimates, join_steps):
        tables, filters, joins = self.parser.parse(sql_query)
        reconstructed_sql = self.parser.reconstruct_sql(tables, filters, joins)
        res = self.executor.execute_query(reconstructed_sql, time_out_seconds=3600)
        if res is None:
            return

        threshold = res[0][0] / float(join_estimates[-1])
        print(reconstructed_sql, threshold ** (1/len(tables)))

        # if threshold < 1.0:
        #     return

        if len(tables) == 1:
            with open(self.out_file, 'a') as f:
                print(f"{reconstructed_sql}#####{threshold}", file=f)
            res = self.executor.execute_query(self.parser.reconstruct_sql({cardinality['tables'][0]['alias']: cardinality['tables'][0]['table_name']}, {}, {}))
            if res is None:
                return
            table_total_num = res[0][0]
            if len(cardinality['tables'][0]['merged_columns']) == 0:
                return
            elif len(cardinality['tables'][0]['merged_columns']) == 1:
                filter = filters.get(cardinality['tables'][0]['alias'], [])
                if len(filter) <= 1:
                    return
                self.analyze_single_table_filters(cardinality['tables'][0]['alias'], tables, [], filter, table_total_num, threshold ** (1/len(filter)))
            else:
                self.analyze_single_table_cols(cardinality['tables'][0], cardinality['tables'][0]['alias'], table_total_num, filters.get(cardinality['tables'][0]['alias'], []), threshold ** (1/len(cardinality['tables'][0]['merged_columns'])))
        else:
            self.analyze_tables(cardinality, filters, threshold ** (1/len(tables)))
            self.analyze_joins(sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters, threshold ** (1/len(tables)))

class reason_sql_parallel():
    def __init__(self, parser: SQLParser, executor: SQLExecutor, cache: SQLCacheDB, global_threshold = 1.2, global_threshold_le = 0.8):
        self.parser = parser
        self.executor = executor
        self.cache = cache
        self.global_threshold = global_threshold
        self.global_threshold_le = global_threshold_le

    def analyze_tables(self, cardinality, filters, threshold):
        for table in cardinality['tables']:
            table_raw_sql = self.parser.reconstruct_sql({table['alias']: table['table_name']}, {}, {})
            cache_res = self.cache.get_value(table_raw_sql, 'true_cardinality')
            if cache_res == -1:
                res = self.executor.execute_query(table_raw_sql)
                if res is None:
                    continue
                self.cache.update_sql_value(table_raw_sql, true_cardinality=res[0][0])
                table_total_num = res[0][0]
            else:
                table_total_num = cache_res

            table_alias = table['alias']
            table_filters = filters.get(table_alias, [])
            table_P = table['final_prob'] if table['final_prob'] is not None else 1.0

            table_sql = self.parser.reconstruct_sql({table['alias']: table['table_name']}, {table_alias: table_filters}, {})
            if self.cache.get_value(table_sql, 'is_pruned'):
                continue
            if table_filters != []:
                cache_res = self.cache.get_value(table_sql, 'true_cardinality')
                if cache_res == -1:
                    res = self.executor.execute_query(table_sql)
                    if res is None:
                        continue
                    self.cache.update_sql_value(table_sql, true_cardinality=res[0][0])
                    table_true_sample = res[0][0]
                else:
                    table_true_sample = cache_res

                table_repair_value = table_true_sample / (table_total_num * table_P)
            else:
                # table_repair_value = 1.0
                continue

            if self.cache.get_value(table_sql, 'repair_value') == -1:
                self.cache.update_sql_value(table_sql, repair_value=table_repair_value)

            if table_repair_value >= self.global_threshold:
                self.cache.update_sql_value(table_sql, is_selected = 1)

                if len(table['merged_columns']) <= 1:
                    continue

                self.analyze_single_table_cols(table, table_alias, table_total_num, table_filters, table_repair_value ** (1 / len(table['merged_columns'])))
            # else:
            #     self.cache.update_sql_value(table_sql, is_pruned=1)

    def analyze_single_table_cols(self, table, table_alias, table_total_num, table_filters, threshold):
        col_table_dict = {table_alias: table['table_name']}
        col_filter_dict = {table_alias: []}
        prev_col_filters = []
        for col_name, col_prob in table['merged_columns'].items():
            curr_col_filters = [f for f in table_filters if col_name in f]
            if len(curr_col_filters) == 0:
                continue
            col_filter_dict[table_alias].extend(curr_col_filters)
            col_sql = self.parser.reconstruct_sql(col_table_dict, col_filter_dict, {})

            cache_res = self.cache.get_value(col_sql, 'true_cardinality')
            if cache_res == -1:
                col_res = self.executor.execute_query(col_sql)
                if col_res is None:
                    prev_col_filters.extend(curr_col_filters)
                    continue
                self.cache.update_sql_value(col_sql, true_cardinality=col_res[0][0])
            else:
                col_res = [(cache_res,)]

            col_factor = col_res[0][0] / (table_total_num * col_prob)

            if self.cache.get_value(col_sql, 'repair_value') == -1:
                self.cache.update_sql_value(col_sql, repair_value=col_factor)

            if col_factor >= self.global_threshold:
                self.cache.update_sql_value(col_sql, is_selected=1)
                
                if len(curr_col_filters) > 1 or len(prev_col_filters) != 0:
                    self.analyze_single_table_filters(table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num, col_factor ** (1 / len(col_filter_dict[table_alias])))
            # else:
            #     self.cache.update_sql_value(col_sql, is_pruned=1)
            prev_col_filters.extend(curr_col_filters)

    def analyze_single_table_filters(self, table_alias, col_table_dict, prev_col_filters, curr_col_filters, table_total_num, threshold):
        if len(prev_col_filters) == 0:
            curr_col_filters_all_subsets = all_subsets(curr_col_filters, start=1, end = len(curr_col_filters) - 1)
        else:
            curr_col_filters_all_subsets = all_subsets(curr_col_filters, start=0, end = len(curr_col_filters) - 1)
        for curr_subset in curr_col_filters_all_subsets:
            col_filter_subset = prev_col_filters + curr_subset
            sub_cond_sql = self.parser.reconstruct_sql(col_table_dict, {table_alias: col_filter_subset}, {})

            if self.cache.get_value(sub_cond_sql, 'is_pruned'):
                continue

            cache_res = self.cache.get_value(sub_cond_sql, 'true_cardinality')
            if cache_res == -1:
                sub_cond_res = self.executor.execute_query(sub_cond_sql)
                if sub_cond_res is None:
                    continue
                self.cache.update_sql_value(sub_cond_sql, true_cardinality=sub_cond_res[0][0])
            else:
                sub_cond_res = [(cache_res,)]
            sub_cond_P = self.cache.get_value(sub_cond_sql, 'est_cardinality')
            if sub_cond_P == -1:
                print("Error: est_cardinality not found in cache for SQL:", sub_cond_sql)
                continue
            sub_cond_factor = sub_cond_res[0][0] / (table_total_num * sub_cond_P)

            if self.cache.get_value(sub_cond_sql, 'repair_value') == -1:
                self.cache.update_sql_value(sub_cond_sql, repair_value=sub_cond_factor)

            if sub_cond_factor >= self.global_threshold:
                self.cache.update_sql_value(sub_cond_sql, is_selected=1)
            # else:
            #     self.cache.update_sql_value(sub_cond_sql, is_pruned=1)

    def analyze_multi_table_cols(self, cardinality, join_tables, join_filters, join_conditions, join_steps, threshold):
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

                if self.cache.get_value(col_sql, 'is_pruned'):
                    continue

                cache_res = self.cache.get_value(col_sql, 'true_cardinality')
                if cache_res == -1:
                    col_res = self.executor.execute_query(col_sql)
                    if col_res is None:
                        continue
                    self.cache.update_sql_value(col_sql, true_cardinality=col_res[0][0])
                else:
                    col_res = [[cache_res]]
                
                join_estimate = self.cache.get_value(col_sql, 'est_cardinality')
                if join_estimate == -1:
                    print("Error: est_cardinality not found in cache for SQL:", col_sql)
                    continue

                col_factor = col_res[0][0] / join_estimate

                if self.cache.get_value(col_sql, 'repair_value') == -1:
                    self.cache.update_sql_value(col_sql, repair_value=col_factor)
                
                if col_factor >= self.global_threshold:
                    self.cache.update_sql_value(col_sql, is_selected=1)

    def analyze_joins(self, sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters, threshold):
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
            if self.cache.get_value(join_sql, 'is_pruned'):
                continue

            cache_res = self.cache.get_value(join_sql, 'true_cardinality')
            if cache_res == -1:
                join_res = self.executor.execute_query(join_sql)
                if join_res is None:
                    continue
                self.cache.update_sql_value(join_sql, true_cardinality=join_res[0][0])
            else:
                join_res = [[cache_res]]

            join_estimate = self.cache.get_value(join_sql, 'est_cardinality')
            if join_estimate == -1:
                for query, ests in zip(all_sub_queries, join_estimates):
                    left_t, right_t = query
                    if sorted(left_t.split(' ') + right_t.split(' ')) == sorted(all_join_tables):
                        join_estimate = ests
                        self.cache.update_sql_value(join_sql, est_cardinality=join_estimate)
                        break
                if join_estimate == -1:
                    print("Error: est_cardinality not found in cache for SQL:", join_sql)
                    continue
            join_repair_value = join_res[0][0] / join_estimate

            if self.cache.get_value(join_sql, 'repair_value') == -1:
                self.cache.update_sql_value(join_sql, repair_value=join_repair_value)

            if join_repair_value >= self.global_threshold:
                self.cache.update_sql_value(join_sql, is_selected=1)

                all_filters_count = sum([len(value) for value in join_filters.values()])
                if all_filters_count <= 1:
                    continue
                self.analyze_multi_table_cols(cardinality, join_tables, join_filters, tmp_join_condions, join_steps, threshold ** (1 / all_filters_count))

    def analyze_single_query(self, sql_query, all_sub_queries, cardinality, join_estimates, join_steps):
        tables, filters, joins = self.parser.parse(sql_query)
        reconstructed_sql = self.parser.reconstruct_sql(tables, filters, joins)

        is_pruned = self.cache.get_value(reconstructed_sql, 'is_pruned')
        if is_pruned:
            return

        cache_res = self.cache.get_value(reconstructed_sql, 'true_cardinality')
        if cache_res == -1:
            res = self.executor.execute_query(reconstructed_sql, time_out_seconds=3600)
            if res is None:
                return
            self.cache.update_sql_value(reconstructed_sql, true_cardinality=res[0][0])
            threshold = res[0][0] / float(join_estimates[-1])
        else:    
            threshold = cache_res / float(join_estimates[-1])

        print(reconstructed_sql, threshold ** (1/len(tables)))
        if self.cache.get_value(reconstructed_sql, 'repair_value') == -1:
            self.cache.update_sql_value(reconstructed_sql, repair_value=threshold)

        if threshold <= self.global_threshold and threshold >= self.global_threshold_le:
            self.cache.update_sql_value(reconstructed_sql, is_pruned=1)
            return 

        if len(tables) == 1:
            table_sql = self.parser.reconstruct_sql({cardinality['tables'][0]['alias']: cardinality['tables'][0]['table_name']}, {}, {})

            is_pruned = self.cache.get_value(table_sql, 'is_pruned')
            if is_pruned:
                return
            
            cache_res = self.cache.get_value(table_sql, 'true_cardinality')
            if cache_res == -1:
                res = self.executor.execute_query(table_sql)
                if res is None:
                    return
                self.cache.update_sql_value(table_sql, true_cardinality=res[0][0])
                table_total_num = res[0][0]
            else:
                table_total_num = cache_res
            
            if len(cardinality['tables'][0]['merged_columns']) == 0:
                return
            elif len(cardinality['tables'][0]['merged_columns']) == 1:
                filter = filters.get(cardinality['tables'][0]['alias'], [])
                if len(filter) <= 1:
                    return
                self.analyze_single_table_filters(cardinality['tables'][0]['alias'], tables, [], filter, table_total_num, threshold ** (1/len(filter)))
            else:
                self.analyze_single_table_cols(cardinality['tables'][0], cardinality['tables'][0]['alias'], table_total_num, filters.get(cardinality['tables'][0]['alias'], []), threshold ** (1/len(cardinality['tables'][0]['merged_columns'])))
        else:
            self.analyze_tables(cardinality, filters, threshold ** (1/len(tables)))
            self.analyze_joins(sql_query, cardinality, all_sub_queries, join_estimates, join_steps, tables, filters, threshold ** (1/len(tables)))

if __name__ == "__main__":
    pass