import pickle
import sys
import time
import re
import os
import numpy as np
import torch
import pandas as pd
import multiprocessing
import copy
sys.path.append('ASM')
from ASM.Join_scheme.join_graph import get_join_hyper_graph
from ASM.logical_tree import parse_logic_tree, fillcol, get_subtree, to_neurocard_ops
from Join_scheme.bound import Bound_ensemble
# sys.modules['Join_scheme'] = ASM.Join_scheme

from ASM.AR.experiments import EXPERIMENT_CONFIGS
from ASM.AR.neurocard import NeuroCard

def load_neurocard(dataset, table, ar_path, config_path):

    config = EXPERIMENT_CONFIGS[config_path.format(dataset, table)]
    config['__run'] = config['workload_name'] = config_path.format(dataset, table)
    if os.path.exists(ar_path.format(dataset, table)):
        print('table', table, 'updated')
        config['checkpoint_to_load'] = ar_path.format(dataset, table)
    else:
        print('table', table, 'not updated')
        config['checkpoint_to_load'] = f"ASM/AR_models/{dataset}-single-{table}.tar"
    config['queries_csv'] = None
    config['__gpu'] = 1
    config['__cpu'] = 1
    config['external'] = False
    nc = NeuroCard(config)

    with open(f"{config['data_dir']}/min_count.pkl", "rb") as f:
        min_count = pickle.load(f)
    # min_count = pickle.load(open(f"{config['data_dir']}/min_count.pkl","rb"))
    nc.min_count = dict()
    for col in min_count:
        assert np.all(min_count[col] >= 1.), f'{np.where(min_count[col]) < 1.}'
        nc.min_count[col] = torch.tensor(min_count[col], device=nc.get_device())

    return nc

def load_bound_model(model_path, ar_path, config_path, sample_size, dataset):
    bound_ensemble = None

    with open(model_path, "rb") as f:
        bound_ensemble_nc = pickle.load(f)

    ncs = dict()
    for table_obj in bound_ensemble_nc.schema.tables:
        table = table_obj.table_name

        nc = load_neurocard(dataset, table, ar_path, config_path)
        use_raw_table = table_obj.table_size < 1000
        nc.ready_for_evaluate(use_raw_table)

        ncs[table] = nc

    bound_ensemble_nc.ncs = ncs

    bound_ensemble_nc.use_ar = True
    if sample_size is None:
        bound_ensemble_nc.sample_size = 2048
    else:
        bound_ensemble_nc.sample_size = sample_size

    return bound_ensemble_nc

def run_ASM_one(query,
        sub_plan,
        query_predicate,
        bound_ensemble_nc,
        get_Probability = None,
        optimizer = None,
        raw_card = False
        ):

    bound_ensemble_nc.query_predicate = query_predicate

    torch.manual_seed(0)

    if optimizer is not None:
        ests, raw_ests, c_time, m_time, s_time = bound_ensemble_nc.get_cardinality_bound_all(query, sub_plan, get_Probability=get_Probability, optimizer=optimizer)
    else:
        ests, c_time, m_time, s_time = bound_ensemble_nc.get_cardinality_bound_all(query, sub_plan, get_Probability=get_Probability)

    if optimizer is not None and raw_card:
        return ests, raw_ests
    else:
        return ests

def initialize_model(ncs, tables_alias, join_keys, schema,
                     sample_size,
                     data,
                     get_Probability=None):
    def evaluate_model(i, alias, table, pred, keys):
        nc = ncs[table]

        query = dict()

        if len(pred) > 0:
            tree = parse_logic_tree(pred, alias, table, schema)

            cols = fillcol(tree)

            to_neurocard_ops(tree)
            for col in cols:
                subtree = get_subtree(tree, col)
                query[col] = subtree

        if get_Probability is not None:
            with open(get_Probability, 'a') as f:
                print(f"evaluate_one_tree: {alias}, {table}", file=f)
        
        p, table_len, cur_idx, sample, logits, cols, ops, vals, dom_ops = nc.evaluate_one_tree(query, keys, sample_size, get_Probability=get_Probability)
        if not torch.is_tensor(p):
            p = torch.Tensor(p).to(nc.get_device())

        return p
    for i, alias in enumerate(data):
        (table, pred, keys) = data[alias]
        result_p = evaluate_model(i, alias, table, pred, keys)

    if hasattr(result_p, 'numel'):
        if result_p.numel() == 1:
            final_val = result_p.item()
        else:
            final_val = result_p.mean().item()
    else:
        final_val = result_p

    return final_val

def get_table_P(query, query_predicate, bound_ensemble_nc, get_Probability=None):
    bound_ensemble_nc.query_predicate = query_predicate
    tables_all, table_queries, join_cond, join_keys = bound_ensemble_nc.parse_query_simple(query)

    all_aliases = list(tables_all.keys())
    all_aliases.sort()

    equivalent_group, table_equivalent_group, table_key_equivalent_group, table_key_group_map = \
        get_join_hyper_graph(join_keys, bound_ensemble_nc.equivalent_keys, tables_all, join_cond)

    p = initialize_model(bound_ensemble_nc.ncs, tables_all, join_keys, bound_ensemble_nc.schema, sample_size=bound_ensemble_nc.sample_size, data=bound_ensemble_nc.query_predicate, get_Probability=get_Probability)

    return p

def preprocess_between(text):
    """
    Convert the BETWEEN syntax in SQL into the form of >= AND <=.
    For example: "t.year BETWEEN 1990 AND 2000" -> "t.year >= 1990 AND t.year <= 2000"
    """
    if not text:
        return text
    pattern = r'([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+?)(?=\s+AND\s+|\s*\)|$)'
    
    return re.sub(
        pattern, 
        r'\1 >= \2 AND \1 <= \3', 
        text, 
        flags=re.IGNORECASE | re.DOTALL
    )

def parse_sql_to_dict(sql):
    # 1. Preprocess the SQL and extract the FROM and WHERE parts
    sql = sql.strip().rstrip(';')
    
    # Extract the FROM part
    from_match = re.search(r'FROM\s+(.+?)\s+WHERE', sql, re.IGNORECASE | re.DOTALL)
    if not from_match:
        return None
    from_part = from_match.group(1)
    
    # Extract the WHERE part
    where_match = re.search(r'WHERE\s+(.*)', sql, re.IGNORECASE | re.DOTALL)
    where_part = where_match.group(1) if where_match else ""

    where_part = preprocess_between(where_part)

    # 2. Parse table aliases (Table Dictionary)
    # Structure: {'ct': {'table': 'company_type', 'filters': [], 'join_keys': set()}, ...}
    tables_info = {}
    table_list = [t.strip() for t in from_part.split(',')]
    for t_str in table_list:
        # Match either "table AS alias" or "table alias"
        parts = re.split(r'\s+AS\s+|\s+', t_str, flags=re.IGNORECASE)
        if len(parts) >= 2:
            real_name = parts[0].strip()
            alias = parts[-1].strip()
            tables_info[alias] = {
                'table': real_name,
                'filters': [],
                'join_keys': set()
            }

    # 3. Split WHERE conditions (handling nested parentheses)
    conditions = split_conditions(where_part)

    # 4. Traverse and classify conditions
    for cond in conditions:
        cond = cond.strip()
        if not cond: continue

        # Extract all occurrences of "alias.column" in the condition
        # Regex logic: match "alias.column", where the alias must be defined in FROM
        col_refs = re.findall(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', cond)
        
        valid_refs = [(alias, col) for alias, col in col_refs if alias in tables_info]
        involved_aliases = set(alias for alias, col in valid_refs)

        if len(involved_aliases) > 1:
            for alias, col in valid_refs:
                tables_info[alias]['join_keys'].add(col)
        elif len(involved_aliases) == 1:
            alias = list(involved_aliases)[0]
            tables_info[alias]['filters'].append(cond)
        else:
            pass

    # 5. Format the output
    result = {}
    for alias, info in tables_info.items():
        filter_str = " AND ".join(info['filters'])
        if filter_str:
            filter_str = f" {filter_str} "
        
        result[alias] = (
            info['table'],
            filter_str,
            info['join_keys']
        )

    return result

def split_conditions(text):
    """
    Intelligently split an SQL WHERE clause with support for nested parentheses.
    Split "A=1 AND (B=2 OR C=3) AND D=4" into ["A=1", "(B=2 OR C=3)", "D=4"].
    """
    conditions = []
    buffer = []
    depth = 0
    in_quote = False
    quote_char = None
    i = 0
    n = len(text)

    while i < n:
        char = text[i]

        # Handle quoted strings to avoid misinterpreting AND inside quotes
        if char in ("'", '"'):
            if not in_quote:
                in_quote = True
                quote_char = char
            elif char == quote_char:
                # Simple escape handling: two consecutive quotes are treated as an escape
                if i + 1 < n and text[i+1] == quote_char:
                    buffer.append(char)
                    i += 1 
                else:
                    in_quote = False
                    quote_char = None

        if not in_quote:
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
            elif depth == 0:
                # Check whether this is a top-level AND
                # Condition: current characters are A, N, D and surrounded by boundaries (spaces or parentheses)
                if text[i:i+3].upper() == 'AND':
                    # Check the left boundary
                    prev_char = text[i-1] if i > 0 else ' '
                    # Check the right boundary
                    next_char = text[i+3] if i + 3 < n else ' '
                    
                    if (prev_char.isspace() or prev_char == ')') and \
                       (next_char.isspace() or next_char == '('):
                        # Found a separator; save the current buffer
                        conditions.append("".join(buffer))
                        buffer = []
                        i += 3 # Skip AND
                        continue
        
        buffer.append(char)
        i += 1

    if buffer:
        conditions.append("".join(buffer))
    
    return conditions

if __name__ == "__main__":
    pass