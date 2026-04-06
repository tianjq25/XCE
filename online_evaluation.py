"""
Online Evaluation: ASM vs ASM+DecisionTree (Two-Phase)

Phase 1 – ASM estimates only (no execution):
    For each query, run pure ASM to generate cardinality estimates.
    Save to output_asm_estimates.

Phase 2 – ASM + Decision Tree (incrementally trained, async):
    For each query:
      1. Check if previous async training completed; if so, swap to new model.
      2. Execute with ASM+Tree to get estimates + actual rows (for training data).
      3. Run a *separate* pure-ASM call to get per-table predicted_P.
      4. Collect training data; every N queries, trigger async tree retrain.
      5. Queries continue with current model while training runs in background.
      6. Save ASM+Tree estimates to output_tree_estimates.

Output: two estimate files (ASM and ASM+Tree) for use on another server
        to test execution time separately. Format: query_idx: N, then one
        estimate per line per sub-plan.
"""

from codecs import raw_unicode_escape_decode
import os, sys, math, json, pickle, traceback, time, gc, random, pickle
import numpy as np
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import logging

os.environ['CUDA_VISIBLE_DEVICES'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'

import ASM.AR.common
sys.modules['common'] = ASM.AR.common

from sql_parser import SQLParser, parse_query_all_join
from sql_executor import SQLExecutor
from RegressionFramework.utils import read_sqls, read_sqls_with_names, flat_depth2_list
from RegressionFramework.RegressionFramework import RepariRegressionFramework
from RegressionFramework.Plan.Plan import json_str_to_json_obj, Plan
from RegressionFramework.NonShiftedModel.AdaptivaGroupTree import TreeNode
from RegressionFramework.StaticPlanGroup import Group
from run_ASM import load_bound_model, run_ASM_one, parse_sql_to_dict
from cardinality_extract import process_log_file
from docker_management import DockerFileHandler
from plan_handler import extract_join_order
from check_join import UnionFind

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def load_pretrain_training_data(path: str) -> List[Tuple[str, float]]:
    """Load (sql, repair_value) pairs from file. Format: sql#####repair_value per line."""
    pairs = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("#####")
            if len(parts) >= 2:
                sql, rv = parts[0], float(parts[1])
                pairs.append((sql, rv))
    return pairs


@dataclass
class EvalConfig:
    dataset: str = "imdb"
    user: str = "postgres"
    password: str = "postgres"
    host: str = "localhost"
    port: str = "30010"
    timeout_seconds: int = 3600

    container_name: str = "ce-benchmark"

    asm_model_path: str = "ASM/meta_models/model_imdb.pkl"
    asm_ar_path: str = "ASM/AR_models/{}-single-{}.tar"
    asm_config_path: str = "{}-single-{}_infer"
    asm_sample_size: int = 2048
    asm_dataset: str = "imdb"

    original_sqls_path: str = "Data/job4.txt"

    tree_update_interval: int = 10
    leaf_ele_min_count: int = 5

    global_threshold: float = 1.2
    global_threshold_le: float = 0.8

    output_plot: str = "online_eval_comparison.png"
    output_csv: str = "online_eval_results.csv"
    output_training_data: str = "online_eval_training_data.txt"

    output_asm_estimates: str = "online_eval_asm_estimates.txt"
    output_tree_estimates: str = "online_eval_tree_estimates.txt"

    random_seed: int = 42

    pretrain_training_data_path: Optional[str] = None


# ---------------------------------------------------------------------------
#  Extract actual rows from EXPLAIN ANALYZE JSON plan
# ---------------------------------------------------------------------------

def extract_actual_rows_from_plan(plan_node: dict) -> dict:
    """
    Returns dict: frozenset(aliases) -> {'actual_rows': int, 'plan_rows': int}
    """
    results = {}

    def get_alias(node):
        return node.get('Alias') or node.get('Relation Name')

    def visit(node):
        node_type = node.get('Node Type', '')
        actual_rows = node.get('Actual Rows')
        plan_rows = node.get('Plan Rows')

        if 'Scan' in node_type:
            alias = get_alias(node)
            if alias and actual_rows is not None:
                results[frozenset([alias])] = {
                    'actual_rows': actual_rows,
                    'plan_rows': plan_rows,
                }
            return {alias} if alias else set()

        if 'Plans' in node:
            plans = node['Plans']
            if len(plans) == 1:
                return visit(plans[0])
            elif len(plans) == 2:
                left_tables = visit(plans[0])
                right_tables = visit(plans[1])
                combined = left_tables | right_tables
                if actual_rows is not None:
                    results[frozenset(combined)] = {
                        'actual_rows': actual_rows,
                        'plan_rows': plan_rows,
                    }
                return combined
        return set()

    visit(plan_node)
    return results


# ---------------------------------------------------------------------------
#  ASM estimate lookup
# ---------------------------------------------------------------------------

def build_asm_estimate_map(sub_plan_queries, join_estimates):
    """Map frozenset(aliases) -> ASM estimated cardinality."""
    est_map = {}
    for (left_t, right_t), est in zip(sub_plan_queries, join_estimates):
        aliases = sorted(right_t.strip().split(' ') + [left_t.strip()])
        est_map[frozenset(aliases)] = est
    return est_map


# ---------------------------------------------------------------------------
#  Lazy sub-query SQL reconstruction
# ---------------------------------------------------------------------------

def _reconstruct_join_sql(alias_set, tables, filters, join_cond, parser):
    """Reconstruct the SQL for a specific set of joined aliases.
    Called on-demand only when the repair_value passes the threshold.
    """
    all_aliases = sorted(alias_set)
    join_tables = {a: tables[a] for a in all_aliases}
    join_filters = {a: filters.get(a, []) for a in all_aliases}

    uf = UnionFind()
    tmp_conds = []
    for _i, t in enumerate(all_aliases):
        for cond in join_cond.get(t, []):
            lc, rc = cond.split('=')
            lc, rc = lc.strip(), rc.strip()
            lt, rt = lc.split('.')[0], rc.split('.')[0]
            if (lt == t and rt in all_aliases[_i+1:]) or \
               (rt == t and lt in all_aliases[_i+1:]):
                if uf.check_connected(lc, rc):
                    continue
                uf.union(lc, rc)
                tmp_conds.append(cond)

    return parser.reconstruct_sql(join_tables, join_filters, tmp_conds)


# ---------------------------------------------------------------------------
#  Collect training data from a single executed query
# ---------------------------------------------------------------------------

def collect_training_data(
    cleaned_sql: str,
    parser: SQLParser,
    sub_plan_queries: list,
    join_estimates: list,
    join_steps: list,
    actual_rows_map: dict,
    cardinality_info: dict,
    table_total_cache: dict,
    global_threshold: float,
    global_threshold_le: float,
) -> List[Tuple[str, float]]:
    """
    Extract (sql, repair_value) pairs from a single query execution.

    Single-table:  repair_value = actual_rows / (predicted_P * table_total)
    Multi-table:   repair_value = actual_rows / asm_join_estimate

    SQL reconstruction is deferred until *after* the repair_value passes
    the importance threshold.

    table_total_cache must be pre-populated (no DB calls happen here).
    """
    pairs = []
    tables, filters, joins = parser.parse(cleaned_sql)

    est_map = build_asm_estimate_map(sub_plan_queries, join_estimates)

    join_cond = None

    pred_P_by_alias = {}
    if cardinality_info and 'tables' in cardinality_info:
        for tbl in cardinality_info['tables']:
            pred_P_by_alias[tbl['alias']] = tbl.get('final_prob')

    def is_important(rv):
        # return rv >= global_threshold or rv <= global_threshold_le
        return rv >= global_threshold

    # --- Single-table sub-queries ---
    for alias in tables:
        if not filters.get(alias):
            continue
        key = frozenset([alias])
        if key not in actual_rows_map:
            continue

        actual_rows = actual_rows_map[key]['actual_rows']

        table_name = tables[alias]
        table_total = table_total_cache.get(table_name, 0)
        if table_total <= 0:
            continue

        predicted_P = pred_P_by_alias.get(alias)
        if predicted_P is None or predicted_P <= 0:
            continue

        repair_value = actual_rows / (predicted_P * table_total)
        if is_important(repair_value):
            sub_sql = parser.reconstruct_sql(
                {alias: table_name},
                {alias: filters.get(alias, [])}, {})
            pairs.append((sub_sql, repair_value))

    # --- Multi-table join sub-queries ---
    for alias_set, info in actual_rows_map.items():
        if len(alias_set) <= 1:
            continue

        actual_rows = info['actual_rows']
        est = est_map.get(alias_set)
        if est is None or est <= 0:
            continue

        repair_value = actual_rows / est
        if is_important(repair_value):
            if join_cond is None:
                _, join_cond, _ = parse_query_all_join(cleaned_sql)
            sub_sql = _reconstruct_join_sql(
                alias_set, tables, filters, join_cond, parser)
            pairs.append((sub_sql, repair_value))

    # --- Full query ---
    full_key = frozenset(tables.keys())
    if full_key in actual_rows_map and len(join_estimates) > 0:
        actual_rows = actual_rows_map[full_key]['actual_rows']
        est = join_estimates[-1]
        if est > 0:
            repair_value = actual_rows / est
            if is_important(repair_value):
                full_sql = parser.reconstruct_sql(tables, filters, joins)
                pairs.append((full_sql, repair_value))

    return pairs


# ---------------------------------------------------------------------------
#  Decision tree trainer with plan cache
# ---------------------------------------------------------------------------

class IncrementalTreeTrainer:
    def __init__(self, config: EvalConfig, executor: SQLExecutor, parser: SQLParser):
        self.config = config
        self.executor = executor
        self.parser = parser
        self.all_training_data: List[Tuple[str, float]] = []
        self.framework: Optional[RepariRegressionFramework] = None
        self._plan_cache: Dict[str, list] = {}
        self._data_lock = threading.Lock()

    def add_training_data(self, data: List[Tuple[str, float]]):
        with self._data_lock:
            self.all_training_data.extend(data)

    def prefetch_plans_for_training(self, data: Optional[List[Tuple[str, float]]] = None) -> None:
        """
        Pre-fetch all uncached plans in the main thread before async training.
        This avoids the training thread using the executor (not thread-safe).
        Must be called from the main thread before submit(train).
        If data is provided, use it; else use all_training_data (with lock).
        """
        if data is not None:
            train_sqls = [sql for sql, _ in data]
        else:
            with self._data_lock:
                train_sqls = [sql for sql, _ in self.all_training_data]
        uncached = {sql for sql in train_sqls if sql not in self._plan_cache}
        if uncached:
            logger.info(f"[Phase2] Pre-fetching {len(uncached)} plans for async training...")
            for sql in uncached:
                try:
                    plan = self.executor.convert_sql_to_plan_without_execute(sql)
                    self._plan_cache[sql] = [json_str_to_json_obj(p) for p in plan]
                except Exception as e:
                    logger.warning(f"Plan fetch failed: {e}")

    def train(self, snapshot: Optional[List[Tuple[str, float]]] = None) -> float:
        """
        Train the decision tree. Returns wall-clock time spent (seconds).
        Must be called after prefetch_plans_for_training() when used async,
        so it never touches the executor (executor is not thread-safe).

        If snapshot is provided, train on it (ensures no data added during training).
        Else take snapshot under lock (for pretrain / single-thread use).
        """
        if snapshot is None:
            with self._data_lock:
                snapshot = list(self.all_training_data)
        if len(snapshot) == 0:
            logger.warning("No training data to build tree.")
            return 0.0

        t_start = time.time()

        train_sqls = [sql for sql, _ in snapshot]
        repair_values = [rv for _, rv in snapshot]

        logger.info(f"Training decision tree with {len(train_sqls)} samples...")

        try:
            # Plans must be pre-fetched by prefetch_plans_for_training() before async submit.
            # Do NOT call executor here - it is shared and not thread-safe.
            #
            # TreeNode.tree_nodes/Group.all_groups are cleared below; the OLD framework
            # (used by main thread for inference) traverses via root->child refs, not
            # these globals, so it remains safe.

            final_sqls = []
            final_plans = []
            final_rvs = []
            for sql, rv in zip(train_sqls, repair_values):
                cached = self._plan_cache.get(sql)
                if cached is not None:
                    final_sqls.append(sql)
                    final_plans.append(cached)
                    final_rvs.append(rv)

            if len(final_sqls) == 0:
                logger.warning("No valid plans for training.")
                return time.time() - t_start

            stamped_plans = []
            for plans, rv in zip(final_plans, final_rvs):
                query_plans = []
                log_rv = math.log(rv) if rv > 0 else math.log(1e-8)
                for p in plans:
                    p_copy = {**p, "metric": log_rv, "predicate": log_rv}
                    query_plans.append(p_copy)
                stamped_plans.append(query_plans)

            TreeNode.tree_nodes.clear()
            Group.all_groups.clear()
            Group.id = 0
            Plan.total_plan_id = 0

            self.framework = RepariRegressionFramework(
                flat_depth2_list(stamped_plans), final_sqls, self.config.dataset,
                mode="static",
                plans_for_queries=stamped_plans
            )
            self.framework.build()
            logger.info("Decision tree trained successfully.")
        except Exception as e:
            logger.error(f"Failed to train decision tree: {e}")
            traceback.print_exc()
            self.framework = None

        elapsed = time.time() - t_start
        logger.info(f"Tree training took {elapsed:.2f}s")
        return elapsed


# ---------------------------------------------------------------------------
#  Query execution helper
# ---------------------------------------------------------------------------

class QueryRunner:
    def __init__(self, config: EvalConfig, executor: SQLExecutor,
                 parser: SQLParser, docker_handler: DockerFileHandler,
                 bound_ensemble_nc):
        self.config = config
        self.executor = executor
        self.parser = parser
        self.docker_handler = docker_handler
        self.bound_ensemble_nc = bound_ensemble_nc
        self._prob_file_counter = 0
        self._prob_counter_lock = threading.Lock()

    def run_query(self, cleaned_sql: str, sub_plan_queries: list,
                  query_predicate: dict, optimizer=None,
                  capture_probability=False):
        """
        Run ASM (optionally with optimizer/tree), inject estimates into PG,
        execute with EXPLAIN ANALYZE, and return timing + plan details.

        Returns:
            execution_time_ms, planning_time_ms,
            join_estimates, join_steps, actual_rows_map,
            cardinality_info (parsed probability log, or None)
        """
        prob_file = None
        cardinality_info = None

        if capture_probability:
            with self._prob_counter_lock:
                self._prob_file_counter += 1
                prob_file = f"_online_eval_prob_{self._prob_file_counter}.txt"
            if os.path.exists(prob_file):
                os.remove(prob_file)
            with open(prob_file, 'w') as f:
                f.write("query: q0\n")

        if optimizer is not None:
            join_estimates, raw_join_estimates = run_ASM_one(
                query=cleaned_sql,
                sub_plan=sub_plan_queries,
                query_predicate=query_predicate,
                bound_ensemble_nc=self.bound_ensemble_nc,
                get_Probability=prob_file,
                optimizer=optimizer,
                raw_card=True
            )
        else:
            join_estimates = run_ASM_one(
                query=cleaned_sql,
                sub_plan=sub_plan_queries,
                query_predicate=query_predicate,
                bound_ensemble_nc=self.bound_ensemble_nc,
                get_Probability=prob_file
            )

        if prob_file is not None:
            parsed = process_log_file(prob_file)
            if parsed:
                for record in parsed:
                    if record['query_id'] == 'q0':
                        cardinality_info = record
                        break
            try:
                os.remove(prob_file)
            except Exception:
                pass

        write_str = "".join(str(est) + "\n" for est in join_estimates)
        method_name = "online_eval_est.txt"
        self.docker_handler.write_file(method_name, write_str)
        self.executor.set_optimize(method_name)

        planning_time_ms = 0.0
        execution_time_ms = 0.0
        actual_plan_json = None

        try:
            self.executor.cursor.execute(
                "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) " + cleaned_sql)
            res = self.executor.cursor.fetchall()
            actual_plan_json = res[0][0][0]
            planning_time_ms = actual_plan_json.get('Planning Time', 0.0)
            execution_time_ms = actual_plan_json.get('Execution Time', 0.0)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            try:
                self.executor.conn.rollback()
            except Exception:
                pass

        self.executor.reset_optimize()

        actual_rows_map = {}
        join_steps = []
        if actual_plan_json is not None:
            plan_root = actual_plan_json.get('Plan', {})
            actual_rows_map = extract_actual_rows_from_plan(plan_root)
            join_steps = extract_join_order(plan_root)

        return (execution_time_ms, planning_time_ms, join_estimates, raw_join_estimates if optimizer is not None else join_estimates,
                join_steps, actual_rows_map, cardinality_info)

    def run_asm_only(self, cleaned_sql: str, sub_plan_queries: list,
                     query_predicate: dict):
        """Run pure ASM (no execution) to get estimates and probability info.

        Used in Phase 2 to obtain ASM estimates for training data collection
        without actually executing the query. Thread-safe for async collection.
        """
        with self._prob_counter_lock:
            self._prob_file_counter += 1
            prob_file = f"_online_eval_prob_{self._prob_file_counter}.txt"
        if os.path.exists(prob_file):
            os.remove(prob_file)
        with open(prob_file, 'w') as f:
            f.write("query: q0\n")

        join_estimates = run_ASM_one(
            query=cleaned_sql,
            sub_plan=sub_plan_queries,
            query_predicate=query_predicate,
            bound_ensemble_nc=self.bound_ensemble_nc,
            get_Probability=prob_file,
            optimizer=None
        )

        cardinality_info = None
        parsed = process_log_file(prob_file)
        if parsed:
            for record in parsed:
                if record['query_id'] == 'q0':
                    cardinality_info = record
                    break
        try:
            os.remove(prob_file)
        except Exception:
            pass

        return join_estimates, cardinality_info


# ---------------------------------------------------------------------------
#  Main evaluation pipeline (two-phase)
# ---------------------------------------------------------------------------

class OnlineEvaluation:
    def __init__(self, config: EvalConfig):
        self.config = config

        logger.info("Initializing online evaluation...")

        self.docker_handler = DockerFileHandler(container_name_or_id=config.container_name)
        self.parser = SQLParser()
        self.executor = SQLExecutor(
            dataset=config.dataset,
            user=config.user,
            password=config.password,
            host=config.host,
            port=config.port,
            docker_file_handler=self.docker_handler,
            timeout_seconds=config.timeout_seconds
        )

        logger.info("Loading ASM model...")
        self.bound_ensemble_nc = load_bound_model(
            model_path=config.asm_model_path,
            ar_path=config.asm_ar_path,
            config_path=config.asm_config_path,
            sample_size=config.asm_sample_size,
            dataset=config.asm_dataset
        )

        self.query_runner = QueryRunner(
            config, self.executor, self.parser,
            self.docker_handler, self.bound_ensemble_nc
        )
        self.tree_trainer = IncrementalTreeTrainer(config, self.executor, self.parser)
        self.table_total_cache: Dict[str, int] = {}

        # Async training: background executor + lock for optimizer swap
        self._training_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tree_train")
        self._training_future: Optional[Future] = None
        self._optimizer_lock = threading.Lock()

        # Async training data collection: overlap collection with next query execution
        self._collection_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="collect")
        self._collection_futures: List[Future] = []

        logger.info("Online evaluation initialized.")

    def _preload_table_totals(self, sqls: List[str]):
        """Pre-fetch row counts for all tables referenced in the query set."""
        all_table_names = set()
        for sql in sqls:
            tables, _, _ = self.parser.parse(sql)
            all_table_names.update(tables.values())

        logger.info(f"Pre-loading row counts for {len(all_table_names)} tables...")
        for table_name in all_table_names:
            if table_name in self.table_total_cache:
                continue
            try:
                res = self.executor.execute_query(f"SELECT COUNT(*) FROM {table_name}")
                if res is not None:
                    self.table_total_cache[table_name] = res[0][0]
            except Exception as e:
                logger.warning(f"Failed to get row count for {table_name}: {e}")
        logger.info(f"Table total cache populated: {len(self.table_total_cache)} tables")

    def _wait_collection_futures(self):
        """Wait for all pending async training data collection tasks."""
        if not self._collection_futures:
            return
        for f in self._collection_futures:
            try:
                f.result(timeout=300)
            except Exception as e:
                logger.error(f"[Phase2] Collection task failed: {e}")
        self._collection_futures.clear()

    def _get_sub_plan_queries(self, cleaned_sql):
        self.docker_handler.clear_file('join_est_record_job.txt')
        self.executor.cursor.execute('SET print_sub_queries=true;')
        self.executor.cursor.execute("SET query_no=0;")
        self.executor.cursor.execute("EXPLAIN (FORMAT JSON) " + cleaned_sql)
        self.executor.cursor.execute('SET print_sub_queries=false;')
        all_sub = self.executor.read_subquery()
        return all_sub.get('query_0', [])

    # ------------------------------------------------------------------
    #  Phase 1: ASM estimates only (no execution)
    # ------------------------------------------------------------------
    def _run_phase1(self, cleaned_sqls, sub_plan_cache, predicate_cache):
        """Generate ASM cardinality estimates for each query. No execution."""
        n = len(cleaned_sqls)
        asm_estimates_all = []

        for idx, cleaned_sql in enumerate(cleaned_sqls):
            q_name = self.q_names[idx]
            logger.info(f"[Phase1] === Query {idx}/{n} q_name={q_name} (ASM) ===")

            sub_plan_queries = sub_plan_cache[idx]
            query_predicate = predicate_cache[idx]

            if query_predicate is None:
                logger.warning(f"[Phase1] Skipping query {idx} ({q_name}): failed to parse")
                asm_estimates_all.append([])
                continue

            try:
                asm_ests, _ = self.query_runner.run_asm_only(
                    cleaned_sql, sub_plan_queries, query_predicate)
                asm_estimates_all.append(asm_ests)
            except Exception as e:
                logger.error(f"[Phase1] Query {idx} ({q_name}) failed: {e}")
                traceback.print_exc()
                asm_estimates_all.append([])

        return asm_estimates_all

    # ------------------------------------------------------------------
    #  Phase 2: ASM + Decision Tree (incremental, async training)
    # ------------------------------------------------------------------
    def _run_phase2(self, cleaned_sqls, sub_plan_cache, predicate_cache,
                    initial_optimizer=None):
        """Run every query with ASM+Tree, collect estimates and training data.

        Decision tree training is async: when interval is reached, training starts
        in background while queries continue with the previous model. After each
        query, check if training is done and swap to the new model if ready.

        initial_optimizer: optional (framework, executor, parser) from pretrain.
        Returns list of (tree_estimates per query).
        """
        n = len(cleaned_sqls)
        tree_estimates_all = []

        with self._optimizer_lock:
            optimizer = initial_optimizer
        queries_since_last_train = 0

        for idx, cleaned_sql in enumerate(cleaned_sqls):
            q_name = self.q_names[idx]
            logger.info(f"[Phase2] === Query {idx}/{n} q_name={q_name} (ASM+Tree) ===")

            # ---- Check if async training completed; swap optimizer if ready ----
            if self._training_future is not None and self._training_future.done():
                try:
                    self._training_future.result()
                    if self.tree_trainer.framework is not None:
                        with self._optimizer_lock:
                            optimizer = (self.tree_trainer.framework,
                                         self.executor, self.parser)
                        logger.info(f"[Phase2] Async training done, optimizer swapped to new tree (before query {q_name}).")
                        self._save_snapshot(q_name)
                    else:
                        logger.warning("[Phase2] Async training finished but framework is None.")
                except Exception as e:
                    logger.error(f"[Phase2] Async training failed: {e}")
                    traceback.print_exc()
                finally:
                    self._training_future = None

            sub_plan_queries = sub_plan_cache[idx]
            query_predicate = predicate_cache[idx]

            if query_predicate is None:
                logger.warning(f"[Phase2] Skipping query {idx} ({q_name}): failed to parse")
                tree_estimates_all.append([])
                continue

            # ---- Execute with ASM+Tree (needed for actual_rows for training data) ----
            # Uses current optimizer (may be old model if training still in progress)
            try:
                with self._optimizer_lock:
                    current_optimizer = optimizer
                (_, _, tree_ests, raw_asm_ests, tree_steps,
                 tree_actual, cardinality_info) = self.query_runner.run_query(
                    cleaned_sql, sub_plan_queries, query_predicate,
                    optimizer=current_optimizer, capture_probability=True)
                tree_estimates_all.append(tree_ests)
            except Exception as e:
                logger.error(f"[Phase2] ASM+Tree run failed for query {idx} ({q_name}): {e}")
                traceback.print_exc()
                tree_ests, tree_steps, tree_actual = [], [], {}
                raw_asm_ests = []
                cardinality_info = None 
                tree_estimates_all.append([])

            # ---- Submit async training data collection (overlaps with next query) ----
            # 注意这里：通过默认参数把当前循环的值锁死
            def _do_collect(
                c_sql=cleaned_sql,
                s_queries=sub_plan_queries,
                r_ests=raw_asm_ests,
                t_steps=tree_steps,
                t_actual=tree_actual,
                c_info=cardinality_info
            ):
                try:
                    new_pairs = collect_training_data(
                        c_sql, self.parser,  # 使用绑定的局部变量 c_sql
                        s_queries, r_ests, t_steps,
                        t_actual, c_info,
                        self.table_total_cache,
                        self.config.global_threshold,
                        self.config.global_threshold_le,
                    )
                    if new_pairs:
                        self.tree_trainer.add_training_data(new_pairs)
                        with self.tree_trainer._data_lock:
                            total = len(self.tree_trainer.all_training_data)
                        logger.info(
                            f"[Phase2] Async collected {len(new_pairs)} pairs (total: {total})")
                except Exception as e:
                    logger.error(f"[Phase2] Async training data collection failed: {e}")
                    traceback.print_exc()

            self._collection_futures.append(
                self._collection_executor.submit(_do_collect))
            # def _do_collect():
            #     try:
            #         new_pairs = collect_training_data(
            #             cleaned_sql, self.parser,
            #             sub_plan_queries, raw_asm_ests, tree_steps,
            #             tree_actual, cardinality_info,
            #             self.table_total_cache,
            #             self.config.global_threshold,
            #             self.config.global_threshold_le,
            #         )
            #         if new_pairs:
            #             self.tree_trainer.add_training_data(new_pairs)
            #             with self.tree_trainer._data_lock:
            #                 total = len(self.tree_trainer.all_training_data)
            #             logger.info(
            #                 f"[Phase2] Async collected {len(new_pairs)} pairs (total: {total})")
            #     except Exception as e:
            #         logger.error(f"[Phase2] Async training data collection failed: {e}")
            #         traceback.print_exc()

            # self._collection_futures.append(
            #     self._collection_executor.submit(_do_collect))

            # ---- Periodically trigger async retrain (non-blocking) ----
            queries_since_last_train += 1
            if queries_since_last_train >= self.config.tree_update_interval:
                queries_since_last_train = 0
                # Use existing data directly; snapshot under lock so training sees fixed data
                with self.tree_trainer._data_lock:
                    snapshot = list(self.tree_trainer.all_training_data)
                if len(snapshot) > 0:
                    if self._training_future is None or self._training_future.done():
                        # Pre-fetch plans in main thread before submit: executor is not thread-safe.
                        self.tree_trainer.prefetch_plans_for_training(snapshot)
                        logger.info("[Phase2] Triggering async decision tree training (queries continue with current model)...")
                        self._training_future = self._training_executor.submit(
                            self.tree_trainer.train, snapshot)
                    else:
                        logger.info("[Phase2] Training already in progress, skipping this interval.")

        # ---- Wait for all collection and any in-flight training before returning ----
        self._wait_collection_futures()
        if self._training_future is not None:
            logger.info("[Phase2] Waiting for final async training to complete...")
            try:
                self._training_future.result(timeout=3600)
                if self.tree_trainer.framework is not None:
                    with self._optimizer_lock:
                        optimizer = (self.tree_trainer.framework,
                                     self.executor, self.parser)
                    logger.info("[Phase2] Final training done, optimizer updated.")
                    self._save_snapshot("final")
            except Exception as e:
                logger.error(f"[Phase2] Final training failed: {e}")
            finally:
                self._training_future = None

        return tree_estimates_all

    # ------------------------------------------------------------------
    #  Main entry point
    # ------------------------------------------------------------------
    def run(self, sqls: Optional[List[str]] = None,
            q_names: Optional[List[str]] = None):
        if sqls is None:
            q_names, sqls = read_sqls_with_names(self.config.original_sqls_path)

        if q_names is None:
            q_names = [f"q{i}" for i in range(len(sqls))]

        assert len(q_names) == len(sqls), \
            f"q_names ({len(q_names)}) and sqls ({len(sqls)}) must have the same length"

        paired = list(zip(q_names, sqls))
        rng = random.Random(self.config.random_seed)
        rng.shuffle(paired)
        q_names, sqls = zip(*paired) if paired else ([], [])
        q_names, sqls = list(q_names), list(sqls)

        logger.info(f"Shuffled queries with seed={self.config.random_seed} (template-adjacent queries dispersed)")
        logger.info(f"Query order after shuffle: {q_names}")

        self.q_names = q_names

        order_path = "online_eval_query_order.json"
        with open(order_path, "w") as f:
            json.dump(q_names, f, indent=2)
        logger.info(f"Query order saved to {order_path}")

        n = len(sqls)
        logger.info(f"Starting online evaluation with {n} queries")

        cleaned_sqls = []
        for sql in sqls:
            td, fi, jo = self.parser.parse(sql)
            cleaned_sqls.append(self.parser.reconstruct_sql(td, fi, jo))

        self._preload_table_totals(cleaned_sqls)

        # Pre-compute sub_plan_queries and predicates (shared by both phases)
        logger.info("Pre-computing sub-plan queries and predicates...")
        sub_plan_cache = []
        predicate_cache = []
        for idx, cleaned_sql in enumerate(cleaned_sqls):
            sub_plan_queries = self._get_sub_plan_queries(cleaned_sql)
            query_predicate = parse_sql_to_dict(cleaned_sql)
            sub_plan_cache.append(sub_plan_queries)
            predicate_cache.append(query_predicate)

        # Load pretrain data and train initial tree (if configured)
        initial_optimizer = None
        if self.config.pretrain_training_data_path and os.path.exists(self.config.pretrain_training_data_path):
            logger.info(f"Loading pretrain data from {self.config.pretrain_training_data_path}...")
            pretrain_pairs = load_pretrain_training_data(self.config.pretrain_training_data_path)
            self.tree_trainer.add_training_data(pretrain_pairs)
            logger.info(f"Loaded {len(pretrain_pairs)} pretrain pairs (total: {len(self.tree_trainer.all_training_data)})")
            logger.info("Training initial decision tree from pretrain data...")
            self.tree_trainer.prefetch_plans_for_training()
            self.tree_trainer.train()
            if self.tree_trainer.framework is not None:
                initial_optimizer = (self.tree_trainer.framework, self.executor, self.parser)
                logger.info("Initial optimizer (ASM+Tree) ready from pretrain.")
                with open("online_eval_framework.pkl", "wb") as f:
                    pickle.dump(self.tree_trainer.framework, f)

            else:
                logger.warning("Pretrain tree training failed, starting with empty optimizer.")

        # Phase 1: ASM estimates only
        # logger.info("=" * 60)
        # logger.info("Phase 1: Generating ASM cardinality estimates")
        # logger.info("=" * 60)
        # asm_estimates_all = self._run_phase1(cleaned_sqls, sub_plan_cache, predicate_cache)

        # Phase 2: ASM + Decision Tree estimates + training data
        logger.info("=" * 60)
        logger.info("Phase 2: Generating ASM+Tree estimates (with incremental training)")
        logger.info("=" * 60)
        tree_estimates_all = self._run_phase2(
            cleaned_sqls, sub_plan_cache, predicate_cache,
            initial_optimizer=initial_optimizer)

        # self._save_estimates(asm_estimates_all, tree_estimates_all)
        # self._save_estimates(tree_estimates_all = tree_estimates_all)
        # self._save_training_data()
        # self._log_model_size()
        logger.info("Online evaluation finished.")

    def _save_snapshot(self, q_name: str):
        """Save a decision tree snapshot tagged with the q_name that triggered it."""
        if self.tree_trainer.framework is None:
            return
        snap_dir = os.path.join("Server", "demo", "snap_shots")
        os.makedirs(snap_dir, exist_ok=True)
        path = os.path.join(snap_dir, f"online_{q_name}.pkl")
        try:
            with open(path, "wb") as f:
                pickle.dump(self.tree_trainer.framework, f)
            logger.info(f"Snapshot saved: {path}")
        except Exception as e:
            logger.error(f"Failed to save snapshot {path}: {e}")

    # ------------------------------------------------------------------
    def _save_estimates(self, asm_estimates_all = None, tree_estimates_all = None):
        """Save ASM and ASM+Tree cardinality estimates to two files.

        Format per query: 'query_idx: N' followed by one estimate per line.
        The other server can parse and use these for execution-time testing.
        """
        def write_estimates_file(path, estimates_all):
            with open(path, 'w') as f:
                for idx, ests in enumerate(estimates_all):
                    # f.write(f"query_idx: {idx}\n")
                    for e in ests:
                        f.write(f"{e}\n")
            logger.info(f"Estimates saved to {path} ({len(estimates_all)} queries)")

        if asm_estimates_all is not None:
            write_estimates_file(self.config.output_asm_estimates, asm_estimates_all)
        if tree_estimates_all is not None:
            write_estimates_file(self.config.output_tree_estimates, tree_estimates_all)

    def _log_model_size(self):
        """Log the size of the final decision tree model in bytes and MB."""
        size_bytes = self.get_model_size_bytes()
        if size_bytes is None:
            logger.info("Final model size: N/A (no model trained)")
            return
        size_mb = size_bytes / (1024 * 1024)
        logger.info(f"Final model size: {size_bytes:,} bytes ({size_mb:.4f} MB)")

    def get_model_size_bytes(self) -> Optional[int]:
        """Return the size of the current decision tree model in bytes, or None if no model."""
        if self.tree_trainer.framework is None:
            return None
        try:
            return len(pickle.dumps(self.tree_trainer.framework))
        except Exception:
            return None

    def _save_training_data(self):
        with open(self.config.output_training_data, 'w') as f:
            for sql, rv in self.tree_trainer.all_training_data:
                f.write(f"{sql}#####{rv}\n")
        logger.info(
            f"Training data saved to {self.config.output_training_data} "
            f"({len(self.tree_trainer.all_training_data)} entries)")

    def shutdown(self):
        self._wait_collection_futures()
        if self._training_future is not None:
            try:
                self._training_future.result(timeout=5)
            except Exception:
                pass
        self._training_executor.shutdown(wait=True)
        self._collection_executor.shutdown(wait=True)
        self.executor.close()


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    config = EvalConfig(
        dataset="imdb",
        user="postgres",
        password="postgres",
        host="localhost",
        port="30010",
        timeout_seconds=3600,
        container_name="ce-benchmark",

        asm_model_path="ASM/meta_models/model_imdb.pkl",
        asm_ar_path="ASM/AR_models/{}-single-{}.tar",
        asm_config_path="{}-single-{}_infer",
        asm_sample_size=2048,
        asm_dataset="imdb",

        original_sqls_path="Data/job_test.txt",
        # pretrain_training_data_path="training_dataset_imdb.txt",

        tree_update_interval=10,
        leaf_ele_min_count=5,
        global_threshold=1.2,
        global_threshold_le=0.8,

        output_plot="online_eval_comparison.png",
        output_csv="online_eval_results.csv",
        output_training_data="online_eval_training_data.txt",
        output_asm_estimates="online_eval_asm_estimates_test.txt",
        output_tree_estimates="online_eval_XCE_estimates_test.txt",
    )

    evaluator = OnlineEvaluation(config)
    try:
        evaluator.run()
    finally:
        evaluator.shutdown()
