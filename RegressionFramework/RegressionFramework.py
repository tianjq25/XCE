import copy
import random

import joblib

from RegressionFramework.NonShiftedModel.AdaptivaGroupTree import TreeNode
from RegressionFramework.NonShiftedModel.AdaptivePlanGroup import AdaptivePlanGroup, AdaptivePlanGroupOptimization
from RegressionFramework.NonShiftedModel.PlansManeger import PlansManager
from RegressionFramework.Plan.Plan import Plan
from RegressionFramework.Plan.PlanFactory import PlanFactory
from RegressionFramework.ShiftedPlanProducer.ShiftedManager import LeroShiftedManager, \
    PerfguardShiftedManager, HyperqoShiftedManager
from RegressionFramework.StaticPlanGroup import Group, StaticConfig
from RegressionFramework.utils import flat_depth2_list
# from Perfguard.plan2score import get_perfguard_result
from RegressionFramework.NonShiftedModel.AdaptivaGroupAction import (
    Action, NumRangeAction, RangeAction, FilterColAction, FilterOpAction,
    FilterValueAction, JoinKeyAction, JoinTypeAction, TableTypeAction,
    TableNameAction, ProjectAction
)

from copy import deepcopy
from col_handler import ColHandler
from RegressionFramework.config import db_node_config

class RegressionFramework:
    def __init__(self, plans, sqls, db, algo, mode, leaf_ele_min_count=5, forest=100,
                 plans_for_queries=None):
        self._plans = plans
        self.plans_for_queries = copy.copy(plans_for_queries)
        # only used in single model
        self._sqls = sqls
        self.db = db
        self.algo = algo

        # static or dynamic
        self.mode = mode

        plan_idx = 0
        if plans_for_queries is not None and len(plans_for_queries) > 0 and len(plans_for_queries[0]) > 0 and type(plans_for_queries[0][0]) != Plan:
            for i in range(len(plans_for_queries)):
                plan_objects = []
                for plan in plans_for_queries[i]:
                    plan_objects.append(self._to_plan_object(plan, plan_idx, None))
                    plan_idx += 1
                self.plans_for_queries[i] = plan_objects
            self._plans = flat_depth2_list(self.plans_for_queries)
        else:
            if type(self._plans[0]) != Plan:
                self._plans = self._to_plan_objects(self._plans)

        self.shifted_manager = None

        self.iod_models = []

        self.leaf_ele_min_count = leaf_ele_min_count
        self.forest_pos = forest

    def iod_model_factory(self, min_ele_count, static_config):
        return AdaptivePlanGroup(min_ele_count=min_ele_count, static_config=static_config)

    def clear_plan_for_cal_memory(self, model: AdaptivePlanGroup):
        for group in Group.all_groups:
            group: Group = group
            group.plans = []
            group.ratios = group.ratios[0:2]
        managers = model.key_to_plan_manager.values()
        for manager in managers:
            manager: PlansManager = manager
            manager.plans = []
            manager.clear()

        for tree_node in TreeNode.tree_nodes:
            tree_node: TreeNode = tree_node
            tree_node.actions = []
        model.key_to_plan_manager = {}

    def build(self):
        # self.shifted_manager.build(self._plans, self._sqls)
        self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
                                                      static_config=StaticConfig()))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(scan_type_enable=True,
        #                                                                          table_name_enable=True)))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(scan_type_enable=True,
        #                                                                          table_name_enable=True,
        #                                                                          join_type_enable=True)))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(scan_type_enable=True,
        #                                                                          table_name_enable=True,
        #                                                                          join_type_enable=True,
        #                                                                          join_key_enable=True)))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(scan_type_enable=True,
        #                                                                          table_name_enable=True,
        #                                                                          join_type_enable=True,
        #                                                                          join_key_enable=True,
        #                                                                          filter_enable=True,
        #                                                                          filter_col_enable=True)))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(join_key_enable=True)))
        # self.iod_models.append(self.iod_model_factory(min_ele_count=self.leaf_ele_min_count,
        #                                               static_config=StaticConfig(join_key_enable=True, filter_enable=True, filter_col_enable=True)))

        # if self.forest_pos < 5:
        #     self.iod_models = [self.iod_models[self.forest_pos - 1]]

        self._build_iod_model()

    def _build_iod_model(self):
        for iod_model in self.iod_models:
            iod_model.build(self._plans)

    def _to_plan_objects(self, plans, predicts=None):
        objects = []
        for i, plan in enumerate(plans):
            objects.append(self._to_plan_object(plan, i, predicts[i] if predicts is not None else None))
        return objects

    def _to_plan_object(self, plan, idx=None, predict=None):
        return PlanFactory.get_plan_instance("pg", plan, idx, predict)

    def evaluate(self, plan1, plan2=None, predict=None, ood_thres=None):
        raise RuntimeError

    def _confidence_for_new_structure(self):
        raise RuntimeError

class RepariRegressionFramework(RegressionFramework):
    def __init__(self, plans, sqls, db, algo="repair", mode="static", config_dict=None,
                 forest=100, plans_for_queries=None):
        # {(p1.id,p2.id):latency}
        super().__init__(plans, sqls, db, algo, mode, forest=forest, plans_for_queries=plans_for_queries)
        self.plans_2_predicts = {}
        # self.shifted_manager = LeroShiftedManager(db, model, algo)
        self.config_dict = config_dict
        self.subspace_confidence = {}

    def _build_iod_model(self):
        for iod_model in self.iod_models:
            iod_model.build(self.plans_for_queries)

    def evaluate(self, plan1, plan2=None, predict=None, ood_thres=None):

        # is_filter = self.shifted_manager.is_filter(plan1)
        # if is_filter:
        #     return -1

        ratios = []

        # for i in range(len(self.iod_models)):
        #     g1: Group = self.iod_models[i].get_group(plan1)

        #     if g1 is None:
        #         # print(self.iod_models[i].static_group.get_group_key(plan1))
        #         continue
        #     elif g1.size() < self.leaf_ele_min_count:
        #         continue
        #     else:
        #         min_r, mean_r, max_r = g1.confidence_range()
        #         # beta = 0.5
        #         # if min_r > -beta and max_r < beta:
        #         ratios.append(mean_r)

        for i in range(len(self.iod_models)):
            statis_key = self.iod_models[i].static_group.get_group_key(plan1)
            if statis_key not in self.iod_models[i].key_to_static_root:
                continue
            root: TreeNode = self.iod_models[i].key_to_static_root[statis_key]
            if root.size() < self.leaf_ele_min_count:
                continue
            ratio = self.get_group_val_with_root(root, plan1)

            if ratio == -1:
                continue

            ratios.append(ratio)

        if len(ratios) == 0:
            return -1
        aver = float(sum(ratios)) / len(ratios)
        return aver
    
    def get_group_val_with_root(self, root, plan: Plan, is_right=True):
        tree_node: TreeNode = root

        if tree_node.empty():
            raise RuntimeError

        if tree_node.is_leaf():
            if tree_node.empty():
                raise RuntimeError
            if tree_node.size() < self.leaf_ele_min_count:
                return -1
            if tree_node.variance() > 3:
                return -1
            # if is_right:
            #     return -1
            min_r, mean_r, max_r = tree_node.confidence_range()
            # return min_r
            return mean_r

        action = tree_node.split_action
        if isinstance(action, NumRangeAction):
            if action.is_plan_left_group(plan) == 1:
                return self.get_group_val_with_root(tree_node.left_child, plan)
            elif action.is_plan_left_group(plan) == 0:
                return self.get_group_val_with_root(tree_node.right_child, plan)
            elif action.is_plan_left_group(plan) == 2:
                plan_id = plan.plan_id
                lower_bound = ColHandler.cols[action.col]["min"]
                upper_bound = ColHandler.cols[action.col]["max"]

                left_plan = deepcopy(plan)
                right_plan = deepcopy(plan)

                for plan_node in plan.get_all_nodes():
                    node_id = plan_node.node_id
                    node_type = plan_node.node_type
                    if node_type in db_node_config.FILTER_TYPES:
                        plan_manager: PlansManager = PlansManager([plan])
                        cols_to_ops_to_values = plan_manager.get_all_filter_infos(plan_id, node_id)
                        if action.col not in cols_to_ops_to_values:
                            continue
                        for op in cols_to_ops_to_values[action.col]:
                            for value in cols_to_ops_to_values[action.col][op]:
                                if op in [">", ">="]:
                                    if value > lower_bound:
                                        lower_bound = value
                                elif op in ["<", "<="]:
                                    if value < upper_bound:
                                        upper_bound = value
                        
                        left_plan.node_id_to_node[node_id].predicates.append((action.col, "<=", action.split_value))
                        right_plan.node_id_to_node[node_id].predicates.append((action.col, ">", action.split_value))

                left_val = self.get_group_val_with_root(tree_node.left_child, left_plan)
                right_val = self.get_group_val_with_root(tree_node.right_child, right_plan)
                
                return left_val * (action.split_value - lower_bound) / (upper_bound - lower_bound) + right_val * (upper_bound - action.split_value) / (upper_bound - lower_bound)
        else:
            if action.is_plan_left_group(plan):
                return self.get_group_val_with_root(tree_node.left_child, plan, is_right=False)
            else:
                return self.get_group_val_with_root(tree_node.right_child, plan, is_right=True if is_right else False)
        
    @staticmethod
    def format_action_detail(action):
        """Format an Action into a human-readable split-condition string."""
        if isinstance(action, NumRangeAction):
            return f"[Numeric range] col {action.col} <= {action.split_value}"
        elif isinstance(action, FilterValueAction):
            return f"[Filter value] predicate {action.col} {action.op} {action.target_value}"
        elif isinstance(action, FilterOpAction):
            return f"[Filter op] predicate {action.col} {action.op}"
        elif isinstance(action, FilterColAction):
            return f"[Filter column] filter on column {action.col}"
        elif isinstance(action, JoinKeyAction):
            return f"[Join key] Join Key = {action.target_value}"
        elif isinstance(action, JoinTypeAction):
            return f"[Join type] Join Type = {action.target_value}"
        elif isinstance(action, TableTypeAction):
            return f"[Scan type] Scan Type = {action.target_value}"
        elif isinstance(action, TableNameAction):
            return f"[Table] Table = {action.target_value}"
        elif isinstance(action, ProjectAction):
            return f"[Project] column {action.col}"
        elif isinstance(action, RangeAction):
            return f"[Discrete range] split_value = {action.split_value}, candidates = {action.sorted_values}"
        else:
            return f"[Unknown] {type(action).__name__}: {action.name()}"

    def print_tree(self, node: TreeNode, depth=0):
        indent = "  " * depth
        if node is None:
            return

        min_r, mean_r, max_r = node.confidence_range()

        if node.is_leaf():
            print(f"{indent}[Leaf] size: {node.size()}, mean: {mean_r:.4f}, "
                  f"range: [{min_r:.4f}, {max_r:.4f}], variance: {node.variance():.4f}")
            for i, action in enumerate(node.actions):
                left_tree_node, right_tree_node = action.fake_split(node)
                detail = self.format_action_detail(action)
                print(f"{indent}  candidate split {i}: {detail}, "
                      f"left_size: {left_tree_node.size()}, right_size: {right_tree_node.size()}, "
                      f"score: {action.score:.4f}")
            return

        action = node.split_action
        condition = self.format_action_detail(action)

        print(f"{indent}[Split] condition: {condition}, "
              f"size: {node.size()}, mean: {mean_r:.4f}, variance: {node.variance():.4f}")

        print(f"{indent}  ├─ true (L):")
        self.print_tree(node.left_child, depth + 2)
        print(f"{indent}  └─ false (R):")
        self.print_tree(node.right_child, depth + 2)

    def print_query_path(self, plan: Plan, model_idx=0):
        """Print the decision-tree matching path for a plan (query)."""
        iod_model = self.iod_models[model_idx]
        statis_key = iod_model.static_group.get_group_key(plan)

        print(f"{'='*60}")
        print(f"Query path trace")
        print(f"Static group key: {statis_key}")
        print(f"{'='*60}")

        if statis_key not in iod_model.key_to_static_root:
            print(f"  [No match] static group key not present in tree")
            return None

        root: TreeNode = iod_model.key_to_static_root[statis_key]
        if root.size() < self.leaf_ele_min_count:
            print(f"  [Insufficient samples] root size {root.size()} < minimum {self.leaf_ele_min_count}")
            return None

        return self._trace_path(root, plan, depth=0)

    def _trace_path(self, tree_node: TreeNode, plan: Plan, depth=0):
        """Recursively trace plan path in the tree; return final prediction."""
        indent = "  " * depth
        min_r, mean_r, max_r = tree_node.confidence_range()

        if tree_node.is_leaf():
            print(f"{indent}[Leaf reached] size: {tree_node.size()}, "
                  f"mean: {mean_r:.4f}, range: [{min_r:.4f}, {max_r:.4f}], "
                  f"variance: {tree_node.variance():.4f}")

            print(f"{indent}  plans in leaf:")
            for idx, p in enumerate(tree_node.plans):
                metric_str = f"{p.metric:.4f}" if p.metric is not None else "N/A"
                exec_str = f"{p.execution_time:.2f}" if p.execution_time else "N/A"
                print(f"{indent}    [{idx}] plan_id={p.plan_id}, "
                      f"metric={metric_str}, exec_time={exec_str}")
                self._print_plan_tree(p.root, prefix=f"{indent}         ")

            if tree_node.size() < self.leaf_ele_min_count:
                print(f"{indent}  → result: insufficient samples (size={tree_node.size()} < {self.leaf_ele_min_count}), return -1")
                return -1
            if tree_node.variance() > 3:
                print(f"{indent}  → result: variance too high (var={tree_node.variance():.4f} > 3), return -1")
                return -1
            print(f"{indent}  → result: predicted mean = {mean_r:.4f}")
            return mean_r

        action = tree_node.split_action
        condition = self.format_action_detail(action)
        print(f"{indent}[Split] condition: {condition}, "
              f"size: {tree_node.size()}, mean: {mean_r:.4f}")

        if isinstance(action, NumRangeAction):
            direction = action.is_plan_left_group(plan)
            if direction == 1:
                print(f"{indent}  → query range within {action.col} <= {action.split_value}, go left")
                return self._trace_path(tree_node.left_child, plan, depth + 1)
            elif direction == 0:
                print(f"{indent}  → query range within {action.col} > {action.split_value}, go right")
                return self._trace_path(tree_node.right_child, plan, depth + 1)
            elif direction == 2:
                lower_bound = ColHandler.cols[action.col]["min"]
                upper_bound = ColHandler.cols[action.col]["max"]
                plan_id = plan.plan_id

                left_plan = deepcopy(plan)
                right_plan = deepcopy(plan)

                for plan_node in plan.get_all_nodes():
                    node_id = plan_node.node_id
                    node_type = plan_node.node_type
                    if node_type in db_node_config.FILTER_TYPES:
                        plan_manager: PlansManager = PlansManager([plan])
                        cols_to_ops_to_values = plan_manager.get_all_filter_infos(plan_id, node_id)
                        if action.col not in cols_to_ops_to_values:
                            continue
                        for op in cols_to_ops_to_values[action.col]:
                            for value in cols_to_ops_to_values[action.col][op]:
                                if op in [">", ">="]:
                                    if value > lower_bound:
                                        lower_bound = value
                                elif op in ["<", "<="]:
                                    if value < upper_bound:
                                        upper_bound = value

                        left_plan.node_id_to_node[node_id].predicates.append((action.col, "<=", action.split_value))
                        right_plan.node_id_to_node[node_id].predicates.append((action.col, ">", action.split_value))

                left_ratio = (action.split_value - lower_bound) / (upper_bound - lower_bound) if upper_bound != lower_bound else 0.5
                right_ratio = (upper_bound - action.split_value) / (upper_bound - lower_bound) if upper_bound != lower_bound else 0.5
                print(f"{indent}  → query range straddles split (range [{lower_bound}, {upper_bound}]), weighted mix:")
                print(f"{indent}    left subtree weight: {left_ratio:.4f}, right subtree weight: {right_ratio:.4f}")

                print(f"{indent}    ├─ left ({action.col} <= {action.split_value}):")
                left_val = self._trace_path(tree_node.left_child, left_plan, depth + 2)
                print(f"{indent}    └─ right ({action.col} > {action.split_value}):")
                right_val = self._trace_path(tree_node.right_child, right_plan, depth + 2)

                result = left_val * left_ratio + right_val * right_ratio
                print(f"{indent}  → weighted: {left_val:.4f} * {left_ratio:.4f} + {right_val:.4f} * {right_ratio:.4f} = {result:.4f}")
                return result
        else:
            if action.is_plan_left_group(plan):
                print(f"{indent}  → query satisfies condition, go left")
                return self._trace_path(tree_node.left_child, plan, depth + 1)
            else:
                print(f"{indent}  → query does not satisfy condition, go right")
                return self._trace_path(tree_node.right_child, plan, depth + 1)

    @staticmethod
    def _print_plan_tree(plan_node, prefix="", is_last=True, is_root=True):
        """Recursively print the plan operator tree."""
        node_json = plan_node.node_json
        node_type = plan_node.node_type

        detail_parts = [node_type]

        if "Relation Name" in node_json:
            alias = node_json.get("Alias", "")
            rel = node_json["Relation Name"]
            detail_parts.append(f"on {rel}" + (f" ({alias})" if alias and alias != rel else ""))

        if "Hash Cond" in node_json:
            detail_parts.append(f"cond: {node_json['Hash Cond']}")
        elif "Join Filter" in node_json:
            detail_parts.append(f"cond: {node_json['Join Filter']}")
        elif "Merge Cond" in node_json:
            detail_parts.append(f"cond: {node_json['Merge Cond']}")

        if hasattr(plan_node, 'predicates') and plan_node.predicates:
            preds = [f"{c} {o} {v}" for c, o, v in plan_node.predicates]
            detail_parts.append(f"filter: [{', '.join(preds)}]")

        line = " | ".join(detail_parts)

        if is_root:
            connector = ""
        else:
            connector = "└─ " if is_last else "├─ "

        print(f"{prefix}{connector}{line}")

        children = plan_node.children
        child_prefix = prefix + ("   " if is_last or is_root else "│  ")
        for i, child in enumerate(children):
            RepariRegressionFramework._print_plan_tree(
                child, prefix=child_prefix,
                is_last=(i == len(children) - 1), is_root=False
            )

class LeroRegressionFramework(RegressionFramework):
    def __init__(self, plans, sqls, db, model, algo="lero", mode="static", config_dict=None,
                 forest=100, plans_for_queries=None):
        # {(p1.id,p2.id):latency}
        super().__init__(plans, sqls, db, model, algo, mode, forest=forest,
                         plans_for_queries=plans_for_queries)
        self.plans_2_predicts = {}
        self.shifted_manager = LeroShiftedManager(db, model, algo)
        self.config_dict = config_dict
        self.subspace_confidence = {}

    def _build_iod_model(self):
        for iod_model in self.iod_models:
            iod_model.build(self.plans_for_queries, self.model)

    def evaluate(self, plan1, plan2=None, predict=None, ood_thres=None):
        s_delete_enable, j_delete_enable, t_delete_enable, f_delete_enable = self.shifted_manager.get_subspace_result(
            ood_thres)

        # ood
        if self.config_dict is not None and self.config_dict["disable_unseen"]:
            s_delete_enable, j_delete_enable, t_delete_enable, f_delete_enable = False, False, False, False
        else:
            is_filter_1 = self.shifted_manager.is_filter(plan1)
            is_filter_2 = self.shifted_manager.is_filter(plan2)
            if is_filter_1 or is_filter_2:
                return -1
        confidences = []

        # for eliminate experiment
        if self.config_dict is not None and self.config_dict["disable_see"]:
            return 1

        for i in range(len(self.iod_models)):
            g1: Group = self.iod_models[i].get_group(plan1)
            g2: Group = self.iod_models[i].get_group(plan2)

            if g1 is None or g2 is None:
                confidences.append(
                    self._choose_confidence(s_delete_enable, j_delete_enable, t_delete_enable, f_delete_enable, i))
            elif g1.size() < self.leaf_ele_min_count or g2.size() < self.leaf_ele_min_count:
                confidences.append(1.1)
            else:
                confidences.append(self._pair_group_confidence(g1, g2))

        assert len(confidences) > 0
        return float(sum(confidences)) / len(confidences)

    def _choose_confidence(self, s_delete_enable, j_delete_enable, t_delete_enable, f_delete_enable, i):
        if i == 0 and s_delete_enable:
            return -1
        elif i == 1 and t_delete_enable:
            return -1
        elif (i == 2 or i == 3) and j_delete_enable:
            return -1
        elif i == 4 and s_delete_enable and j_delete_enable and t_delete_enable and f_delete_enable:
            return -1
        return 1

    # def iod_model_factory(self, min_ele_count, static_config):
    #     return AdaptivePlanGroupOptimization(min_ele_count=min_ele_count, static_config=static_config)

    def _pair_group_confidence(self, group1: Group, group2: Group):
        is_same_group = group1.id == group2.id
        plans1 = group1.plans
        plans2 = group2.plans

        key = self.get_pair_space_key(group1.id, group2.id)
        if key in self.subspace_confidence:
            return self.subspace_confidence[key]

        total_count = 0
        true_count = 0
        for i, plan1 in enumerate(plans1):
            start = i + 1 if is_same_group else 0
            for j in range(start, len(plans2)):
                plan2: Plan = plans2[j]
                if plan1.execution_time <= plan2.execution_time and self._select(plan1, plan2) == 0:
                    true_count += 1
                elif plan1.execution_time >= plan2.execution_time and self._select(plan1, plan2) == 1:
                    true_count += 1
                total_count += 1
        if total_count != 0:
            confidence = true_count / total_count
        else:
            confidence = 1.0

        self.subspace_confidence[key] = confidence
        return confidence

    def _select(self, plan1: Plan, plan2):
        return 0 if plan1.predict <= plan2.predict else 1

    def get_pair_space_key(self, id1, id2):
        if id1 <= id2:
            return "{}_{}".format(id1, id2)
        else:
            return "{}_{}".format(id2, id1)


class HyperQoRegressionFramework(RegressionFramework):

    def __init__(self, plans, sqls, db, training_set_name, model, algo="hyperqo", mode="static"):
        super().__init__(plans, sqls, db, training_set_name, model, algo, mode)
        self.shifted_manager = HyperqoShiftedManager(db, training_set_name, model, algo)

    def evaluate(self, plan1, plan2=None, predict=None, ood_thres=None):

        is_filter = self.shifted_manager.is_filter(plan1)
        if is_filter:
            return -1

        ratios = []

        for i in range(len(self.iod_models)):
            g1: Group = self.iod_models[i].get_group(plan1)

            if g1 is None:
                continue
            elif g1.size() < self.leaf_ele_min_count:
                continue
            else:
                min_r, mean_r, max_r = g1.confidence_range()
                beta = 0.5
                # if min_r > -beta and max_r < beta:
                ratios.append(mean_r)
        if len(ratios) == 0:
            return predict if self.db == "stats" or self.db == "tpch" else -1
        aver = float(sum(ratios)) / len(ratios)
        return predict / (aver + 1)


class PerfRegressionFramework(LeroRegressionFramework):

    def __init__(self, plans, sqls, db, training_set_name, model, algo="perfguard", mode="static", sample=20,
                 config_dict=None, forest=100):
        super().__init__(plans, sqls, db, training_set_name, model, algo, mode, config_dict=config_dict, forest=forest)
        self.group_id_2_confidence = {}
        self.shifted_manager = PerfguardShiftedManager(db, training_set_name, model, algo)
        self.sample = 3
        self.random = random.Random()

    def _build_iod_model(self):
        for iod_model in self.iod_models:
            iod_model.build(self._plans, self.model)

    def _pair_group_confidence(self, group1: Group, group2: Group):
        is_same_group = group1.id == group2.id
        id1 = group1.id
        id2 = group2.id

        key = self.get_pair_space_key(group1.id, group2.id)
        if key in self.subspace_confidence:
            return self.subspace_confidence[key]

        plans1 = group1.plans
        plans2 = group2.plans
        if len(plans1) > self.sample:
            plans1 = self.random.sample(plans1, self.sample)
        if len(plans2) > self.sample:
            plans2 = self.random.sample(plans2, self.sample)

        key = (id1, id2) if id1 < id2 else (id2, id1)
        if key in self.group_id_2_confidence:
            return self.group_id_2_confidence[key]

        results = self._compare(plans1, plans2)
        total_count = 0
        true_count = 0
        for i, plan1 in enumerate(plans1):
            start = i + 1 if is_same_group else 0
            for j in range(start, len(plans2)):
                plan2: Plan = plans2[j]
                cmp_result = results[i * len(plans2) + j]
                if plan1.execution_time <= plan2.execution_time and cmp_result == 1:
                    true_count += 1
                elif plan1.execution_time >= plan2.execution_time and cmp_result == 0:
                    true_count += 1
                total_count += 1
        if total_count != 0:
            confidence = true_count / total_count
        else:
            confidence = 1.0
        self.group_id_2_confidence[key] = confidence
        self.subspace_confidence[key] = confidence

        return confidence

    def _compare(self, plans1, plans2):
        plans1 = [p.plan_json for p in plans1]
        plans2 = [p.plan_json for p in plans2]

        left = []
        right = []
        for p1 in plans1:
            for p2 in plans2:
                left.append(p1)
                right.append(p2)
        return get_perfguard_result(left, right, self.model)
