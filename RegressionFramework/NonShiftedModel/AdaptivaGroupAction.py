from bisect import bisect_left

from RegressionFramework.Common.dotDrawer import PlanDotDrawer
from RegressionFramework.NonShiftedModel.AdaptivaGroupTree import TreeNode
from RegressionFramework.NonShiftedModel.PlansManeger import PlansManager
from RegressionFramework.Plan.Plan import FilterPlanNode, JoinPlanNode, PlanNode, Plan
from RegressionFramework.Plan.PlanConfig import PgNodeConfig
from RegressionFramework.config import db_node_config, GroupEnable
from RegressionFramework.Plan.PgPlan import PgOtherPlanNode, PgScanPlanNode

from col_handler import ColHandler
from copy import deepcopy

class Action:
    def __init__(self, plan_node_id, plans_manager: PlansManager, score):
        self.plan_node_id = plan_node_id
        self.plans_manager: PlansManager = plans_manager
        self.score = score

    def name(self):
        return ""

    def __hash__(self):
        return hash((type(self), self.plan_node_id))

    def __eq__(self, other):
        return type(self) == type(other) and self.plan_node_id == other.plan_node_id

    def split(self, tree_node: TreeNode):
        left_tree_node, right_tree_node = self._split_no_add_action(tree_node)
        self.add_actions_left(left_tree_node, tree_node)
        self.add_actions_right(right_tree_node, tree_node)
        return left_tree_node, right_tree_node

    def is_plan_left_group(self, plan):
        for node_id in plan.node_id_to_node:
            plan_node = plan.node_id_to_node[node_id]
            if isinstance(self, FilterColAction) or isinstance(self, FilterOpAction) or isinstance(self, FilterValueAction):
                if isinstance(plan_node, PgScanPlanNode):
                    if self.is_left_group(plan_node):
                        return True
            elif isinstance(self, JoinKeyAction):    
                if isinstance(plan_node, JoinPlanNode):
                    if self.is_left_group(plan_node):
                        return True
            elif isinstance(self, NumRangeAction):
                if isinstance(plan_node, PgScanPlanNode):
                    # 0 1 2
                    return self.is_left_group(plan_node)
        return False

    def _split_no_add_action(self, tree_node: TreeNode):
        left_group_plans = []
        right_group_plans = []
        plans = tree_node.plans

        if isinstance(self, NumRangeAction):
            left_group_plans = self.left_plans
            right_group_plans = self.right_plans
        else:
            for plan in plans:
                if self.is_plan_left_group(plan):
                    left_group_plans.append(plan)
                else:
                    right_group_plans.append(plan)
            # plan_node = self.plans_manager.get_node(plan.plan_id, self.plan_node_id)
        #     if isinstance(plan_node, PgOtherPlanNode):
        #         continue

        #     if self.is_left_group(plan_node):
        #         left_group_plans.append(plan)
        #     else:
        #         right_group_plans.append(plan)
        # left_group_plans = [plan for plan in tree_node.plans if plan.plan_id in self.plan_node_id]
        # right_group_plans = [plan for plan in tree_node.plans if plan.plan_id not in self.plan_node_id]

        return TreeNode(left_group_plans), TreeNode(right_group_plans)

    def fake_split(self, tree_node: TreeNode):
        return  self._split_no_add_action(tree_node)

    @classmethod
    def update_actions(cls, plan_manager: PlansManager, root: TreeNode):
        plans = root.plans

        col_to_plan_id_to_node_id = {}
        col_to_op_to_plan_id_to_node_id = {}
        col_to_op_value_to_plan_id_to_node_id = {}
        join_key_to_plan_id_to_node_id = {}

        for plan in plans:
            plan_id = plan.plan_id
            # print(f"plan_id: {plan_id}")
            for plan_node in plan.get_all_nodes():
                node_id = plan_node.node_id
                node_type = plan_node.node_type

                if node_type in db_node_config.FILTER_TYPES:
                    cols_to_ops_to_values = plan_manager.get_all_filter_infos(plan_id, node_id)
                    cols = [] if len(cols_to_ops_to_values) == 0 else cols_to_ops_to_values.keys()
                    for col in cols:
                        cls.init_dict_and_inc(col_to_plan_id_to_node_id, plan_id, node_id, col)
                        for op in cols_to_ops_to_values[col].keys():
                            cls.init_dict_and_inc(col_to_op_to_plan_id_to_node_id, plan_id, node_id, col, op)
                            for value in cols_to_ops_to_values[col][op]:
                                cls.init_dict_and_inc(col_to_op_value_to_plan_id_to_node_id, plan_id, node_id, col, op, value)
                if node_type in db_node_config.JOIN_TYPES:
                    if not GroupEnable.join_key_enable:
                        join_keys = plan_manager.get_all_join_keys(plan_id, node_id)
                        for split_join_key in join_keys:
                            cls.init_dict_and_inc(join_key_to_plan_id_to_node_id, plan_id, node_id, split_join_key)

        # print(plan_manager.plan_id_to_node_id_to_node)
        # print(len(col_to_plan_id_to_node_id), len(col_to_op_to_plan_id_to_node_id), len(col_to_op_value_to_plan_id_to_node_id), len(join_key_to_plan_id_to_node_id))
        # print("col_to_plan_id_to_node_id:", col_to_plan_id_to_node_id)
        # print("col_to_op_to_plan_id_to_node_id:", col_to_op_to_plan_id_to_node_id)
        # print("col_to_op_value_to_plan_id_to_node_id:", col_to_op_value_to_plan_id_to_node_id)
        # print("join_key_to_plan_id_to_node_id:", join_key_to_plan_id_to_node_id)

        plan_size = len(plans)
        for col in col_to_plan_id_to_node_id.keys():
            count = len(col_to_plan_id_to_node_id[col])
            if count < plan_size:
                # for node_id in col_to_plan_id_to_node_id[col].values():
                # for plan_id in col_to_plan_id_to_node_id[col].keys():
                root.add_action(FilterColAction(tuple(col_to_plan_id_to_node_id[col].keys()), col, plan_manager, count / plan_size))
            else:
                if col in ColHandler.cols and "min" in ColHandler.cols[col] and "max" in ColHandler.cols[col]:
                    min_value = ColHandler.cols[col]["min"]
                    max_value = ColHandler.cols[col]["max"]
                    step = (max_value - min_value) / 10.0

                    plan_id_to_max_min_value = {}
                    for plan_id in col_to_plan_id_to_node_id[col].keys():
                        lower = min_value
                        upper = max_value

                        for node_id in col_to_plan_id_to_node_id[col][plan_id]:
                            cols_to_ops_to_values = plan_manager.get_all_filter_infos(plan_id, node_id)
                            if col in cols_to_ops_to_values:
                                for op in cols_to_ops_to_values[col].keys():
                                    if op in [">", ">="]:
                                        for value in cols_to_ops_to_values[col][op]:
                                            if value > lower:
                                                lower = value
                                    elif op in ["<", "<="]:
                                        for value in cols_to_ops_to_values[col][op]:
                                            if value < upper:
                                                upper = value

                        if plan_id not in plan_id_to_max_min_value:
                            plan_id_to_max_min_value[plan_id] = (lower, upper)

                    for i in range(1, 10):
                        split_value = min_value + i * step
                        left_count_split = 0 # smaller than split_value
                        right_count_split = 0 # larger than split_value

                        for plan_id in col_to_plan_id_to_node_id[col].keys():
                            if plan_id in plan_id_to_max_min_value:
                                lower, upper = plan_id_to_max_min_value[plan_id]
                                if lower < split_value and upper > split_value:
                                    left_count_split += 1
                                    right_count_split += 1
                                elif lower >= split_value:
                                    right_count_split += 1
                                elif upper <= split_value:
                                    left_count_split += 1

                        if left_count_split == 0 or right_count_split == 0:
                            continue

                        left_plans = []
                        right_plans = []
                        for plan in plans:
                            if plan.plan_id in plan_id_to_max_min_value:
                                lower, upper = plan_id_to_max_min_value[plan.plan_id]
                                if lower <= split_value and upper >= split_value:
                                    left_plan = deepcopy(plan)
                                    right_plan = deepcopy(plan)

                                    try:
                                        left_plan.metric = plan.metric * ((split_value - lower) / (upper - lower))
                                        right_plan.metric = plan.metric * ((upper - split_value) / (upper - lower))
                                    except:
                                        print(lower, split_value, upper)
                                        print(ColHandler.cols)
                                        raise

                                    for node_id in col_to_plan_id_to_node_id[col][plan.plan_id]:
                                        left_plan.node_id_to_node[node_id].predicates.append((col, "<=", split_value))
                                        right_plan.node_id_to_node[node_id].predicates.append((col, ">", split_value))
                                    left_plan.plan_id = Plan.total_plan_id
                                    Plan.total_plan_id += 1
                                    right_plan.plan_id = Plan.total_plan_id
                                    Plan.total_plan_id += 1
                                    left_plans.append(left_plan)
                                    right_plans.append(right_plan)
                                elif lower > split_value:
                                    right_plans.append(deepcopy(plan))
                                elif upper < split_value:
                                    left_plans.append(deepcopy(plan))
                        # print("split_value type:", type(split_value), split_value)
                        # print("plan_node_id arg type:", type(tuple(col_to_plan_id_to_node_id[col].keys())))
                        # print("plan_node_id elements types:", {type(x) for x in col_to_plan_id_to_node_id[col].keys()})

                        # print(split_value)
                        # print(tuple(list(col_to_plan_id_to_node_id[col].keys())))

                        root.add_action(NumRangeAction(tuple(list(col_to_plan_id_to_node_id[col].keys())), PlansManager(left_plans + right_plans), col, split_value, left_plans, right_plans, left_count_split / (left_count_split + right_count_split)))
                else:
                    for op in col_to_op_to_plan_id_to_node_id[col].keys():
                        op_count = len(col_to_op_to_plan_id_to_node_id[col][op])
                        if op_count < plan_size:
                            # for node_id in col_to_op_to_plan_id_to_node_id[col][op].values():
                            # for plan_id in col_to_op_to_plan_id_to_node_id[col][op].keys():
                            root.add_action(FilterOpAction(tuple(col_to_op_to_plan_id_to_node_id[col][op].keys()), col, op, plan_manager, op_count / plan_size))
                        else:
                            values = col_to_op_value_to_plan_id_to_node_id[col][op].keys()
                            values = sorted(list(values))
                            for i in range(1, len(values)):
                                value = values[i]
                                value_count = len(col_to_op_value_to_plan_id_to_node_id[col][op][value])
                                if value_count < plan_size:
                                    # for plan_id in col_to_op_value_to_plan_id_to_node_id[col][op][value].keys():
                                    root.add_action(
                                        FilterValueAction(tuple(col_to_op_value_to_plan_id_to_node_id[col][op][value].keys()), col, op, value, plan_manager,value_count / plan_size))
            
        if not GroupEnable.join_key_enable:                                              
            for join_key in join_key_to_plan_id_to_node_id.keys():
                count = len(join_key_to_plan_id_to_node_id[join_key])
                if count < plan_size:
                    # for plan_id in join_key_to_plan_id_to_node_id[join_key].keys():
                    root.add_action(JoinKeyAction(tuple(join_key_to_plan_id_to_node_id[join_key].keys()), join_key, plan_manager, count / plan_size))

    @classmethod
    def init_dict_and_inc(cls, _add_dict: dict, plan_id, node_id, value, value2=None, value3=None):
        if value not in _add_dict:
            _add_dict[value] = {}
        if value2 is None:
            if plan_id not in _add_dict[value]:
                _add_dict[value][plan_id] = []
            _add_dict[value][plan_id].append(node_id)
        elif value3 is None:
            if value2 not in _add_dict[value]:
                _add_dict[value][value2] = {}
            if plan_id not in _add_dict[value][value2]:
                _add_dict[value][value2][plan_id] = []
            _add_dict[value][value2][plan_id].append(node_id)
        else:
            if value2 not in _add_dict[value]:
                _add_dict[value][value2] = {}
            if value3 not in _add_dict[value][value2]:
                _add_dict[value][value2][value3] = {}
            if plan_id not in _add_dict[value][value2][value3]:
                _add_dict[value][value2][value3][plan_id] = []
            _add_dict[value][value2][value3][plan_id].append(node_id)

    @classmethod
    def _aux_add_action(cls, root: TreeNode, plan_manager, node_id_to_value_to_count, node_id, plan_size,
                        enable, action):
        if not enable:
            for value, count in node_id_to_value_to_count[node_id].items():
                if count < plan_size:
                    root.add_action(
                        action(node_id, value, plan_manager, count / plan_size))

    def add_actions_left(self, target: TreeNode, origin: TreeNode, ignore_actions=None, ignore_action_types=None):
        if target.empty():
            return
        self.update_actions(self.plans_manager, target)

    def add_actions_right(self, target: TreeNode, origin: TreeNode, ignore_actions=None, ignore_action_types=None):
        if target.empty():
            return
        self.update_actions(self.plans_manager, target)

    @classmethod
    def is_ignore_action(cls, action, ignore_actions=None):
        if ignore_actions is None:
            return False
        return action in ignore_actions

    @classmethod
    def is_ignore_action_types(cls, action, ignore_action_types=None):
        if ignore_action_types is None:
            return False
        return type(action) in ignore_action_types

    def node_id_equal(self, action):
        return action.plan_node_id == self.plan_node_id

    def is_left_group(self, plan_node):
        raise RuntimeError


class RangeAction(Action):
    def __init__(self, plan_node_id, plans_manager: PlansManager, split_value, values, score):
        super().__init__(plan_node_id, plans_manager, score)
        self.sorted_values = sorted(list(set(values)))
        self.split_value = split_value

    def __hash__(self):
        return hash((type(self), self.plan_node_id, self.split_value))

    def __eq__(self, other):
        return super().__eq__(
            other) and self.split_value == other.split_value and self.sorted_values == other.sorted_values

    def is_left_group(self, plan_node: PlanNode):
        split_pos = bisect_left(self.sorted_values, self.split_value)
        cur_pos = bisect_left(self.sorted_values, self.get_cur_value(plan_node))
        if cur_pos <= split_pos:
            return True
        return False

    def get_cur_value(self, plan_node: PlanNode):
        raise RuntimeError

    def get_next_action(self, value, values):
        raise RuntimeError

class NumRangeAction(Action):
    def __init__(self, plan_node_id, plans_manager: PlansManager, col, split_value, left_plans, right_plans, score):
        super().__init__(plan_node_id, plans_manager, score)
        self.col = col
        # print(type(split_value), split_value)
        self.split_value = split_value
        self.left_plans = left_plans
        self.right_plans = right_plans

    def __hash__(self):
        # print("HASH plan_node_id:", type(self.plan_node_id), self.plan_node_id)
        # print("HASH split_value:", type(self.split_value), self.split_value)
        return hash((type(self), self.plan_node_id, self.split_value))

    def __eq__(self, other):
        return super().__eq__(
            other) and self.split_value == other.split_value

    def is_left_group(self, plan_node: PlanNode):
        lower_bound = ColHandler.cols[self.col]["min"]
        upper_bound = ColHandler.cols[self.col]["max"]

        predicates = plan_node.predicates
        for predicate in predicates:
            col = predicate[0]
            op = predicate[1]
            value = predicate[2]
            if col != self.col:
                continue
            if op in [">", ">="]:
                lower_bound = max(lower_bound, value)
            elif op in ["<", "<="]:
                upper_bound = min(upper_bound, value)

        if lower_bound <= self.split_value and upper_bound >= self.split_value:
            return 2
        elif lower_bound >= self.split_value:
            return 0
        elif upper_bound <= self.split_value:
            return 1

class OnceAction(Action):

    def __init__(self, target_value, plan_node_id, plans_manager: PlansManager, score):
        super().__init__(plan_node_id, plans_manager, score)
        self.target_value = target_value

    def __hash__(self):
        return hash((type(self), self.plan_node_id, self.target_value))

    def name(self):
        return ""

    def __eq__(self, other):
        return super().__eq__(other) and self.target_value == other.target_value

    def is_left_group(self, plan_node):
        cur_value = self.get_cur_value(plan_node)
        if self.target_value == cur_value:
            return True
        return False

    def get_cur_value(self, plan_node):
        raise RuntimeError


class ProjectAction(OnceAction):
    def __init__(self, plan_node_id, col, plans_manager: PlansManager, score):
        super().__init__(col, plan_node_id, plans_manager, score)
        self.col = col

    # def is_left_group(self, plan_node: ProjectPlanNode):
    #     project_cols = set(plan_node.project_cols)
    #     if self.col in project_cols:
    #         return True
    #     return False


class FilterColAction(OnceAction):
    def __init__(self, plan_node_id, col, plans_manager: PlansManager, score):
        super().__init__(col, plan_node_id, plans_manager, score)
        self.col = col

    def name(self):
        return "F_Col_{}".format(self.col)

    def is_left_group(self, plan_node: FilterPlanNode):
        predicates = plan_node.predicates
        for predicate in predicates:
            if self.col == predicate[0]:
                return True
        return False


class FilterOpAction(Action):
    def __init__(self, plan_node_id, col, op, plans_manager: PlansManager, score):
        super().__init__(plan_node_id, plans_manager, score)
        self.op = op
        self.col = col

    def __hash__(self):
        return hash((type(self), self.plan_node_id, self.col, self.op))

    def name(self):
        return "F_Col_{}_Op_{}".format(self.col, self.op.split(".")[-1])

    def __eq__(self, other):
        return super().__eq__(other) and self.col == other.col and self.op == other.op

    def is_left_group(self, plan_node: FilterPlanNode):
        predicates = plan_node.predicates
        for predicate in predicates:
            col = predicate[0]
            op = predicate[1]
            if self.col == col and self.op == op:
                return True
        return False


class FilterValueAction(OnceAction):
    def __init__(self, plan_node_id, col, op, split_value, plans_manager: PlansManager, score):
        super().__init__(split_value, plan_node_id, plans_manager, score)
        self.col = col
        self.op = op

    def name(self):
        return "F_Col_{}_Op_{}_value_{}".format(self.col, self.op.split(".")[-1], self.target_value)

    def __hash__(self):
        return hash((type(self), self.plan_node_id, self.target_value, self.col, self.op))

    def __eq__(self, other):
        return super().__eq__(
            other) and self.col == other.col and self.op == other.op and self.target_value == other.target_value

    def is_left_group(self, plan_node: FilterPlanNode):
        predicates = plan_node.predicates
        for predicate in predicates:
            col = predicate[0]
            op = predicate[1]
            value = predicate[2]
            if self.col == col and self.op == op and value == self.target_value:
                return True
        return False


class JoinKeyAction(OnceAction):
    def __init__(self, plan_node_id, split_key, plans_manager: PlansManager, score):
        super().__init__(split_key, plan_node_id, plans_manager, score)

    def get_cur_value(self, plan_node: JoinPlanNode):
        return plan_node.get_join_key_str()

    def name(self):
        return "J_Key_{}".format(self.target_value)


class JoinTypeAction(OnceAction):
    def __init__(self, plan_node_id, split_join_type, plans_manager: PlansManager, score):
        super().__init__(split_join_type, plan_node_id, plans_manager, score)

    def get_cur_value(self, plan_node: PlanNode):
        return plan_node.get_join_type()

    def name(self):
        return "J_Type_{}".format(self.target_value.split(".")[-1])


class TableTypeAction(OnceAction):
    def __init__(self, plan_node_id, table_type, plans_manager: PlansManager, score):
        super().__init__(table_type, plan_node_id, plans_manager, score)

    def name(self):
        return "T_Type_{}".format(self.target_value.split(".")[-1])

    def get_cur_value(self, plan_node: PlanNode):
        return plan_node.get_scan_type()


class TableNameAction(OnceAction):
    def __init__(self, plan_node_id, table_name, plans_manager: PlansManager, score):
        super().__init__(table_name, plan_node_id, plans_manager, score)

    def get_cur_value(self, plan_node: PlanNode):
        return plan_node.get_table_name()

    def name(self):
        return "T_Name_{}".format(self.target_value)
