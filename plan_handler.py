import json

def extract_join_order(plan_root):
    """
    Input: the root node dictionary of the Plan (i.e., plan['Plan'])
    Output: List[Tuple], for example [('t1', 't2'), ('t1 t2', 't3')]
    """
    join_steps = []

    # --- Helper function: extract the alias ---
    def _get_alias(node):
        return node.get('Alias') or node.get('Relation Name') or "Unknown"

    # --- Helper function: format the table set ---
    def _fmt(tables_set):
        return " ".join(sorted(tables_set))

    # --- Core recursive function ---
    def _visit(node):
        node_type = node.get('Node Type', '')

        # 1. [Termination condition] If this is a scan node
        # (including Bitmap Heap Scan)
        # As long as "Scan" appears in the node type, we stop exploring
        # the join relationship further and directly treat it as a leaf node
        if 'Scan' in node_type:
            return {_get_alias(node)}

        # 2. [Recursive traversal] Process child nodes
        if 'Plans' in node:
            plans = node['Plans']
            
            # Case A: Single child node
            # (Aggregate, Hash, Sort, Materialize, etc.)
            # These nodes are "transparent" and simply pass through
            # the table set from the lower level
            if len(plans) == 1:
                return _visit(plans[0])

            # Case B: Two child nodes
            # (Nested Loop, Hash Join, Merge Join)
            # These nodes constitute a join operation
            elif len(plans) == 2:
                # Recursively obtain the table sets of the left and right subtrees
                left_tables = _visit(plans[0])
                right_tables = _visit(plans[1])

                # Record this join step (left vs right)
                # Use the closure variable join_steps to collect the results
                join_steps.append((
                    _fmt(left_tables),
                    _fmt(right_tables)
                ))

                # Return the merged set to the upper level
                return left_tables | right_tables

        # 3. [Fallback] Neither a Scan node nor containing Plans
        # (this usually should not happen)
        return set()

    # --- Start execution ---
    _visit(plan_root)
    return join_steps

if __name__ == "__main__":
    pass