from sql_executor import SQLExecutor
class ColHandler:
    cols = {}
    executor: SQLExecutor = None

    @staticmethod
    def get_col_type(col_name, table_name, alias_name=None):
        key = f"{alias_name}.{col_name}" if alias_name is not None else f"{table_name}.{col_name}"
        if key in ColHandler.cols:
            return ColHandler.cols[key]
        col_type = ColHandler.executor.get_col_type(col_name, table_name)
        ColHandler.cols[key] = {}
        ColHandler.cols[key]['type'] = col_type
        
        if col_type in ('smallint','integer','bigint','numeric','real','double precision'):
            res = ColHandler.executor.execute_query(f"SELECT MAX({col_name}), MIN({col_name}) FROM {table_name}")
            ColHandler.cols[key]['max'] = res[0][0]
            ColHandler.cols[key]['min'] = res[0][1]

        return ColHandler.cols[key]
    
    @staticmethod
    def set_executor(executor: SQLExecutor):
        ColHandler.executor = executor

    @staticmethod
    def clear():
        ColHandler.cols = {}
        ColHandler.executor = None