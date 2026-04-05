# Self defined table for single table join

employees = {
    "table_name": "employees",
    "columns_names": ["NAME", "ID", "MANAGER_ID", "DEPARTMENT_ID", "ROLE"],
    "columns_types": ["VARCHAR", "BIGINT", "BIGINT", "BIGINT", "VARCHAR"],
    "type_size": [50, 8, 8, 8, 50],
    "columns_encrypt": [0, 1, 1, 0, 0],
    "lines": 10000,
    "index_cols": ["ID"]
}

EMPLOYEES_SCHEMA = [
    {"name": "NAME",          "type": "str", "bytes": 50},
    {"name": "ID",            "type": "int", "bytes": 8},
    {"name": "MANAGER_ID",    "type": "int", "bytes": 8},
    {"name": "DEPARTMENT_ID", "type": "int", "bytes": 8},
    {"name": "ROLE",          "type": "str", "bytes": 50}
]


NATION_SCHEMA = [
    {"name": "N_NATIONKEY",  "type": "int",  "bytes": 8},
    {"name": "N_NAME",       "type": "str",  "bytes": 25},  # CHAR(25)
    {"name": "N_REGIONKEY",  "type": "int",  "bytes": 8},
    {"name": "N_COMMENT",    "type": "str",  "bytes": 152}  # VARCHAR(152)
]

SUPPLIER_SCHEMA = [
    {"name": "S_SUPPKEY",    "type": "int",    "bytes": 8},
    {"name": "S_NAME",       "type": "str",    "bytes": 25},   # CHAR(25)
    {"name": "S_ADDRESS",    "type": "str",    "bytes": 40},   # VARCHAR(40)
    {"name": "S_NATIONKEY",  "type": "int",    "bytes": 8},
    {"name": "S_PHONE",      "type": "str",    "bytes": 15},   # CHAR(15)
    {"name": "S_ACCTBAL",    "type": "double", "bytes": 8},
    {"name": "S_COMMENT",    "type": "str",    "bytes": 101}   # VARCHAR(101)
]

CUSTOMER_SCHEMA = [
    {"name": "C_CUSTKEY",    "type": "int",    "bytes": 8},
    {"name": "C_NAME",       "type": "str",    "bytes": 25},   # VARCHAR(25)
    {"name": "C_ADDRESS",    "type": "str",    "bytes": 40},   # VARCHAR(40)
    {"name": "C_NATIONKEY",  "type": "int",    "bytes": 8},
    {"name": "C_PHONE",      "type": "str",    "bytes": 15},   # CHAR(15)
    {"name": "C_ACCTBAL",    "type": "double", "bytes": 8},
    {"name": "C_MKTSEGMENT", "type": "str",    "bytes": 10},   # CHAR(10)
    {"name": "C_COMMENT",    "type": "str",    "bytes": 117}   # VARCHAR(117)
]

ORDERS_SCHEMA = [
    {"name": "O_ORDERKEY",      "type": "int",    "bytes": 8},
    {"name": "O_CUSTKEY",       "type": "int",    "bytes": 8},
    {"name": "O_ORDERSTATUS",   "type": "str",    "bytes": 1},    # CHAR(1)
    {"name": "O_TOTALPRICE",    "type": "double", "bytes": 8},
    {"name": "O_ORDERDATE",     "type": "str",    "bytes": 10},   # DATE
    {"name": "O_ORDERPRIORITY", "type": "str",    "bytes": 15},   # CHAR(15)
    {"name": "O_CLERK",         "type": "str",    "bytes": 15},   # CHAR(15)
    {"name": "O_SHIPPRIORITY",  "type": "int",    "bytes": 8},
    {"name": "O_COMMENT",       "type": "str",    "bytes": 79}    # VARCHAR(79)
]

LINEITEM_SCHEMA = [
    {"name": "L_ORDERKEY",      "type": "int",    "bytes": 8},
    {"name": "L_PARTKEY",       "type": "int",    "bytes": 8},
    {"name": "L_SUPPKEY",       "type": "int",    "bytes": 8},
    {"name": "L_LINENUMBER",    "type": "int",    "bytes": 8},
    {"name": "L_QUANTITY",      "type": "double", "bytes": 8},
    {"name": "L_EXTENDEDPRICE", "type": "double", "bytes": 8},
    {"name": "L_DISCOUNT",      "type": "double", "bytes": 8},
    {"name": "L_TAX",           "type": "double", "bytes": 8},
    {"name": "L_RETURNFLAG",    "type": "str",    "bytes": 1},    # CHAR(1)
    {"name": "L_LINESTATUS",    "type": "str",    "bytes": 1},    # CHAR(1)
    {"name": "L_SHIPDATE",      "type": "str",    "bytes": 10},   # DATE
    {"name": "L_COMMITDATE",    "type": "str",    "bytes": 10},   # DATE
    {"name": "L_RECEIPTDATE",   "type": "str",    "bytes": 10},   # DATE
    {"name": "L_SHIPINSTRUCT",  "type": "str",    "bytes": 25},   # CHAR(25)
    {"name": "L_SHIPMODE",      "type": "str",    "bytes": 10},   # CHAR(10)
    {"name": "L_COMMENT",       "type": "str",    "bytes": 44}    # VARCHAR(44)
]

ENC_COLS = ["N_NATIONKEY", "S_SUPPKEY", "C_CUSTKEY", "O_ORDERKEY"]

# Nation Supplier Customer Order LineItem 
TPCH_SCHEMA = {
    "nation": NATION_SCHEMA,
    "supplier": SUPPLIER_SCHEMA,
    "customer": CUSTOMER_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "lineitem": LINEITEM_SCHEMA
}
