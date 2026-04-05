#include "table_meta.hpp"

unordered_map<string, TableMeta> table_meta = {
    {"employees", {
        "employees",
        {"NAME", "ID", "MANAGER_ID", "DEPARTMENT_ID", "ROLE"},
        {0, 3, 3, 1, 0},
        {50, 24, 24, 8, 50},
        {0, 1, 1, 0, 0},
        {"ID"}
    }},
    {"nation", {
        "nation",
        {"N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"},
        {3, 0, 1, 0},            // 1=int, 0=str
        {24, 25, 8, 152},
        {1, 0, 0, 0},            
        {"N_NATIONKEY"}           
    }},
    {"supplier", {
        "supplier",
        {"S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT"},
        {3, 0, 0, 1, 0, 2, 0},  // 2=double
        {24, 25, 40, 8, 15, 8, 101},
        {1, 0, 0, 0, 0, 0, 0},  
        {"S_SUPPKEY"}            
    }},
    {"customer", {
        "customer",
        {"C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT"},
        {3, 0, 0, 1, 0, 2, 0, 0},  // C_MKTSEGMENT=string
        {24, 25, 40, 8, 15, 8, 10, 117},
        {1, 0, 0, 0, 0, 0, 0, 0},  
        {"C_CUSTKEY"}               
    }},
    {"orders", {
        "orders",
        {"O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", 
         "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"},
        {3, 1, 0, 2, 0, 0, 0, 1, 0},  // O_SHIPPRIORITY=int
        {24, 8, 1, 8, 10, 15, 15, 8, 79},
        {1, 0, 0, 0, 0, 0, 0, 0, 0},  
        {"O_ORDERKEY"}                
    }},
    {"lineitem", {
        "lineitem",
        {"L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"},
        {1, 1, 1, 1, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0},
        {8, 8, 8, 8, 8, 8, 8, 8, 1, 1, 10, 10, 10, 25, 10, 44},
        {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        {"L_ORDERKEY"},
    }},
};
