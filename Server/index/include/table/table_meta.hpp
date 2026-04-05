#ifndef TABLE_META_HPP
#define TABLE_META_HPP
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
using namespace std;

typedef struct {
    string table_name;
    vector<string> columns_names;
    vector<int> columns_types;
    vector<int> type_size;
    vector<int> columns_encrypt;
    vector<string> index_cols;
} TableMeta;

#pragma pack(push, 1)
typedef struct {
    char NAME[50];
    // uint64_t ID;
    unsigned char ID[24];
    // uint64_t MANAGER_ID;
    unsigned char MANAGER_ID[24];
    uint64_t DEPARTMENT_ID;
    char ROLE[50];
} Employees;

typedef struct {
    // uint64_t N_NATIONKEY;
    unsigned char N_NATIONKEY[24];
    char N_NAME[25];
    uint64_t N_REGIONKEY;
    char N_COMMENT[152];
} Nation;

typedef struct {
    // uint64_t S_SUPPKEY;
    unsigned char S_SUPPKEY[24];
    char S_NAME[25];
    char S_ADDRESS[40];
    uint64_t S_NATIONKEY;
    char S_PHONE[15];
    double S_ACCTBAL;
    char S_COMMENT[101];
} Supplier;

typedef struct {
    // uint64_t C_CUSTKEY;
    unsigned char C_CUSTKEY[24];
    char C_NAME[25];
    char C_ADDRESS[40];
    uint64_t C_NATIONKEY;
    char C_PHONE[15];
    double C_ACCTBAL;
    char C_MKTSEGMENT[10];
    char C_COMMENT[117];
} Customer;

typedef struct {
    // uint64_t O_ORDERKEY;
    unsigned char O_ORDERKEY[24];
    uint64_t O_CUSTKEY;
    char O_ORDERSTATUS[1];
    double O_TOTALPRICE;
    char O_ORDERDATE[10];
    char O_ORDERPRIORITY[15];
    char O_CLERK[15];
    uint64_t O_SHIPPRIORITY;
    char O_COMMENT[79];
} Orders;

typedef struct {
    uint64_t L_ORDERKEY;
    uint64_t L_PARTKEY;
    uint64_t L_SUPPKEY;
    uint64_t L_LINENUMBER;
    double L_QUANTITY;
    double L_EXTENDEDPRICE;
    double L_DISCOUNT;
    double L_TAX;
    char L_RETURNFLAG[1];
    char L_LINESTATUS[1];
    char L_SHIPDATE[10];
    char L_COMMITDATE[10];
    char L_RECEIPTDATE[10];
    char L_SHIPINSTRUCT[25];
    char L_SHIPMODE[10];
    char L_COMMENT[44];
} Lineitem;
#pragma pack(pop)

extern unordered_map<string, TableMeta> table_meta;

#endif