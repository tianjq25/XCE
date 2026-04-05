#include <cmath>
#include <cstdint>
#include <iostream>
#include <sys/mman.h>
#include <json-c/json.h>
#include <stdio.h>
#include <sys/file.h>
#include <unistd.h>
#include <fstream>
#include "json.hpp"
#include "join.hpp"
#include "utils/ycsb_utils.h"
using json = nlohmann::json;

#define REQ_PATH "communicate/request"
#define RESP_PATH "communicate/response"
#define SIGNAL_PATH "signal"

using json = nlohmann::json;
/*
int main() {
    std::vector<std::string> table_names = { "employees"};
    // std::vector<std::string> table_names = { "employees", "nation", "supplier", "customer", "orders", "lineitem"};
    // Load tables
    std::unordered_map<std::string, Table*> tables;
    for (const auto& table_name : table_names) {
        tables[table_name] = new Table(table_name);
    }
    std::cout << "Tables loaded" << std::endl;

    // Build indexes
    // std::unordered_map<std::string, TEEIndex<uint64_t, uint64_t>*> TEEIdxMap;
    std::unordered_map<std::string, TieredBTree<uint64_t, uint64_t>*> TieredBTMap;
    for (const auto& table_name : table_names) {
        if (table_name == "lineitem") continue;
        std::string index_file = "tables/" + table_name + "_" + table_meta[table_name].index_cols[0] + ".col";
        std::vector<std::pair<uint64_t, uint64_t>> index_data;
        uint64_t index_size = load_data<std::pair<uint64_t, uint64_t>>(index_file, index_data);
        std::sort(index_data.begin(), index_data.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        int epsilon = 256;
        // TEEIdxMap[table_name] = new TEEIndex<uint64_t, uint64_t>(index_data, epsilon, table_name + "_" + table_meta[table_name].index_cols[0]);
        TieredBTMap[table_name] = new TieredBTree<uint64_t, uint64_t>(index_data, table_name + "_" + table_meta[table_name].index_cols[0]);
    }
    std::cout << "Indexes built" << std::endl;

    // TEEIdxMap["employees"]->size_report();
    // return 0;

    // std::string n1 = "CHINA";
    // std::string n2 = "UNITED STATES";
    // std::vector<std::tuple<std::string, std::string, std::string, double>> revenue_naive;
    // uint64_t naive_time = naive_multi_table_join(tables, n1, n2, revenue_naive);
    // std::cout << "Naive join time: " << naive_time / 1e6 << " ms" << std::endl;
    // for (const auto& row : revenue_naive) {
    //     std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
    //     std::cout << std::get<3>(row) << std::endl;
    // }

    // std::cout << "---------------- LETIndex -------------------" << std::endl;
    // std::vector<std::tuple<std::string, std::string, std::string, double>> revenue_index;
    // std::map<std::string, uint64_t> time_count_index;
    // uint64_t index_time = index_multi_table_join_detail(tables, TEEIdxMap, n1, n2, revenue_index, time_count_index);
    // std::cout << "Index join time: " << index_time / 1e6 << "ms" << std::endl;
    // for (const auto& entry : time_count_index) {
    //     std::cout << entry.first << ": " << entry.second / 1e3 << " microseconds" << std::endl;
    // }
    // for (const auto& row : revenue_index) {
    //     std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
    //     std::cout << std::get<3>(row) << std::endl;
    // }
    // std::cout << "---------------- TieredBTree -------------------" << std::endl;
    // std::vector<std::tuple<std::string, std::string, std::string, double>> revenue_tiered;
    // std::map<std::string, uint64_t> time_count_tiered;
    // uint64_t tiered_time = index_multi_table_join_detail<TieredBTree<uint64_t, uint64_t>>(tables, TieredBTMap, n1, n2, revenue_tiered, time_count_tiered);
    // std::cout << "Tiered join time: " << tiered_time / 1e6 << " ms" << std::endl;
    // for (const auto& entry : time_count_tiered) {
    //     std::cout << entry.first << ": " << entry.second / 1e3 << " microseconds" << std::endl;
    // }
    // for (const auto& row : revenue_tiered) {
    //     std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
    //     std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
    //     std::cout << std::get<3>(row) << std::endl;
    // }

    std::vector<uint64_t> res_rows;
    // uint64_t time_count = join(TEEIdxMap["employees"], tables["employees"], res_rows);
    // std::cout << "Join time for TEEIdxMap: " << time_count << " ns" << std::endl;
    // for (int i = 0; i < 3; i++)
    //     std::cout << res_rows[i] << std::endl;
    // res_rows.clear();
    uint64_t tiered_time_count = join(TieredBTMap["employees"], tables["employees"], res_rows);
    std::cout << "Join time for TieredBTMap: " << tiered_time_count << " ns" << std::endl;
    for (int i = 0; i < 3; i++)
        std::cout << res_rows[i] << std::endl;

    return 0;
}
*/

void single_table_join(std::unordered_map<std::string, Table*> &tables,
    std::unordered_map<std::string, TEEIndex<uint64_t, uint64_t>*> &TEEIdxMap,
    std::unordered_map<std::string, TieredBTree<uint64_t, uint64_t>*> &TieredBTMap) {
    std::ifstream file(REQ_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open file" << std::endl;
        exit(1);
    }

    json data;
    try {
        file >> data;
        if (!data.contains("index")) {
            std::cerr << "Error: missing required field (index)" << std::endl;
            file.close();
            return;
        }

        auto& index = data["index"];
        if (!index.is_number_integer() || index < 0 || index > 2) {
            std::cerr << "Error: index must be 0, 1, or 2, current value is " << index << std::endl;
            file.close();
            return;
        }

        file.close();
        uint64_t index_time = 0, index_scan_time = 0;
        std::vector<uint64_t> res_rows;
        json pgm_detail_rows = json::array();
        json res_data;
        switch (index.get<int>()) {
            case 0: { // Without index
                index_time = join_detail(nullptr, tables["employees"], res_rows, index_scan_time, 0);
                break;
            }
            case 1: { // LETIndex
                index_time = join_detail(TEEIdxMap["employees"], tables["employees"], res_rows, index_scan_time, 1, &pgm_detail_rows);
                res_data["pgm_detail_rows"] = pgm_detail_rows;
                break;
            }
            case 2: { // TieredBTIndex
                index_time = join_detail(TieredBTMap["employees"], tables["employees"], res_rows, index_scan_time, 2);
                break;
            }
            default:
                std::cerr << "Error: invalid index value" << std::endl;
                break;
        }
        std::cout << "search end" << std::endl;

        // dump to response file
        std::ofstream response(RESP_PATH);
        // 1. latency field
        res_data["latency"] = index_time / 1e6;
        // 2. res_table field
        json res_table;
        res_table["col_type"] = {"int", "char50", "char50"};
        json rows = json::array();
        int row_count = std::min(10, (int)res_rows.size());
        auto row_it = tables["employees"]->row_begin();
        for (int i = 0; i < row_count; i++) {
            auto scan_row = std::get<Employees>(*(row_it));
            auto emp_row = std::get<Employees>(*(tables["employees"]->row_begin() + res_rows[i]));
            rows.push_back(json::array({
                binary_to_hex(scan_row.ID, 24),
                safe_string(scan_row.NAME, 50),
                safe_string(emp_row.NAME, 50)
            }));
            ++row_it;
        }
        res_table["rows"] = rows;
        res_data["res_table"] = res_table;

        // 3. tree field
        // json tree;
        // json nlj_emp_mgr, nlj_emp_mgr_children;
        // json full_scan;
        // full_scan["children"] = {};
        // nlj_emp_mgr_children[" Full Table Scan (employees mgr)"] = full_scan;
        // json index_scan;
        // index_scan["children"] = {};
        // index_scan["time"] = index_scan_time;
        // nlj_emp_mgr_children["Index Scan (emp.manager_id)"] = index_scan;
        // nlj_emp_mgr["children"] = nlj_emp_mgr_children;
        // tree["Nested Loop Join (emp ⨝ mgr)"] = nlj_emp_mgr;
        // res_data["tree"] = tree;

        std::cout << "start to write response" << std::endl;
        // dump to response file
        std::ofstream res_file(RESP_PATH);
        if (!res_file) {
            std::cerr << "Failed to open response file" << std::endl;
            return;
        }
        res_file << res_data.dump(4) << std::endl;
        res_file.close();

    } catch (const json::parse_error& e) {
        file.close();
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        exit(1);
    }
}

void Q3_join(std::unordered_map<std::string, Table*> &tables,
    std::unordered_map<std::string, TEEIndex<uint64_t, uint64_t>*> &TEEIdxMap,
    std::unordered_map<std::string, TieredBTree<uint64_t, uint64_t>*> &TieredBTMap) {
    std::ifstream file(REQ_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open file" << std::endl;
        exit(1);
    }
    json data;
    try {
        file >> data;
        if (!data.contains("index")) {
            std::cerr << "Error: missing required fields (index or nation)" << std::endl;
            file.close();
            return;
        }
        auto& index = data["index"];
        if (!index.is_number_integer() || index < 1 || index > 2) {
            std::cerr << "Error: index must be 1 or 2, current value is " << index << std::endl;
            file.close();
            return;
        }
        file.close();
        uint64_t index_time = 0, index_scan_time = 0;
        std::vector<std::tuple<uint64_t, double, std::string, uint64_t>> revenue;
        std::map<std::string, uint64_t> time_count_index;
        switch (index.get<int>()) {
            case 1: {
                index_time = index_Q3_detail(tables, TEEIdxMap, "BUILDING", revenue, time_count_index);
                break;
            }
            case 2: {
                index_time = index_Q3_detail(tables, TieredBTMap, "BUILDING", revenue, time_count_index);
                break;
            }
            default:
                std::cerr << "Error: invalid index value" << std::endl;
                break;
        }

        // dump to response file
        std::ofstream response(RESP_PATH);
        json res_data;
        // 1. latency field
        res_data["latency"] = index_time / 1e6;
        // 2. res_table field
        json res_table;
        res_table["col_type"] = {"uint64", "double", "char10", "uint64"};
        json rows = json::array();
        int row_count = std::min(10, (int)revenue.size());
        for (int i = 0; i < row_count; i++) {
            auto row = revenue[i];
            rows.push_back(json::array({
                std::get<0>(row),
                std::get<1>(row),
                std::get<2>(row),
                std::get<3>(row)
            }));
        }
        res_table["rows"] = rows;
        res_data["res_table"] = res_table;
        
        // 3. tree field
        json tree;
        json filter, filter_children, scan_lineitem;
        scan_lineitem["children"] = {};
        filter_children["scan lineitem"] = scan_lineitem;
        filter["children"] = filter_children;

        json nlj_li_ord, nlj_li_ord_children;
        json idx_search_ord;
        idx_search_ord["children"] = {};
        nlj_li_ord_children["filter (L_SHIPDATE)"] = filter;
        nlj_li_ord_children["index search(orders)"] = idx_search_ord;
        nlj_li_ord["children"] = nlj_li_ord_children;
        nlj_li_ord["time"] = time_count_index["orders"];

        json nlj_li_cu, nlj_li_cu_children;
        json idx_search_cu;
        idx_search_cu["children"] = {};
        nlj_li_cu_children["Nested Loop Join"] = nlj_li_ord;
        nlj_li_cu_children["index search(customer)"] = idx_search_cu;
        nlj_li_cu["children"] = nlj_li_cu_children;
        nlj_li_cu["time"] = time_count_index["customer"];

        json agg, agg_children;
        agg_children["Nested Loop Join"] = nlj_li_cu;
        agg["children"] = agg_children;

        json sor, sor_children;
        sor_children["Aggregation"] = agg;
        sor["children"] = sor_children;

        tree["Sort"] = sor;
        res_data["tree"] = tree;

        // dump to response file
        std::ofstream res_file(RESP_PATH);
        if (!res_file) {
            std::cerr << "Failed to open response file" << std::endl;
            return;
        }
        res_file << res_data.dump(4) << std::endl;
        res_file.close();

    }
    catch (const json::parse_error& e) {
        file.close();
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        exit(1);
    }
}

void multi_table_join(std::unordered_map<std::string, Table*> &tables,
    std::unordered_map<std::string, TEEIndex<uint64_t, uint64_t>*> &TEEIdxMap,
    std::unordered_map<std::string, TieredBTree<uint64_t, uint64_t>*> &TieredBTMap) {
    std::ifstream file(REQ_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open file" << std::endl;
        exit(1);
    }

    json data;
    try {
        file >> data;
        if (!data.contains("index") || !data.contains("nation")) {
            std::cerr << "Error: missing required fields (index or nation)" << std::endl;
            file.close();
            return;
        }

        auto& index = data["index"];
        if (!index.is_number_integer() || index < 0 || index > 2) {
            std::cerr << "Error: index must be 0, 1, or 2, current value is " << index << std::endl;
            file.close();
            return;
        }

        auto& nation = data["nation"];
        if (!nation.is_array() || nation.size() != 2) {
            std::cerr << "Error: nation must be an array of size 2, current size is " 
                      << nation.size() << std::endl;
            file.close();
            return;
        }
        for (const auto& country : nation) {
            if (!country.is_string()) {
                std::cerr << "Error: nation array contains non-string elements" << std::endl;
                file.close();
                return;
            }
        }

        file.close();

        std::string nation1 = nation[0].get<std::string>();
        std::string nation2 = nation[1].get<std::string>();
        std::cout << "nation1: " << nation1 << ", nation2: " << nation2 << std::endl;
        
        std::vector<std::tuple<std::string, std::string, std::string, double>> revenue_index;
        std::map<std::string, uint64_t> time_count_index;
        uint64_t index_time;
        switch (index.get<int>()) {
            case 0: {
                std::cout << "Start naive multi table join" << std::endl;
                index_time = naive_multi_table_join_detail(tables, nation1, nation2, revenue_index, time_count_index);
                std::cout << "Naive join time: " << index_time / 1e6 << " ms" << std::endl;
                for (const auto& entry : time_count_index) {
                    std::cout << entry.first << ": " << entry.second << " nanoseconds" << std::endl;
                }
                for (const auto& row : revenue_index) {
                    std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
                    std::cout << std::get<3>(row) << std::endl;
                }
                break;
            }
            case 1: {
                index_time = index_multi_table_join_detail(tables, TEEIdxMap, nation1, nation2, revenue_index, time_count_index);
                std::cout << "Index join time: " << index_time / 1e6 << "ms" << std::endl;
                for (const auto& entry : time_count_index) {
                    std::cout << entry.first << ": " << entry.second << " nanoseconds" << std::endl;
                }
                for (const auto& row : revenue_index) {
                    std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
                    std::cout << std::get<3>(row) << std::endl;
                }
                break;
            }
            case 2: {
                index_time = index_multi_table_join_detail<TieredBTree<uint64_t, uint64_t>>(tables, TieredBTMap, nation1, nation2, revenue_index, time_count_index);
                std::cout << "Tiered join time: " << index_time / 1e6 << " ms" << std::endl;
                for (const auto& entry : time_count_index) {
                    std::cout << entry.first << ": " << entry.second << " nanoseconds" << std::endl;
                }
                for (const auto& row : revenue_index) {
                    std::cout << safe_string(std::get<0>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<1>(row).c_str(), 25) << " ";
                    std::cout << safe_string(std::get<2>(row).c_str(), 4) << " ";
                    std::cout << std::get<3>(row) << std::endl;
                }
                break;
            }
            default:
                std::cerr << "Error: invalid index value" << std::endl;
                break;
        }

        // dump to response file
        std::ofstream response(RESP_PATH);
        json res_data;
        // 1. latency field
        res_data["latency"] = index_time / 1e6;
        // 2. res_table field
        json res_table;
        res_table["col_type"] = {"char25", "char25", "char4", "double"};
        json rows = json::array();
        for (const auto& row : revenue_index) {
            rows.push_back(json::array({
                safe_string(std::get<0>(row).c_str(), 25),
                safe_string(std::get<1>(row).c_str(), 25),
                safe_string(std::get<2>(row).c_str(), 4),
                std::get<3>(row)
            }));
        }
        res_table["rows"] = rows;
        res_data["res_table"] = res_table;
        // 3. tree field
        json tree;
        // bottom up
        json filter, filter_children, scan_lineitem;
        scan_lineitem["children"] = {};
        filter_children["scan lineitem"] = scan_lineitem;
        filter["children"] = filter_children;

        json nlj_li_sup, nlj_li_sup_children;
        json idx_search_sup;
        idx_search_sup["children"] = {};
        nlj_li_sup_children["filter (L_SHIPDATE)"] = filter;
        nlj_li_sup_children["index search(supplier)"] = idx_search_sup;
        nlj_li_sup["children"] = nlj_li_sup_children;
        nlj_li_sup["time"] = time_count_index["supplier"];

        json nlj_li_or, nlj_li_or_children;
        json idx_search_ord;
        idx_search_ord["children"] = {};
        nlj_li_or_children["Nested Loop Join"] = nlj_li_sup;
        nlj_li_or_children["index search(orders)"] = idx_search_ord;
        nlj_li_or["children"] = nlj_li_or_children;
        nlj_li_or["time"] = time_count_index["orders"];

        json nlj_or_cu, nlj_or_cu_children;
        json idx_search_cu;
        idx_search_cu["children"] = {};
        nlj_or_cu_children["Nested Loop Join"] = nlj_li_or;
        nlj_or_cu_children["index search(customer)"] = idx_search_cu;
        nlj_or_cu["children"] = nlj_or_cu_children;
        nlj_or_cu["time"] = time_count_index["customer"];

        json nlj_na_cu_su, nlj_na_cu_su_children;
        json idx_search_na;
        idx_search_na["children"] = {};
        nlj_na_cu_su_children["Nested Loop Join"] = nlj_or_cu;
        nlj_na_cu_su_children["index search(nation)"] = idx_search_na;
        nlj_na_cu_su["children"] = nlj_na_cu_su_children;
        nlj_na_cu_su["time"] = time_count_index["nation"];

        json agg, agg_children;
        agg_children["Nested Loop Join"] = nlj_na_cu_su;
        agg["children"] = agg_children;

        json sor, sor_children;
        sor_children["Aggregation"] = agg;
        sor["children"] = sor_children;

        tree["Sort"] = sor;
        res_data["tree"] = tree;

        // dump to response file
        std::ofstream res_file(RESP_PATH);
        if (!res_file) {
            std::cerr << "Failed to open response file" << std::endl;
            return;
        }
        res_file << res_data.dump(4) << std::endl;
        res_file.close();

    } catch (const json::parse_error& e) {
        file.close();
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        exit(1);
    }
}

void get_table(std::unordered_map<std::string, Table*> tables) {
    std::ifstream file(REQ_PATH);
    if (!file.is_open()) {
        std::cerr << "Failed to open file" << std::endl;
        exit(1);
    }
    json data;
    try {
        file >> data;
        if (!data.contains("table")) {
            std::cerr << "Error: missing required field (table)" << std::endl;
            file.close();
            return;
        }
        auto table = data["table"];
        if (!table.is_string()) {
            std::cerr << "Error: table name must be a string" << std::endl;
            file.close();
            return;
        }
        std::string table_name = table.get<std::string>();
        file.close();

        if (tables.find(table_name) == tables.end()) {
            std::cerr << "Error: table " << table_name << " not found" << std::endl;
            json res_data;
            res_data["error"] = "Table not found";
            // dump to response file
            std::ofstream res_file(RESP_PATH);
            if (!res_file) {
                std::cerr << "Failed to open response file" << std::endl;
                return;
            }
            res_file << res_data.dump(4) << std::endl;
            res_file.close();
            return;
        }

        // dump to response file
        std::ofstream response(RESP_PATH);
        json res_data, table_data;
        json col_name = json::array();
        json col_type = json::array();
        for (int i = 0; i < tables[table]->meta.type_size.size(); i++) {
            col_name.push_back(tables[table]->meta.columns_names[i]);
            if (tables[table]->meta.columns_types[i] == 0) {
                col_type.push_back("char" + std::to_string(tables[table]->meta.type_size[i]));
            } else if (tables[table]->meta.columns_types[i] == 1) {
                col_type.push_back("int");
            } else if (tables[table]->meta.columns_types[i] == 2) {
                col_type.push_back("double");
            } else {
                col_type.push_back("encrypted int");
            }
        }
        table_data["col_name"] = col_name;
        table_data["col_type"] = col_type;
        json rows = json::array();
        int row_num = std::min(15, (int)tables[table]->lines);
        auto row_it = tables[table]->row_begin();
        for (int i = 0; i < row_num; i++) {
            rows.push_back(row_it.get_row_array());
            ++row_it;
        }

        table_data["rows"] = rows;
        res_data["table"] = table_data;

        // dump to response file
        std::ofstream res_file(RESP_PATH);
        if (!res_file) {
            std::cerr << "Failed to open response file" << std::endl;
            return;
        }
        res_file << res_data.dump(4) << std::endl;
        res_file.close();
    } catch (const json::parse_error& e) {
        std::cerr << "JSON parse error: " << e.what() << std::endl;
        file.close();
        exit(1);
    }
}

int main() {
    std::vector<std::string> table_names = { "employees", "nation", "supplier", "customer", "orders", "lineitem"};
    // Load tables
    std::unordered_map<std::string, Table*> tables;
    for (const auto& table_name : table_names) {
        tables[table_name] = new Table(table_name);
    }
    std::cout << "Tables loaded" << std::endl;

    // Build indexes
    std::unordered_map<std::string, TEEIndex<uint64_t, uint64_t>*> TEEIdxMap;
    std::unordered_map<std::string, TieredBTree<uint64_t, uint64_t>*> TieredBTMap;
    for (const auto& table_name : table_names) {
        if (table_name == "lineitem") continue;
        std::string index_file = "tables/" + table_name + "_" + table_meta[table_name].index_cols[0] + ".col";
        std::vector<std::pair<uint64_t, uint64_t>> index_data;
        uint64_t index_size = load_data<std::pair<uint64_t, uint64_t>>(index_file, index_data);
        // Print row number
        std::cout << "Row number of " << table_name << " is " << index_size << std::endl;
        std::sort(index_data.begin(), index_data.end(), [](const auto& a, const auto& b) {
            return a.first < b.first;
        });
        int epsilon = 256;
        TEEIdxMap[table_name] = new TEEIndex<uint64_t, uint64_t>(index_data, epsilon, table_name + "_" + table_meta[table_name].index_cols[0]);
        TieredBTMap[table_name] = new TieredBTree<uint64_t, uint64_t>(index_data, table_name + "_" + table_meta[table_name].index_cols[0]);
    }
    std::cout << "Indexes built" << std::endl;
    TEEIdxMap["orders"]->size_report();


    // Initialize shared memory
    const int SIZE = 16;
    int shm_fd = shm_open(SIGNAL_PATH, O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(1);
    }
    void *ptr = mmap(0, SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (ptr == MAP_FAILED) {
        perror("mmap");
        exit(1);
    }

    // read uint64_t from shared memory
    uint64_t *signal = (uint64_t *)ptr;
    while (true) {
        if (*signal != 0) {
            std::cout << "signal: " << *signal << std::endl;
        }
        if (*signal == 0) {
            usleep(10000);
        } else if (*signal == 1) {
            // single-table join
            std::cout << "single-table join" << std::endl;
            single_table_join(tables, TEEIdxMap, TieredBTMap);
            *signal = 0;
        } else if (*signal == 2) {
            // multi-table join
            std::cout << "multi-table join" << std::endl;
            multi_table_join(tables, TEEIdxMap, TieredBTMap);
            *signal = 0;
        } else if (*signal == 3) {
            // get tables
            std::cout << "get table" << std::endl;
            get_table(tables);
            *signal = 0;
        } else if (*signal == 4) {
            // Q3 join
            std::cout << "Q3 join" << std::endl;
            Q3_join(tables, TEEIdxMap, TieredBTMap);
            *signal = 0;
        } else {
            *signal = 0;
            break;
        }
    }


    munmap(ptr, SIZE);
    close(shm_fd);
    
    return 0;
}