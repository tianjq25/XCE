#pragma once
#include <cstdint>
#include "index.hpp"
#include <table/table.hpp>
#include <tuple>

/*
    SELECT 
    emp.id AS employee,
    emp.name AS employee_name,
    mgr.name AS superior_name
    FROM 
        employees emp
    JOIN 
        employees mgr ON emp.manager_id = mgr.id;
 */
uint64_t join(Index<uint64_t, uint64_t> *index, Table *table, std::vector<uint64_t>& res_rows) {
    auto start = std::chrono::high_resolution_clock::now();
    auto it = table->column_begin("MANAGER_ID");
    while (it != table->column_end("MANAGER_ID")) {
        uint64_t manager_id = std::get<uint64_t>(*it);
        auto res = index->lookup(manager_id);
        if (res.first)
            res_rows.push_back(res.second);
        ++it;
    }
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

void join_example(TEEIndex<uint64_t, uint64_t> *index, Table *table, std::vector<uint64_t>& firsts, std::vector<uint64_t>& lasts, std::vector<uint64_t>& poses, std::vector<uint64_t> &row_ids) {
    auto it = table->column_begin("MANAGER_ID");
    int count = 0;
    while (it != table->column_end("MANAGER_ID")) {
        if (count > 10) break;
        uint64_t first, last, pos;
        uint64_t manager_id = std::get<uint64_t>(*it);
        auto res = index->lookup(manager_id, true, &first, &last, &pos);
        if (res.first) {
            count++;
            firsts.push_back(first);
            lasts.push_back(last);
            poses.push_back(pos);
            row_ids.push_back(res.second);
        }
        ++it;
    }
}

uint64_t join_detail(Index<uint64_t, uint64_t> *index, Table *table, std::vector<uint64_t>& res_rows, uint64_t &index_scan_time, int idx_type=1, json *rows=nullptr) {
    if (idx_type == 0) { // Without index
        auto start = std::chrono::high_resolution_clock::now();
        auto it = table->column_begin("MANAGER_ID");
        while (it != table->column_end("MANAGER_ID")) {
            uint64_t manager_id = std::get<uint64_t>(*it);
            auto search_it = table->column_begin("ID");
            uint64_t row_id = 0;
            while (search_it != table->column_end("ID")) {
                if (std::get<uint64_t>(*search_it) == manager_id) {
                    res_rows.push_back(row_id);
                    break;
                }
                ++search_it;
                ++row_id;
            }
            ++it;
        }
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    
    index_scan_time = 0;
    auto start = std::chrono::high_resolution_clock::now();
    auto it = table->column_begin("MANAGER_ID");
    while (it != table->column_end("MANAGER_ID")) {
        uint64_t manager_id = std::get<uint64_t>(*it);
        auto index_scan_start = std::chrono::high_resolution_clock::now();
        auto res = index->lookup(manager_id);
        auto index_scan_end = std::chrono::high_resolution_clock::now();
        index_scan_time += std::chrono::duration_cast<std::chrono::nanoseconds>(index_scan_end - index_scan_start).count();
        if (res.first)
            res_rows.push_back(res.second);
        ++it;
    }
    auto end = std::chrono::high_resolution_clock::now();

    if (idx_type == 1) { // LETIndex
        std::vector<uint64_t> firsts, lasts, poses, row_ids;
        join_example(dynamic_cast<TEEIndex<uint64_t, uint64_t>*>(index), table, firsts, lasts, poses, row_ids);
        auto row_it = table->row_begin();
        for (int i = 0; i < firsts.size(); i++) {
            auto mng_row = std::get<Employees>(*(row_it + row_ids[i]));
            auto emp_row = std::get<Employees>(*(row_it + i));
            rows->push_back(json::array({
                firsts[i],
                lasts[i],
                poses[i],
                row_ids[i],
                binary_to_hex(emp_row.MANAGER_ID, 24),
                safe_string(mng_row.NAME, 50),
            }));
        }
        // for (int i = 0; i < firsts.size(); i++)
        //     std::cout << "first: " << firsts[i] << ", last: " << lasts[i] << ", pos: " << poses[i] << ", row_id: " << row_ids[i] << std::endl;
    }

    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

/*
SELECT 
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT 
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(YEAR FROM l_shipdate) AS l_year,
        l_extendedprice*(1-l_discount) AS volume
    FROM 
        supplier, 
        lineitem, 
        orders, 
        customer, 
        nation n1,
        nation n2
    WHERE 
        s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND (
            (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') 
            OR 
            (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        )
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    GROUP BY 
        supp_nation, cust_nation, l_year
    ORDER BY 
        supp_nation, cust_nation, l_year;
*/
uint64_t naive_multi_table_join(std::unordered_map<std::string, Table*> &tables, 
    const std::string &nation1, const std::string &nation2,
    std::vector<std::tuple<std::string, std::string, std::string, double>> &revenue) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::tuple<std::string, std::string, std::string>, double> revenue_map;
    // Scan lineitems
    auto lineitem_it = tables["lineitem"]->row_begin();
    // *lineitem_it is a Lineitem struct
    while (lineitem_it != tables["lineitem"]->row_end()) {
        Lineitem lineitem = std::get<Lineitem>(*lineitem_it);
        if (std::string(lineitem.L_SHIPDATE, 4) != "1995" && std::string(lineitem.L_SHIPDATE, 4) != "1996") {
            ++lineitem_it;
            continue;
        }
    
        // Scan suppliers
        auto supplier_it = tables["supplier"]->row_begin();
        while (supplier_it != tables["supplier"]->row_end()) {
            Supplier supplier = std::get<Supplier>(*supplier_it);
            uint64_t suppkey = cipher.decrypt_int64(supplier.S_SUPPKEY, supplier_it.get_row_id());
            if (suppkey != lineitem.L_SUPPKEY) {
                ++supplier_it;
                continue;
            }


            // Scan orders
            auto order_it = tables["orders"]->row_begin();
            while (order_it != tables["orders"]->row_end()) {
                Orders order = std::get<Orders>(*order_it);
                uint64_t orderkey = cipher.decrypt_int64(order.O_ORDERKEY, order_it.get_row_id());
                if (orderkey != lineitem.L_ORDERKEY) {
                    ++order_it;
                    continue;
                }

                // Scan customers
                auto customer_it = tables["customer"]->row_begin();
                while (customer_it != tables["customer"]->row_end()) {
                    Customer customer = std::get<Customer>(*customer_it);
                    uint64_t custkey = cipher.decrypt_int64(customer.C_CUSTKEY, customer_it.get_row_id());
                    if (custkey != order.O_CUSTKEY) {
                        ++customer_it;
                        continue;
                    }

                    // Scan nations
                    Nation supp_nation, cust_nation;
                    auto nation_it = tables["nation"]->row_begin();
                    while (nation_it != tables["nation"]->row_end()) {
                        Nation nation = std::get<Nation>(*nation_it);
                        uint64_t nationkey = cipher.decrypt_int64(nation.N_NATIONKEY, nation_it.get_row_id());
                        if (nationkey == supplier.S_NATIONKEY) {
                            supp_nation = nation;
                        } else if (nationkey == customer.C_NATIONKEY) {
                            cust_nation = nation;
                        }

                        ++nation_it;
                    }

                    // Filter nations
                    if ((std::string(supp_nation.N_NAME, nation1.size()) == nation1 && std::string(cust_nation.N_NAME, nation2.size()) == nation2) || 
                        (std::string(supp_nation.N_NAME, nation2.size()) == nation2 && std::string(cust_nation.N_NAME, nation1.size()) == nation1)) {
                        revenue_map[{supp_nation.N_NAME, cust_nation.N_NAME, std::string(lineitem.L_SHIPDATE, 4)}] += lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT);
                    } else {
                        break;
                    }

                    ++customer_it;
                }
                ++order_it;
            }
            ++supplier_it;
        }
        ++lineitem_it;
    }

    for (const auto& entry : revenue_map) {
        revenue.emplace_back(
            std::get<0>(entry.first),
            std::get<1>(entry.first),
            std::get<2>(entry.first),
            entry.second
        );
    }
    std::sort(revenue.begin(), revenue.end());
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

uint64_t naive_multi_table_join_detail(std::unordered_map<std::string, Table*> &tables, 
    const std::string &nation1, const std::string &nation2,
    std::vector<std::tuple<std::string, std::string, std::string, double>> &revenue,
    std::map<std::string, uint64_t> &time_count) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::tuple<std::string, std::string, std::string>, double> revenue_map;

    std::cout << "Start scan lineitems" << std::endl;
    // Scan lineitems
    auto lineitem_it = tables["lineitem"]->row_begin();
    // *lineitem_it is a Lineitem struct
    while (lineitem_it != tables["lineitem"]->row_end()) {
        Lineitem lineitem = std::get<Lineitem>(*lineitem_it);
        if (std::string(lineitem.L_SHIPDATE, 4) != "1995" && std::string(lineitem.L_SHIPDATE, 4) != "1996") {
            ++lineitem_it;
            continue;
        }
    
        // Scan suppliers
        std::cout << "Start scan suppliers" << std::endl;
        auto supp_start = std::chrono::high_resolution_clock::now();
        auto supplier_it = tables["supplier"]->row_begin();
        std::cout << "row begin" << std::endl;
        while (supplier_it != tables["supplier"]->row_end()) {
            std::cout << "before get" << std::endl;
            Supplier supplier = std::get<Supplier>(*supplier_it);
            std::cout << "well" << std::endl;
            uint64_t suppkey = cipher.decrypt_int64(supplier.S_SUPPKEY, supplier_it.get_row_id());
            std::cout << "suppkey: " << suppkey << std::endl;
            if (suppkey!= lineitem.L_SUPPKEY) {
                ++supplier_it;
                std::cout << "continue" << std::endl;
                continue;
            }


            // Scan orders
            std::cout << "Start scan orders" << std::endl;
            auto order_start = std::chrono::high_resolution_clock::now();
            auto order_it = tables["orders"]->row_begin();
            while (order_it != tables["orders"]->row_end()) {
                Orders order = std::get<Orders>(*order_it);
                uint64_t orderkey = cipher.decrypt_int64(order.O_ORDERKEY, order_it.get_row_id());
                if (orderkey != lineitem.L_ORDERKEY) {
                    ++order_it;
                    continue;
                }

                // Scan customers
                std::cout << "Start scan customers" << std::endl;
                auto cust_start = std::chrono::high_resolution_clock::now();
                auto customer_it = tables["customer"]->row_begin();
                while (customer_it != tables["customer"]->row_end()) {
                    Customer customer = std::get<Customer>(*customer_it);
                    uint64_t custkey = cipher.decrypt_int64(customer.C_CUSTKEY, customer_it.get_row_id());
                    if (custkey != order.O_CUSTKEY) {
                        ++customer_it;
                        continue;
                    }

                    // Scan nations
                    std::cout << "Start scan nations" << std::endl;
                    auto nation_start = std::chrono::high_resolution_clock::now();
                    Nation supp_nation, cust_nation;
                    auto nation_it = tables["nation"]->row_begin();
                    while (nation_it != tables["nation"]->row_end()) {
                        Nation nation = std::get<Nation>(*nation_it);
                        uint64_t nationkey = cipher.decrypt_int64(nation.N_NATIONKEY, nation_it.get_row_id());
                        if (nationkey == supplier.S_NATIONKEY) {
                            supp_nation = nation;
                        } else if (nationkey == customer.C_NATIONKEY) {
                            cust_nation = nation;
                        }

                        ++nation_it;
                    }
                    auto nation_end = std::chrono::high_resolution_clock::now();
                    time_count["nation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(nation_end - nation_start).count();

                    // Filter nations
                    if ((std::string(supp_nation.N_NAME, nation1.size()) == nation1 && std::string(cust_nation.N_NAME, nation2.size()) == nation2) || 
                        (std::string(supp_nation.N_NAME, nation2.size()) == nation2 && std::string(cust_nation.N_NAME, nation1.size()) == nation1)) {
                        std::cout << "Start aggregation" << std::endl;
                        auto agg_start = std::chrono::high_resolution_clock::now();
                        revenue_map[{supp_nation.N_NAME, cust_nation.N_NAME, std::string(lineitem.L_SHIPDATE, 4)}] += lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT);
                        auto agg_end = std::chrono::high_resolution_clock::now();
                        time_count["aggregation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(agg_end - agg_start).count();
                    } else {
                        break;
                    }

                    ++customer_it;
                }
                auto cust_end = std::chrono::high_resolution_clock::now();
                time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();
                ++order_it;
            }
            auto order_end = std::chrono::high_resolution_clock::now();
            time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(order_end - order_start).count();
            ++supplier_it;
        }
        auto supp_end = std::chrono::high_resolution_clock::now();
        time_count["supplier"] += std::chrono::duration_cast<std::chrono::nanoseconds>(supp_end - supp_start).count();
        ++lineitem_it;
    }
    time_count["supplier"] -= time_count["orders"];
    time_count["orders"] -= time_count["customer"];
    time_count["customer"] -= (time_count["nation"] + time_count["aggregation"]);
    std::cout << "Start sort" << std::endl;
    auto sort_start = std::chrono::high_resolution_clock::now();
    for (const auto& entry : revenue_map) {
        revenue.emplace_back(
            std::get<0>(entry.first),
            std::get<1>(entry.first),
            std::get<2>(entry.first),
            entry.second
        );
    }
    std::sort(revenue.begin(), revenue.end());
    auto sort_end = std::chrono::high_resolution_clock::now();
    time_count["sort"] = std::chrono::duration_cast<std::chrono::nanoseconds>(sort_end - sort_start).count();
    auto end = sort_end;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}

template <typename T>
uint64_t index_multi_table_join(std::unordered_map<std::string, Table*> &tables, 
    std::unordered_map<std::string, T*> &indexes,
    const std::string &nation1, const std::string &nation2,
    std::vector<std::tuple<std::string, std::string, std::string, double>> &revenue) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::tuple<std::string, std::string, std::string>, double> revenue_map;
    // Scan lineitems
    auto lineitem_it = tables["lineitem"]->row_begin();
    while (lineitem_it != tables["lineitem"]->row_end()) {
        Lineitem lineitem = std::get<Lineitem>(*lineitem_it);
        if (std::string(lineitem.L_SHIPDATE, 4) != "1995" && std::string(lineitem.L_SHIPDATE, 4) != "1996") {
            ++lineitem_it;
            continue;
        }

        // Search suppliers
        auto sup_res = indexes["supplier"]->lookup(lineitem.L_SUPPKEY);
        if (!sup_res.first) {
            ++lineitem_it;
            continue;
        }
        Supplier supplier = std::get<Supplier>(*(tables["supplier"]->row_begin() + sup_res.second));

        // Search orders
        auto ord_res = indexes["orders"]->lookup(lineitem.L_ORDERKEY);
        if (!ord_res.first) {
            ++lineitem_it;
            continue;
        }
        Orders order = std::get<Orders>(*(tables["orders"]->row_begin() + ord_res.second));

        // Search customers
        auto cust_res = indexes["customer"]->lookup(order.O_CUSTKEY);
        if (!cust_res.first) {
            ++lineitem_it;
            continue;
        }
        Customer customer = std::get<Customer>(*(tables["customer"]->row_begin() + cust_res.second));

        // Search nations
        Nation supp_nation, cust_nation;
        auto supp_nation_res = indexes["nation"]->lookup(supplier.S_NATIONKEY);
        if (supp_nation_res.first) {
            supp_nation = std::get<Nation>(*(tables["nation"]->row_begin() + supp_nation_res.second));
        } else {
            std::cerr << "Supplier nation not found" << std::endl;
            ++lineitem_it;
            continue;
        }
        auto cust_nation_res = indexes["nation"]->lookup(customer.C_NATIONKEY);
        if (cust_nation_res.first) {
            cust_nation = std::get<Nation>(*(tables["nation"]->row_begin() + cust_nation_res.second));
        } else {
            std::cerr << "Customer nation not found" << std::endl;
            ++lineitem_it;
            continue;
        }

        // Filter nations
        if ((std::string(supp_nation.N_NAME, nation1.size()) == nation1 && std::string(cust_nation.N_NAME, nation2.size()) == nation2) || 
            (std::string(supp_nation.N_NAME, nation2.size()) == nation2 && std::string(cust_nation.N_NAME, nation1.size()) == nation1)) {
            revenue_map[{supp_nation.N_NAME, cust_nation.N_NAME, std::string(lineitem.L_SHIPDATE, 4)}] += lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT);
        }

        ++lineitem_it;
    }
    for (const auto& entry : revenue_map) {
        revenue.emplace_back(
            std::get<0>(entry.first),
            std::get<1>(entry.first),
            std::get<2>(entry.first),
            entry.second
        );
    }
    std::sort(revenue.begin(), revenue.end());
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}


template <typename T>
uint64_t index_multi_table_join_detail(std::unordered_map<std::string, Table*> &tables, 
    std::unordered_map<std::string, T*> &indexes,
    const std::string &nation1, const std::string &nation2,
    std::vector<std::tuple<std::string, std::string, std::string, double>> &revenue,
    std::map<std::string, uint64_t> &time_count) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::tuple<std::string, std::string, std::string>, double> revenue_map;
    
    // Scan lineitems
    auto lineitem_it = tables["lineitem"]->row_begin();
    while (lineitem_it != tables["lineitem"]->row_end()) {
        Lineitem lineitem = std::get<Lineitem>(*lineitem_it);
        if (std::string(lineitem.L_SHIPDATE, 4) != "1995" && std::string(lineitem.L_SHIPDATE, 4) != "1996") {
            ++lineitem_it;
            continue;
        }

        // Search suppliers
        auto supp_start = std::chrono::high_resolution_clock::now();
        auto sup_res = indexes["supplier"]->lookup(lineitem.L_SUPPKEY);
        if (!sup_res.first) {
            ++lineitem_it;
            auto supp_end = std::chrono::high_resolution_clock::now();
            time_count["supplier"] += std::chrono::duration_cast<std::chrono::nanoseconds>(supp_end - supp_start).count();
            continue;
        }
        Supplier supplier = std::get<Supplier>(*(tables["supplier"]->row_begin() + sup_res.second));
        auto supp_end = std::chrono::high_resolution_clock::now();
        time_count["supplier"] += std::chrono::duration_cast<std::chrono::nanoseconds>(supp_end - supp_start).count();

        // Search orders
        auto ord_start = std::chrono::high_resolution_clock::now();
        auto ord_res = indexes["orders"]->lookup(lineitem.L_ORDERKEY);
        if (!ord_res.first) {
            ++lineitem_it;
            auto ord_end = std::chrono::high_resolution_clock::now();
            time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(ord_end - ord_start).count();
            continue;
        }
        Orders order = std::get<Orders>(*(tables["orders"]->row_begin() + ord_res.second));
        auto ord_end = std::chrono::high_resolution_clock::now();
        time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(ord_end - ord_start).count();

        // Search customers
        auto cust_start = std::chrono::high_resolution_clock::now();
        auto cust_res = indexes["customer"]->lookup(order.O_CUSTKEY);
        if (!cust_res.first) {
            ++lineitem_it;
            auto cust_end = std::chrono::high_resolution_clock::now();
            time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();
            continue;
        }
        Customer customer = std::get<Customer>(*(tables["customer"]->row_begin() + cust_res.second));
        auto cust_end = std::chrono::high_resolution_clock::now();
        time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();

        // Search nations
        auto nation_start = std::chrono::high_resolution_clock::now();
        Nation supp_nation, cust_nation;
        auto supp_nation_res = indexes["nation"]->lookup(supplier.S_NATIONKEY);
        if (supp_nation_res.first) {
            supp_nation = std::get<Nation>(*(tables["nation"]->row_begin() + supp_nation_res.second));
        } else {
            std::cerr << "Supplier nation not found" << std::endl;
            ++lineitem_it;
            auto nation_end = std::chrono::high_resolution_clock::now();
            time_count["nation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(nation_end - nation_start).count();
            continue;
        }
        auto cust_nation_res = indexes["nation"]->lookup(customer.C_NATIONKEY);
        if (cust_nation_res.first) {
            cust_nation = std::get<Nation>(*(tables["nation"]->row_begin() + cust_nation_res.second));
        } else {
            std::cerr << "Customer nation not found" << std::endl;
            ++lineitem_it;
            auto nation_end = std::chrono::high_resolution_clock::now();
            time_count["nation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(nation_end - nation_start).count();
            continue;
        }
        auto nation_end = std::chrono::high_resolution_clock::now();
        time_count["nation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(nation_end - nation_start).count();

        // Filter nations
        if ((std::string(supp_nation.N_NAME, nation1.size()) == nation1 && std::string(cust_nation.N_NAME, nation2.size()) == nation2) || 
            (std::string(supp_nation.N_NAME, nation2.size()) == nation2 && std::string(cust_nation.N_NAME, nation1.size()) == nation1)) {
            auto agg_start = std::chrono::high_resolution_clock::now();
            revenue_map[{supp_nation.N_NAME, cust_nation.N_NAME, std::string(lineitem.L_SHIPDATE, 4)}] += lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT);
            auto agg_end = std::chrono::high_resolution_clock::now();
            time_count["aggregation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(agg_end - agg_start).count();
        }
        
        ++lineitem_it;
    }
    auto sort_start = std::chrono::high_resolution_clock::now();
    for (const auto& entry : revenue_map) {
        revenue.emplace_back(
            std::get<0>(entry.first),
            std::get<1>(entry.first),
            std::get<2>(entry.first),
            entry.second
        );
    }
    std::sort(revenue.begin(), revenue.end());
    auto sort_end = std::chrono::high_resolution_clock::now();
    time_count["sort"] = std::chrono::duration_cast<std::chrono::nanoseconds>(sort_end - sort_start).count();
    auto end = sort_end;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}


/*
TPC-H query 3

select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'BUILDING'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-12-31'
	and l_shipdate > '1995-12-31'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
*/
template <typename T>
uint64_t index_Q3_detail(std::unordered_map<std::string, Table*> &tables, 
    std::unordered_map<std::string, T*> &indexes,
    const std::string &mktsegment,
    std::vector<std::tuple<uint64_t, double, std::string, uint64_t>> &revenue,
    std::map<std::string, uint64_t> &time_count) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::tuple<uint64_t, std::string, uint64_t>, double> revenue_map;
    
    // Scan lineitems
    auto lineitem_it = tables["lineitem"]->row_begin();
    while (lineitem_it != tables["lineitem"]->row_end()) {
        Lineitem lineitem = std::get<Lineitem>(*lineitem_it);
        if (std::stoi(std::string(lineitem.L_SHIPDATE, 4)) <= 1994) {
            ++lineitem_it;
            continue;
        }

        // Search orders
        auto ord_start = std::chrono::high_resolution_clock::now();
        auto ord_res = indexes["orders"]->lookup(lineitem.L_ORDERKEY);
        if (!ord_res.first) {
            ++lineitem_it;
            auto ord_end = std::chrono::high_resolution_clock::now();
            time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(ord_end - ord_start).count();
            continue;
        }
        Orders order = std::get<Orders>(*(tables["orders"]->row_begin() + ord_res.second));
        if (std::stoi(std::string(order.O_ORDERDATE, 4)) >= 1995) {
            ++lineitem_it;
            auto ord_end = std::chrono::high_resolution_clock::now();
            time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(ord_end - ord_start).count();
            continue;
        }
        auto ord_end = std::chrono::high_resolution_clock::now();
        time_count["orders"] += std::chrono::duration_cast<std::chrono::nanoseconds>(ord_end - ord_start).count();

        // Search customers
        auto cust_start = std::chrono::high_resolution_clock::now();
        auto cust_res = indexes["customer"]->lookup(order.O_CUSTKEY);
        if (!cust_res.first) {
            ++lineitem_it;
            auto cust_end = std::chrono::high_resolution_clock::now();
            time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();
            continue;
        }
        Customer customer = std::get<Customer>(*(tables["customer"]->row_begin() + cust_res.second));
        if (std::string(customer.C_MKTSEGMENT, mktsegment.size()) != mktsegment) {
            ++lineitem_it;
            auto cust_end = std::chrono::high_resolution_clock::now();
            time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();
            continue;
        }
        auto cust_end = std::chrono::high_resolution_clock::now();
        time_count["customer"] += std::chrono::duration_cast<std::chrono::nanoseconds>(cust_end - cust_start).count();

        auto agg_start = std::chrono::high_resolution_clock::now();
        revenue_map[{lineitem.L_ORDERKEY, std::string(order.O_ORDERDATE, 10), order.O_SHIPPRIORITY}] += lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT);
        auto agg_end = std::chrono::high_resolution_clock::now();
        time_count["aggregation"] += std::chrono::duration_cast<std::chrono::nanoseconds>(agg_end - agg_start).count();
        ++lineitem_it;
    }
    auto sort_start = std::chrono::high_resolution_clock::now();
    for (const auto& entry : revenue_map) {
        revenue.emplace_back(
            std::get<0>(entry.first),
            entry.second,
            std::get<1>(entry.first),
            std::get<2>(entry.first)
        );
    }
    std::sort(revenue.begin(), revenue.end());
    auto sort_end = std::chrono::high_resolution_clock::now();
    time_count["sort"] = std::chrono::duration_cast<std::chrono::nanoseconds>(sort_end - sort_start).count();
    auto end = sort_end;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}