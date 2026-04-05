#ifndef INDEX_HPP
#define INDEX_HPP

#include <cstdint>
#include <utility>
#include <vector>
#include "pgm/pgm_index_dynamic.hpp"
#include "btree/b_tree.h"

template <typename Key, typename Value>
class Index {
public:
    virtual ~Index() = default;

    virtual std::pair<bool, Value> lookup(const Key& key) const = 0;
};

template <typename Key, typename Value>
class TEEIndex : public Index<Key, Value> {
public:
    TEEIndex(std::vector<std::pair<uint64_t, uint64_t>> data, int epsilon, std::string col_name) {
        index = new pgm_disk::DynamicPGMIndex<uint64_t, uint64_t>(true, false, data.begin(), data.end(), epsilon, col_name);
    }

    std::pair<bool, Value> lookup(const Key& key, bool detail=false, uint64_t* first=nullptr, uint64_t* last=nullptr, uint64_t* pos=nullptr) {
        auto res = index->find_on_disk_pgm(key, detail, first, last, pos);
        return {res.first, res.second};
    }

    std::pair<bool, Value> lookup(const Key& key) const override {
        auto res = index->find_on_disk_pgm(key);
        return {res.first, res.second};
    }

    void size_report() {
        index->size_report();
    }

    ~TEEIndex() {
        delete index;
    }

private:
    pgm_disk::DynamicPGMIndex<uint64_t, uint64_t> *index;
};

template <typename Key, typename Value>
class TieredBTree : public Index<Key, Value> {
public:
    TieredBTree(std::vector<std::pair<uint64_t, uint64_t>> data, std::string col_name) {
        LeafNodeIterm* leaf_items = reinterpret_cast<LeafNodeIterm*>(data.data());
        index = new BTree(ALL_DISK, true, const_cast<char*>(col_name.c_str()), true);
        index->bulk_load(leaf_items, data.size());
    }

    std::pair<bool, Value> lookup(const Key& key) const override {
        auto res = index->lookup(key);
        return {res.first, res.second};
    }

    ~TieredBTree() {
        delete index;
    }

private:
    BTree *index;
};

#endif // INDEX_HPP