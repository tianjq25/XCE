#ifndef B_TREE_BUFFER_POOL_MANAGER_H
#define B_TREE_BUFFER_POOL_MANAGER_H
#include <unordered_map>
#include <list>
#include "lru.h"
#include "storage_management.h"
#include "../utility.h"


struct Page {
    Block block;
    bool is_dirty = false;
};

class BufferPoolManager {
public:
    BufferPoolManager(char *fn, bool first = false, bool bulk_load = false) {
        sm = new StorageManager(fn, first, bulk_load);
        pages = new Page[BufferPoolSize];
        for (size_t i = 0; i < BufferPoolSize; ++i) {
            free_list.push_back(&pages[i]);
        }
    }

    Block get_block(int block_id) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            return page_table[block_id]->block;
        } else {
            if (!free_list.empty()) {
                Page *page = free_list.front();
                free_list.pop_front();
                page_table[block_id] = page;
                lru.put(block_id);
                sm->get_block(block_id, page->block.data);
                return page->block;
            } else {
                int evict_block_id;
                lru.evict(evict_block_id);
                Page *page = page_table[evict_block_id];
                if (page->is_dirty) {
                    sm->write_block(evict_block_id, page->block);
                }
                page_table.erase(evict_block_id);
                page_table[block_id] = page;
                lru.put(block_id);
                sm->get_block(block_id, page->block.data);
                return page->block;
            }
        }
    }

    void get_block(int block_id, char *data) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            memcpy(data, page_table[block_id]->block.data, BlockSize);
        } else {
            if (!free_list.empty()) {
                Page *page = free_list.front();
                free_list.pop_front();
                page_table[block_id] = page;
                lru.put(block_id);
                sm->get_block(block_id, data);
                memcpy(page->block.data, data, BlockSize);
            } else {
                int evict_block_id;
                lru.evict(evict_block_id);
                Page *page = page_table[evict_block_id];
                if (page->is_dirty) {
                    sm->write_block(evict_block_id, page->block);
                }
                page_table.erase(evict_block_id);
                page_table[block_id] = page;
                lru.put(block_id);
                sm->get_block(block_id, data);
                memcpy(page->block.data, data, BlockSize);
            }
        }
    }

    bool write_block(int block_id, Block block) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            page_table[block_id]->block = block;
            page_table[block_id]->is_dirty = true;
            return true;
        } else {
            if (!free_list.empty()) {
                Page *page = free_list.front();
                free_list.pop_front();
                page_table[block_id] = page;
                lru.put(block_id);
                page->block = block;
                page->is_dirty = true;
                return true;
            } else {
                int evict_block_id;
                lru.evict(evict_block_id);
                Page *page = page_table[evict_block_id];
                if (page->is_dirty) {
                    sm->write_block(evict_block_id, page->block);
                }
                page_table.erase(evict_block_id);
                page_table[block_id] = page;
                lru.put(block_id);
                page->block = block;
                page->is_dirty = true;
                return true;
            }
        }
    }

    size_t get_file_size() {
        return sm->get_file_size();
    }

    ~BufferPoolManager() {
        for (auto &it : page_table) {
            if (it.second->is_dirty) {
                sm->write_block(it.first, it.second->block);
            }
        }
        delete sm;
        delete[] pages;
    }

private:
    StorageManager *sm;
    Page *pages;
    std::unordered_map<int, Page*> page_table;
    std::list<Page *> free_list;
    LRU<int> lru;
};


#endif //B_TREE_BUFFER_POOL_MANAGER_H
