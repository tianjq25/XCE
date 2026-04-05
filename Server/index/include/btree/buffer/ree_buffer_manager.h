#ifndef REE_BUFFER_MANAGER_H
#define REE_BUFFER_MANAGER_H

#include <cstddef>
#include <ios>
#include <unordered_map>
#include <list>
#include "lru.h"
#include "storage_management.h"
#include "../utility.h"

#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */
#include <sys/types.h>
#include <unistd.h>          /* For close */


class REEBufferManager {
public:
    REEBufferManager(char *fn, bool first = false, bool bulk_load = false) {
        sm = new StorageManager(fn, first, bulk_load);
        // create shared memory
        std::string shm_name = std::string("/") + std::string(fn) + "_shm";
        shm_fd = shm_open(shm_name.c_str(), O_CREAT | O_RDWR, 0666);
        // shm_fd = shm_open("/btree_shm", O_CREAT | O_RDWR, 0666);
        if (shm_fd == -1) {
            std::cerr << "Failed to create shared memory." << std::endl;
            exit(1);
        }

        ftruncate(shm_fd, total_size);
        shm_ptr = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (shm_ptr == MAP_FAILED) {
            std::cerr << "Failed to map shared memory." << std::endl;
            exit(1);
        }

        // pages = new Page[REEBufferPoolSize];
        pages = (Page *)shm_ptr;
        for (size_t i = 0; i < REEBufferPoolSize; ++i) {
            free_list.push_back(&pages[i]);
        }
    }

    void save_buffer(int block_id, Block block) {
        if (!free_list.empty()) {
            Page *page = free_list.front();
            free_list.pop_front();
            page_table[block_id] = page;
            lru.put(block_id);
            page->block = block;
            page->is_dirty = false;
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
            page->is_dirty = false;
        }
    }

    Block get_block(int block_id) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            return page_table[block_id]->block;
        } else {
            sm->get_block(block_id, temp_block.data);
            return temp_block;

            // if (!free_list.empty()) {
            //     Page *page = free_list.front();
            //     free_list.pop_front();
            //     page_table[block_id] = page;
            //     lru.put(block_id);
            //     sm->get_block(block_id, page->block.data);
            //     return page->block;
            // } else {
            //     int evict_block_id;
            //     lru.evict(evict_block_id);
            //     Page *page = page_table[evict_block_id];
            //     if (page->is_dirty) {
            //         sm->write_block(evict_block_id, page->block);
            //     }
            //     page_table.erase(evict_block_id);
            //     page_table[block_id] = page;
            //     lru.put(block_id);
            //     sm->get_block(block_id, page->block.data);
            //     return page->block;
            // }
        }
    }

    bool get_block(int block_id, char *data) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            memcpy(data, page_table[block_id]->block.data, BlockSize);
            return true;
        } else {
            sm->get_block(block_id, data);
            return false;

            // if (!free_list.empty()) {
            //     Page *page = free_list.front();
            //     free_list.pop_front();
            //     page_table[block_id] = page;
            //     lru.put(block_id);
            //     sm->get_block(block_id, data);
            //     memcpy(page->block.data, data, BlockSize);
            // } else {
            //     int evict_block_id;
            //     lru.evict(evict_block_id);
            //     Page *page = page_table[evict_block_id];
            //     if (page->is_dirty) {
            //         sm->write_block(evict_block_id, page->block);
            //     }
            //     page_table.erase(evict_block_id);
            //     page_table[block_id] = page;
            //     lru.put(block_id);
            //     sm->get_block(block_id, data);
            //     memcpy(page->block.data, data, BlockSize);
            // }
            // return false;
        }
    }

    bool write_block(int block_id, Block block, bool is_inner = false) {
        if (!is_inner) {
            sm->write_block(block_id, block);
            return true;
        }
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

    ~REEBufferManager() {
        for (auto &it : page_table) {
            if (it.second->is_dirty) {
                sm->write_block(it.first, it.second->block);
            }
        }
        delete sm;
        // delete[] pages;

        munmap(shm_ptr, total_size);
        close(shm_fd);
        shm_unlink("btree_shm");
    }

private:
    size_t total_size = sizeof(Page) * REEBufferPoolSize;
    int shm_fd;
    void *shm_ptr;
    StorageManager *sm;
    Block temp_block;
    Page *pages;
    std::unordered_map<int, Page*> page_table;
    std::list<Page *> free_list;
    LRU<int> lru;
};
#endif //REE_BUFFER_MANAGER_H