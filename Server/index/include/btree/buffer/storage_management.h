/**
 * @file storage_management.h
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2022-02-12
 * 
 * @copyright Copyright (c) 2022
 * 
 */
#pragma once

#include<stdio.h>
#include<stdlib.h>
#include "../utility.h"
#include <map>
#include<iostream>
#include <stdint.h>
#include <cstring>
#include <chrono>

#define Caching 0

class StorageManager {
    private:
    char *file_name = nullptr;
    FILE *fp = nullptr;
    #if Caching
    std::map<int, Block> block_cache; // LRU setting?
    #endif

    void _write_block(void *data, int block_id) {
        fseek(fp, block_id * BlockSize, SEEK_SET);
        fwrite(data, BlockSize, 1, fp);
        return;
    }

    void _read_block(void *data, int block_id) {
        fseek(fp, block_id * BlockSize, SEEK_SET);
        fread(data, BlockSize, 1, fp);
        return;
    }

    char* _allocate_new_block() {
        char* ptr = nullptr;
        ptr = (char *)malloc(BlockSize * sizeof(char));
        return ptr;
    }

    void _get_file_handle() {
        fp = fopen(file_name,"r+b");
    }

    void _create_file(bool bulk) {
        fp = fopen(file_name,"wb");
        char empty_block[BlockSize];
        MetaNode mn;
        mn.block_count = 2;
        mn.root_block_id = 1;
        mn.level = 1;
        memcpy(empty_block, &mn, MetaNodeSize);
        _write_block(empty_block, 0);
        if (!bulk) {
            LeaftNodeHeader lnh;
            lnh.item_count = 0;
            lnh.node_type = LeafNodeType;
            lnh.level = 1; // level starts from 1
            memcpy(empty_block, &lnh, LeaftNodeHeaderSize);
            _write_block(empty_block, 1);
        }
        _close_file_handle();
        _get_file_handle();
    }

    void _close_file_handle() {
        fclose(fp);
    }

    public:
        StorageManager(char *fn, bool first = false, bool bulk_load = false) {
            std::string path = std::string("data/TieredBT/") + fn + ".col";
            file_name = strdup(path.c_str());
            if (first) {
                _create_file(bulk_load);
            } else {
                _get_file_handle();
            }
            
        }

        StorageManager(bool first, char *fn) {
            std::string path = std::string("data/TieredBT/") + fn + ".col";
            file_name = strdup(path.c_str());

            if (first) {
                fp = fopen(file_name,"wb");

                char empty_block[BlockSize];
                MetaNode mn;
                mn.block_count = 1;
                mn.level = 0;
                memcpy(empty_block, &mn, MetaNodeSize);
                _write_block(empty_block, 0);

                _close_file_handle();
            }
            _get_file_handle();
        }

        StorageManager() = default;

        ~StorageManager() {
            if (fp != nullptr) _close_file_handle();
        }  

        Block get_block(int block_id) {
            #ifdef IO_PROFILING
            io_stats.read_block++;
            auto start = std::chrono::high_resolution_clock::now();
            #endif
            Block block;
            //char data[BlockSize];
            _read_block(block.data, block_id);
            //memcpy(block.data, data, BlockSize);
            #ifdef IO_PROFILING
            auto end = std::chrono::high_resolution_clock::now();
            io_stats.read_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            #endif
            return block;
        }

        void get_block(int block_id, char *data) {
            #ifdef IO_PROFILING
            io_stats.read_block++;
            auto start = std::chrono::high_resolution_clock::now();
            #endif
            _read_block(data, block_id);
            #ifdef IO_PROFILING
            auto end = std::chrono::high_resolution_clock::now();
            io_stats.read_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            #endif
        }

        void write_block(int block_id, Block block) {
            #ifdef IO_PROFILING
            io_stats.write_block++;
            auto start = std::chrono::high_resolution_clock::now();
            #endif
            _write_block(&block, block_id);
            #ifdef IO_PROFILING
            auto end = std::chrono::high_resolution_clock::now();
            io_stats.write_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            #endif
        }

        size_t get_file_size() {
            fseek(fp, 0, SEEK_END);
            return ftell(fp);
        }

};