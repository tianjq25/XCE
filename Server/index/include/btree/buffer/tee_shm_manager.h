#ifndef TEE_SHM_MANAGER_H
#define TEE_SHM_MANAGER_H

#include "atomic_queue.h"
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */
#include <sys/types.h>
#include <unistd.h>          /* For close */
#include "../utility.h"
#include <chrono>

class TeeShmManager {
public:
    TeeShmManager(char *fn, bool first = false, bool bulk_load = false) {
        // create shared memory
        shm_fd = shm_open("/btree_shm", O_CREAT | O_RDWR, 0666);
        if (shm_fd == -1) {
            std::cerr << "Failed to create shared memory." << std::endl;
            exit(1);
        }
        if (ftruncate(shm_fd, total_size) == -1) {
            // std::cerr << "ftruncate error" << std::endl;
            // std::cerr << "shared memory has been truncated" << std::endl;
        }
        ptr = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to map shared memory." << std::endl;
            exit(1);
        }

        // std::cout << "create shared memory" << std::endl;

        t2r = new(ptr) BlockBuffer();  // 在共享内存中构造队列对象
        r2t = t2r + 1;
        flag_buffer = static_cast<FlagBuffer*>((void*)(r2t + 1));
        block_id_buffer = static_cast<BlockIDBuffer*>((void*)(flag_buffer + 1));

        // std::cout << "create queue" << std::endl;

        // open file
        flag_buffer->push(FILE_OPEN);
        Block block;
        // | first(1) | bulk_load(1) | fn length(4) | fn |
        block.data[0] = first;
        block.data[1] = bulk_load;
        int fn_len = strlen(fn);
        // std::cout << "fn_len: " << fn_len << std::endl;
        memcpy(block.data + 2, &fn_len, sizeof(int));
        memcpy(block.data + 2 + sizeof(int), fn, fn_len);

        t2r->push(block);

        // std::cout << "send file info" << std::endl;
    }

    uint64_t get_block(int block_id, char *data) {
        std::cout << "tee_shm_get_block" << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        flag_buffer->push(READ);
        Block block;
        memcpy(block.data, &block_id, sizeof(int));
        t2r->push(block);
        block = r2t->pop();
        memcpy(data, block.data, BlockSize);
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    uint64_t write_block(int block_id, Block block) {
        auto start = std::chrono::high_resolution_clock::now();
        flag_buffer->push(WRITE);
        // Block block_id_block;
        // memcpy(block_id_block.data, &block_id, sizeof(int));
        // t2r->push(block_id_block);
        block_id_buffer->push(block_id);
        t2r->push(block);
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }

    size_t get_file_size() {
        flag_buffer->push(FILE_SIZE);
        Block block = r2t->pop();
        size_t file_size;
        memcpy(&file_size, block.data, sizeof(size_t));
        return file_size;
    }

    ~TeeShmManager() {
        flag_buffer->push(FILE_CLOSE);
        munmap(ptr, total_size);
        close(shm_fd);
    }

private:
    size_t total_size = 2 * sizeof(BlockBuffer) + sizeof(FlagBuffer) + sizeof(BlockIDBuffer) + sizeof(SaveBuffer);
    int shm_fd;
    void* ptr;
    BlockBuffer* t2r;
    BlockBuffer* r2t;
    FlagBuffer* flag_buffer;
    BlockIDBuffer* block_id_buffer;
};

#endif //TEE_SHM_MANAGER_H