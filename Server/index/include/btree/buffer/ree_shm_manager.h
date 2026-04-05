#ifndef REE_SHM_MANAGER_H
#define REE_SHM_MANAGER_H
#include "atomic_queue.h"
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>        /* For mode constants */
#include <fcntl.h>           /* For O_* constants */
#include <unistd.h>          /* For close */
#include "../utility.h"
#include "ree_buffer_manager.h"

class REEShmManager {
public:
    REEShmManager() {
        // create shared memory
        shm_fd = shm_open("/btree_shm", O_RDWR, 0666);
        while(shm_fd == -1) {
            std::cout << "waiting for shared memory..." << std::endl;
            sleep(10);
            shm_fd = shm_open("/btree_shm", O_RDWR, 0666);
        }
        // if (shm_fd == -1) {
        //     std::cerr << "Failed to create shared memory." << std::endl;
        //     exit(1);
        // }
        ptr = mmap(nullptr, total_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
        if (ptr == MAP_FAILED) {
            std::cerr << "Failed to map shared memory." << std::endl;
            exit(1);
        }

        std::cout << "create shared memory" << std::endl;

        t2r = static_cast<BlockBuffer*>(ptr);  // 在共享内存中构造队列对象
        r2t = t2r + 1;
        flag_buffer = static_cast<FlagBuffer*>((void*)(r2t + 1));
        block_id_buffer = static_cast<BlockIDBuffer*>((void*)(flag_buffer + 1));


        std::cout << "create queue" << std::endl;

        // read file info
        uint8_t flag = flag_buffer->pop();
        std::cout << "flag: " << (int)flag << std::endl;
        if (flag != FILE_OPEN) {
            std::cerr << "Unknown flag: " << (int)flag << std::endl;
            exit(1);
        }

        Block block = t2r->pop();

        // | first(1) | bulk_load(1) | fn length(4) | fn |
        bool first = block.data[0];
        bool bulk_load = block.data[1];
        int fn_len;
        memcpy(&fn_len, block.data + 2, sizeof(int));
        std::cout << "fn_len: " << fn_len << std::endl;
        char fn[fn_len + 1];
        memcpy(fn, block.data + 2 + sizeof(int), fn_len);
        std::cout << "read file info" << std::endl;

        // initialize REEBufferManager
        rbm = new REEBufferManager(fn, first, bulk_load);
        std::cout << "initialize REEBufferManager" << std::endl;
    }

    void run() {
        char data[BlockSize];
        while (1) {
            uint8_t flag = flag_buffer->pop();
            // std::cout << "flag: " << (int)flag << std::endl;
            if (flag == READ) {
                Block block = t2r->pop();
                int block_id;
                memcpy(&block_id, block.data, sizeof(int));
                rbm->get_block(block_id, data);
                memcpy(block.data, data, BlockSize);
                r2t->push(block);
            } else if (flag == WRITE) {
                int block_id = block_id_buffer->pop();
                Block block = t2r->pop();
                rbm->write_block(block_id, block);

                // Block block = t2r->pop();
                // int block_id;
                // memcpy(&block_id, block.data, sizeof(int));
                // block = t2r->pop();
                // rbm->write_block(block_id, block);
            } else if (flag == FILE_SIZE) {
                size_t file_size = rbm->get_file_size();
                Block block;
                memcpy(block.data, &file_size, sizeof(size_t));
                r2t->push(block);
            } else if (flag == FILE_CLOSE) {
                break;
            } else {
                std::cerr << "Unknown flag: " << (int)flag << std::endl;
                exit(1);
            }
        }
    }

    ~REEShmManager() {
        munmap(ptr, total_size);
        close(shm_fd);
        shm_unlink("btree_shm");
        delete rbm;
    }

private:
    size_t total_size = 2 * sizeof(BlockBuffer) + sizeof(FlagBuffer) + sizeof(BlockIDBuffer);
    int shm_fd;
    void* ptr;
    BlockBuffer* t2r;
    BlockBuffer* r2t;
    FlagBuffer* flag_buffer;
    BlockIDBuffer* block_id_buffer;
    REEBufferManager *rbm;
};

#endif //REE_SHM_MANAGER_H