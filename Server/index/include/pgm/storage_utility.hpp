#ifndef STORAGE_UTILITY_H
#define STORAGE_UTILITY_H
#include "pgm/defs.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdexcept>

#include <cstring>

#include <openssl/evp.h>
#include <openssl/rand.h>


namespace pgm_disk {
    class Encryptor {
    public:
        Encryptor() {
            RAND_bytes(key, sizeof(key));
            RAND_bytes(iv, sizeof(iv));
            
            ctx = EVP_CIPHER_CTX_new();
            if (!EVP_EncryptInit_ex(ctx, cipher, nullptr, key, iv)) {
                EVP_CIPHER_CTX_free(ctx);
                throw std::runtime_error("Failed to initialize encryption context.");
            }
        }

        ~Encryptor() {
            EVP_CIPHER_CTX_free(ctx);
        }

        int encrypt(const unsigned char *plaintext, int plaintext_len, unsigned char *ciphertext) {
            int len;
            int ciphertext_len;

            if (!EVP_EncryptInit_ex(ctx, cipher, nullptr, key, iv)) {
                EVP_CIPHER_CTX_free(ctx);
                throw std::runtime_error("Failed to initialize encryption context.");
            }

            if(!EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, plaintext_len))
                return -1;
            ciphertext_len = len;

            if(!EVP_EncryptFinal_ex(ctx, ciphertext + len, &len))
                return -1;
            ciphertext_len += len;

            return ciphertext_len;
        }

    private:
        const EVP_CIPHER *cipher = EVP_aes_256_ctr();
        EVP_CIPHER_CTX *ctx;
        unsigned char key[32];
        unsigned char iv[16];
    };

    const long BLOCK_SIZE = 8192 / 2;
    const int32_t ItemOnDiskSize = 16; // SOS: hard code!!!
    
    class StorageManager {
    public:
        constexpr static uint8_t ceil_log2(size_t n) {
            return n <= 1 ? 0 : sizeof(long long) * 8 - __builtin_clzll(n - 1);
        }
        uint8_t ceil_log_base(size_t n) {
            return (ceil_log2(n) + ceil_log2(BASE) - 1) / ceil_log2(BASE);
        }
        size_t max_size(uint8_t level) {
            // 返回的是Item数量，不是内存占用
            return size_t(1) << (level * ceil_log2(BASE));
        }

        inline size_t ceil2block(uint8_t level) {
            return ((max_size(level) * ItemOnDiskSize + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        }

        StorageManager() {
            // level_capacity[0] = ceil2block(0);
            level_capacity[0] = 0;
            for (size_t i = 0; i <= MIN_LEVEL; i++) {
                level_capacity[0] += max_size(i);
            }
            level_capacity[0] = (level_capacity[0] * ItemOnDiskSize + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
            for (size_t i = MIN_LEVEL + 1; i < MIN_DISK_LEVEL; i++) {
                level_capacity[i - MIN_LEVEL] = level_capacity[i - MIN_LEVEL - 1] + ceil2block(i);
            }
            #ifdef DETAIL
            for (size_t i = MIN_LEVEL; i < MIN_DISK_LEVEL; i++) {
                std::cout << "level_capacity[" << i << " - MIN_LEVEL] = " << level_capacity[i - MIN_LEVEL] << std::endl;
            }
            #endif

            enc = new Encryptor();
        }

        void read_multi_blocks(int fd, void *data, int block_id, int read_blocks) {
            if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
                throw std::runtime_error("lseek file error in _read_block");
            }
            int ret = read(fd, data, read_blocks * BLOCK_SIZE);
            if (ret == -1) {
                throw std::runtime_error("read error in DirectIORead");
            }
        }

        void read_block(int fd, void *data, int block_id) {
            if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
                throw std::runtime_error("lseek file error in _read_block");
            }
            int ret = read(fd, data, BLOCK_SIZE);
            if (ret == -1) {
                throw std::runtime_error("read error in DirectIORead");
            }
        }
        void write_block(int fd, void *data, int block_id) {
            if (lseek(fd, block_id * BLOCK_SIZE, SEEK_SET) == -1) {
                throw std::runtime_error("lseek file error in _write_block");
            }
            int ret = write(fd, data, BLOCK_SIZE);
            if (ret == -1) {
                throw std::runtime_error("write error in DirectIOWrite");
            }
        }

        void read_block_and_cache(uint8_t level, int fd, void *data, int block_id, char *shm_ptr, int save_left, int save_right, void *left_block, void *right_block) {
            int read_blocks = save_left + save_right + 1;
            int start_block_id = block_id - save_left;
            if (level < MIN_DISK_LEVEL) {
                size_t level_offset = level > MIN_LEVEL ? level_capacity[level - MIN_LEVEL - 1] : 0;
                memcpy(enc_buffer, shm_ptr + level_offset + start_block_id * BLOCK_SIZE, read_blocks * BLOCK_SIZE);
            } else {
                read_multi_blocks(fd, enc_buffer, start_block_id, read_blocks);
            }
            // int decrypt_len = enc->encrypt(enc_buffer, read_blocks * BLOCK_SIZE, predict_cache);
            if (save_left) {
                int decrypt_len = enc->encrypt(enc_buffer, BLOCK_SIZE, (unsigned char*)left_block);
                // memcpy(left_block, predict_cache, BLOCK_SIZE);
                decrypt_len = enc->encrypt(enc_buffer + BLOCK_SIZE, BLOCK_SIZE, (unsigned char*)data);
                // memcpy(data, predict_cache + BLOCK_SIZE, BLOCK_SIZE);
                if (save_right)
                    decrypt_len = enc->encrypt(enc_buffer + 2 * BLOCK_SIZE, BLOCK_SIZE, (unsigned char*)right_block);
                    // memcpy(right_block, predict_cache + 2 * BLOCK_SIZE, BLOCK_SIZE);
            } else {
                int decrypt_len = enc->encrypt(enc_buffer, BLOCK_SIZE, (unsigned char*)data);
                // memcpy(data, predict_cache, BLOCK_SIZE);
                if (save_right)
                    decrypt_len = enc->encrypt(enc_buffer + BLOCK_SIZE, BLOCK_SIZE, (unsigned char*)right_block);
                    // memcpy(right_block, predict_cache + BLOCK_SIZE, BLOCK_SIZE);
            }
        }

        void read_block(uint8_t level, int fd, void *data, int block_id, char *shm_ptr) {
            if (level < MIN_DISK_LEVEL) {
                size_t level_offset = level > MIN_LEVEL ? level_capacity[level - MIN_LEVEL - 1] : 0;
                memcpy(enc_buffer, shm_ptr + level_offset + block_id * BLOCK_SIZE, BLOCK_SIZE);
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int decrypt_len = enc->encrypt(enc_buffer, BLOCK_SIZE, (unsigned char *)data);
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                // io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
            } else {
                #ifdef IO_PROFILING
                io_stats.read_block += 1;
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                read_block(fd, enc_buffer, block_id);
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                io_stats.read_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                start = std::chrono::high_resolution_clock::now();
                #endif
                int decrypt_len = enc->encrypt(enc_buffer, BLOCK_SIZE, (unsigned char *)data);
                #ifdef IO_PROFILING
                end = std::chrono::high_resolution_clock::now();
                io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
            }
        }
        void write_block(uint8_t level, int fd, void *data, int block_id, char *shm_ptr) {
            if (level < MIN_DISK_LEVEL) {
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int encrypt_len = enc->encrypt((unsigned char *)data, BLOCK_SIZE, enc_buffer);
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                // io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
                size_t level_offset = level > MIN_LEVEL ? level_capacity[level - MIN_LEVEL - 1] : 0;
                memcpy(shm_ptr + level_offset + block_id * BLOCK_SIZE, enc_buffer, BLOCK_SIZE);
            } else {
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int encrypt_len = enc->encrypt((unsigned char *)data, BLOCK_SIZE, enc_buffer);
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                io_stats.write_block += 1;
                start = std::chrono::high_resolution_clock::now();
                #endif
                write_block(fd, enc_buffer, block_id);
                #ifdef IO_PROFILING
                end = std::chrono::high_resolution_clock::now();
                io_stats.write_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
            }
        }

        size_t level_capacity[MIN_DISK_LEVEL - MIN_LEVEL];

    private:
        unsigned char enc_buffer[BLOCK_SIZE * 3];
        // unsigned char predict_cache[BLOCK_SIZE * 3];
        Encryptor *enc;
    };
    
}  // namespace pgm_disk

#endif //STORAGE_UTILITY_H
