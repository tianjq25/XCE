#ifndef TEE_BUFFER_MANAGER_H
#define TEE_BUFFER_MANAGER_H
#include <cstdint>
#include <iostream>
#include <sys/types.h>
#include <unordered_map>
#include <list>
#include "lru.h"
#include "./ree_buffer_manager.h"
#include "../utility.h"
#include <chrono>
#include <openssl/evp.h>
#include <openssl/rand.h>

class Encryptor {
public:
    Encryptor() {
        RAND_bytes(key, sizeof(key));
        RAND_bytes(iv, sizeof(iv));
        
        ctx = EVP_CIPHER_CTX_new();
        if (!EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, key, iv)) {
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

        if (!EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, key, iv)) {
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
    EVP_CIPHER_CTX *ctx;
    unsigned char key[32];
    unsigned char iv[16];
};

class TEEBufferManager {
public:
    TEEBufferManager(char *fn, bool first = false, bool bulk_load = false) {
        enc = new Encryptor();
        rbm = new REEBufferManager(fn, first, bulk_load);
        pages = new Page[TEEBufferPoolSize];
        for (size_t i = 0; i < TEEBufferPoolSize; ++i) {
            free_list.push_back(&pages[i]);
        }
    }

    Block get_block(int block_id) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            return page_table[block_id]->block;
        } else {
            bool save_tee = rbm->get_block(block_id, temp_save_buffer.data);
            bool is_inner = temp_save_buffer.data[0] == InnerNodeType && block_id;
            if (is_inner && !save_tee) {
                rbm->save_buffer(block_id, temp_save_buffer);
            }
            if (save_tee) {
                if (!free_list.empty()) {
                    Page *page = free_list.front();
                    free_list.pop_front();
                    page_table[block_id] = page;
                    lru.put(block_id);

                    #ifdef ENC
                    #ifdef IO_PROFILING
                    auto start = std::chrono::high_resolution_clock::now();
                    #endif
                    int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(page->block.data));
                    if (decrypted_len == -1) {
                        throw std::runtime_error("Failed to decrypt block.");
                    }
                    #ifdef IO_PROFILING
                    auto end = std::chrono::high_resolution_clock::now();
                    io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    #endif
                    #else
                    memcpy(page->block.data, temp_save_buffer.data, BlockSize);
                    #endif
                    return page->block;
                    
                } else {
                    int evict_block_id;
                    lru.evict(evict_block_id);
                    Page *page = page_table[evict_block_id];
                    if (page->is_dirty) {
                        #ifdef ENC
                        // encrypt the block before writing
                        #ifdef IO_PROFILING
                        auto start = std::chrono::high_resolution_clock::now();
                        #endif
                        int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(page->block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
                        if (ciphertext_len == -1) {
                            throw std::runtime_error("Failed to encrypt block.");
                        }
                        #ifdef IO_PROFILING
                        auto end = std::chrono::high_resolution_clock::now();
                        io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                        #endif
                        rbm->write_block(evict_block_id, enc_block_buffer, true);
                        #else
                        rbm->write_block(evict_block_id, page->block, true);
                        #endif
                    }
                    page_table.erase(evict_block_id);
                    page_table[block_id] = page;
                    lru.put(block_id);


                    #ifdef ENC
                    #ifdef IO_PROFILING
                    auto start = std::chrono::high_resolution_clock::now();
                    #endif
                    int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(page->block.data));
                    if (decrypted_len == -1) {
                        throw std::runtime_error("Failed to decrypt block.");
                    }
                    #ifdef IO_PROFILING
                    auto end = std::chrono::high_resolution_clock::now();
                    io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    #endif
                    #else
                    memcpy(page->block.data, temp_save_buffer.data, BlockSize);
                    #endif
                    return page->block;
                }
            } else {
                #ifdef ENC
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
                if (decrypted_len == -1) {
                    throw std::runtime_error("Failed to decrypt block.");
                }
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
                return enc_block_buffer;
                #else
                return temp_save_buffer;
                #endif
            }

            // original code
            // if (!free_list.empty()) {
            //     Page *page = free_list.front();
            //     free_list.pop_front();
            //     page_table[block_id] = page;
            //     lru.put(block_id);

            //     #ifdef ENC
            //     auto start = std::chrono::high_resolution_clock::now();
            //     rbm->get_block(block_id, enc_block_buffer.data);
            //     int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(enc_block_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(page->block.data));
            //     if (decrypted_len == -1) {
            //         throw std::runtime_error("Failed to decrypt block.");
            //     }
            //     #else
            //     rbm->get_block(block_id, page->block.data);
            //     #endif
            //     return page->block;
                
            // } else {
            //     int evict_block_id;
            //     lru.evict(evict_block_id);
            //     Page *page = page_table[evict_block_id];
            //     if (page->is_dirty) {
            //         #ifdef ENC
            //         // encrypt the block before writing
            //         int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(page->block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
            //         if (ciphertext_len == -1) {
            //             throw std::runtime_error("Failed to encrypt block.");
            //         }

            //         rbm->write_block(evict_block_id, enc_block_buffer);
            //         #else
            //         rbm->write_block(evict_block_id, page->block);
            //         #endif
            //     }
            //     page_table.erase(evict_block_id);
            //     page_table[block_id] = page;
            //     lru.put(block_id);


            //     #ifdef ENC
            //     rbm->get_block(block_id, enc_block_buffer.data);
            //     int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(enc_block_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(page->block.data));
            //     if (decrypted_len == -1) {
            //         throw std::runtime_error("Failed to decrypt block.");
            //     }
            //     #else
            //     rbm->get_block(block_id, page->block.data);
            //     #endif
            //     return page->block;
            // }
        }
    }

    void get_block(int block_id, char *data) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            memcpy(data, page_table[block_id]->block.data, BlockSize);
        } else {
            bool save_tee = rbm->get_block(block_id, temp_save_buffer.data);
            bool is_inner = temp_save_buffer.data[0] == InnerNodeType && block_id;
            if (is_inner && !save_tee) {
                rbm->save_buffer(block_id, temp_save_buffer);
            }
            if (save_tee) {
                if (!free_list.empty()) {
                    Page *page = free_list.front();
                    free_list.pop_front();
                    page_table[block_id] = page;
                    lru.put(block_id);

                    #ifdef ENC
                    #ifdef IO_PROFILING
                    auto start = std::chrono::high_resolution_clock::now();
                    #endif
                    int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(data));
                    if (decrypted_len == -1) {
                        throw std::runtime_error("Failed to decrypt block.");
                    }
                    #ifdef IO_PROFILING
                    auto end = std::chrono::high_resolution_clock::now();
                    io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    #endif
                    memcpy(page->block.data, data, BlockSize);
                    #else
                    memcpy(data, temp_save_buffer.data, BlockSize);
                    memcpy(page->block.data, data, BlockSize);
                    #endif
                } else {
                    int evict_block_id;
                    lru.evict(evict_block_id);
                    Page *page = page_table[evict_block_id];
                    if (page->is_dirty) {
                        #ifdef ENC
                        // encrypt the block before writing
                        #ifdef IO_PROFILING
                        auto start = std::chrono::high_resolution_clock::now();
                        #endif
                        int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(page->block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
                        if (ciphertext_len == -1) {
                            throw std::runtime_error("Failed to encrypt block.");
                        }
                        #ifdef IO_PROFILING
                        auto end = std::chrono::high_resolution_clock::now();
                        io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                        #endif

                        rbm->write_block(evict_block_id, enc_block_buffer, true);
                        #else
                        rbm->write_block(evict_block_id, page->block, true);
                        #endif
                    }
                    page_table.erase(evict_block_id);
                    page_table[block_id] = page;
                    lru.put(block_id);

                    #ifdef ENC
                    #ifdef IO_PROFILING
                    auto start = std::chrono::high_resolution_clock::now();
                    #endif
                    int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(data));
                    if (decrypted_len == -1) {
                        throw std::runtime_error("Failed to decrypt block.");
                    }
                    #ifdef IO_PROFILING
                    auto end = std::chrono::high_resolution_clock::now();
                    io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    #endif
                    memcpy(page->block.data, data, BlockSize);
                    #else
                    memcpy(data, temp_save_buffer.data, BlockSize);
                    memcpy(page->block.data, data, BlockSize);
                    #endif
                }
            } else {
                #ifdef ENC
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(temp_save_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(data));
                if (decrypted_len == -1) {
                    throw std::runtime_error("Failed to decrypt block.");
                }
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                io_stats.dec_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
                #else
                memcpy(data, temp_save_buffer.data, BlockSize);
                #endif
            }

            // original code
            // if (!free_list.empty()) {
            //     Page *page = free_list.front();
            //     free_list.pop_front();
            //     page_table[block_id] = page;
            //     lru.put(block_id);

            //     #ifdef ENC
            //     rbm->get_block(block_id, enc_block_buffer.data);
            //     int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(enc_block_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(data));
            //     if (decrypted_len == -1) {
            //         throw std::runtime_error("Failed to decrypt block.");
            //     }
            //     memcpy(page->block.data, data, BlockSize);
            //     #else
            //     rbm->get_block(block_id, data);
            //     memcpy(page->block.data, data, BlockSize);
            //     #endif
            // } else {
            //     int evict_block_id;
            //     lru.evict(evict_block_id);
            //     Page *page = page_table[evict_block_id];
            //     if (page->is_dirty) {
            //         #ifdef ENC
            //         // encrypt the block before writing
            //         int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(page->block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
            //         if (ciphertext_len == -1) {
            //             throw std::runtime_error("Failed to encrypt block.");
            //         }

            //         rbm->write_block(evict_block_id, enc_block_buffer);
            //         #else
            //         rbm->write_block(evict_block_id, page->block);
            //         #endif
            //     }
            //     page_table.erase(evict_block_id);
            //     page_table[block_id] = page;
            //     lru.put(block_id);
            //     #ifdef ENC
            //     rbm->get_block(block_id, enc_block_buffer.data);
            //     int decrypted_len = enc->encrypt(reinterpret_cast<unsigned char*>(enc_block_buffer.data), BlockSize, reinterpret_cast<unsigned char*>(data));
            //     if (decrypted_len == -1) {
            //         throw std::runtime_error("Failed to decrypt block.");
            //     }
            //     memcpy(page->block.data, data, BlockSize);
            //     #else
            //     rbm->get_block(block_id, data);
            //     memcpy(page->block.data, data, BlockSize);
            //     #endif
            // }
        }
    }

    bool write_block(int block_id, Block block) {
        if (page_table.find(block_id) != page_table.end()) {
            lru.put(block_id);
            page_table[block_id]->block = block;
            page_table[block_id]->is_dirty = 1;
            return true;
        } else {
            bool is_inner = block.data[0] == InnerNodeType && block_id;
            if (!is_inner) {
                #ifdef ENC
                #ifdef IO_PROFILING
                auto start = std::chrono::high_resolution_clock::now();
                #endif
                int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
                if (ciphertext_len == -1) {
                    throw std::runtime_error("Failed to encrypt block.");
                }
                #ifdef IO_PROFILING
                auto end = std::chrono::high_resolution_clock::now();
                io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                #endif
                rbm->write_block(block_id, enc_block_buffer);
                #else
                rbm->write_block(block_id, block);
                #endif
                return true;
            }
            if (!free_list.empty()) {
                Page *page = free_list.front();
                free_list.pop_front();
                page_table[block_id] = page;
                lru.put(block_id);
                page->block = block;
                page->is_dirty = 1;
                return true;
            } else {
                int evict_block_id;
                lru.evict(evict_block_id);
                Page *page = page_table[evict_block_id];
                if (page->is_dirty) {
                    #ifdef ENC
                    // encrypt the block before writing
                    #ifdef IO_PROFILING
                    auto start = std::chrono::high_resolution_clock::now();
                    #endif
                    int ciphertext_len = enc->encrypt(reinterpret_cast<const unsigned char*>(page->block.data), BlockSize, reinterpret_cast<unsigned char*>(enc_block_buffer.data));
                    if (ciphertext_len == -1) {
                        throw std::runtime_error("Failed to encrypt block.");
                    }
                    #ifdef IO_PROFILING
                    auto end = std::chrono::high_resolution_clock::now();
                    io_stats.enc_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
                    #endif

                    rbm->write_block(evict_block_id, enc_block_buffer, true);
                    #else
                    rbm->write_block(evict_block_id, page->block, true);
                    #endif
                }
                page_table.erase(evict_block_id);
                page_table[block_id] = page;
                lru.put(block_id);
                page->block = block;
                page->is_dirty = 1;
                return true;
            }
        }
    }

    size_t get_file_size() {
        return rbm->get_file_size();
    }

    ~TEEBufferManager() {
        for (auto &it : page_table) {
            if (it.second->is_dirty) {
                rbm->write_block(it.first, it.second->block);
            }
        }
        delete rbm;
        delete[] pages;
    }

    void reset_IO_time() {
        IO_time = 0;
    }

    uint64_t get_IO_time() {
        return IO_time;
    }

private:
    uint64_t IO_time = 0;
    Block enc_block_buffer;
    Block temp_save_buffer;
    Encryptor *enc;
    REEBufferManager *rbm;
    Page *pages;
    std::unordered_map<int, Page*> page_table;
    std::list<Page *> free_list;
    LRU<int> lru;
};


#endif //TEE_BUFFER_MANAGER_H
