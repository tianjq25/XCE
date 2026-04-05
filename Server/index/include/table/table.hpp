#include <cstdint>
#include <string>
#include <fstream>
#include <iostream>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <variant>
#include <openssl/evp.h>
#include <openssl/err.h>
#include "table_meta.hpp"
#include "json.hpp"

using json = nlohmann::json;


class AESGCMCipher {
public:
    AESGCMCipher() {
        std::ifstream key_file("key.bin", std::ios::binary);
        if (!key_file.is_open()) {
            std::cerr << "unable to open key.bin" << std::endl;
            exit(1);
        }
        key_file.read(reinterpret_cast<char*>(key), 32);
        key_file.close();


        // print key to check, in hex
        std::cout << "key: ";
        for (int i = 0; i < 32; i++) {
            std::cout << std::hex << (int)key[i] << " ";
        }
        std::cout << std::dec << std::endl;
    }

    void handleErrors() {
        ERR_print_errors_fp(stderr);
        abort();
    }
    
    uint64_t decrypt_int64(const unsigned char *ciphertext, uint64_t row_id) {
        EVP_CIPHER_CTX *ctx;
        int len;
        int plaintext_len;
        uint64_t result = 0;
        unsigned char plaintext[8];
        
        // 创建IV (12字节)
        unsigned char iv[12];
        memcpy(iv, &row_id, sizeof(uint64_t));
        memset(iv + sizeof(uint64_t), 0, 4);
        
        // 创建并初始化解密上下文
        if(!(ctx = EVP_CIPHER_CTX_new())) handleErrors();
        // 初始化解密操作
        if(1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), NULL, key, iv))
            handleErrors();
        // 提供需要解密的数据
        if(1 != EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, 8))
            handleErrors();
        plaintext_len = len;
        
        // 设置预期的认证标签
        if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, (void*)(ciphertext + 8)))
            handleErrors();
        // 完成解密
        if(1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len))
            handleErrors();
        plaintext_len += len;
        // 释放上下文
        EVP_CIPHER_CTX_free(ctx);

        // 将解密后的字节转换为uint64_t
        memcpy(&result, plaintext, sizeof(uint64_t));
        return result;
    }

private:
    unsigned char key[32]; // read from key.bin
} cipher;

std::string binary_to_hex(const unsigned char* data, size_t len) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        ss << std::setw(2) << static_cast<int>(data[i]);
    }
    return ss.str();
}

// Save string for print
std::string safe_string(const char* data, size_t max_len) {
    size_t len = 0;
    while (len < max_len && data[len] != '\0') len++;
    return std::string(data, len);
}

class Table {
public:
    Table(const std::string& table_name)  {
        this->table_name = table_name;
        data_file.open("tables/" + table_name + ".table", std::ios::binary);
        if (!data_file.is_open()) {
            std::cerr << "unable to open " << table_name << ".table" << std::endl;
            exit(1);
        }
        meta = table_meta[table_name];
        line_size = 0;
        for (int i = 0; i < meta.type_size.size(); i++) {
            line_size += meta.type_size[i];
        }
        // read lines
        data_file.read(reinterpret_cast<char*>(&lines), sizeof(uint64_t));
    }

    ~Table() {
        data_file.close();
    }

    using RowType = std::variant<Employees, Nation, Supplier, Customer, Orders, Lineitem>;
    RowType getRowTypeForTable() const {
        if (table_name == "employees") {
            return Employees{};
        } else if (table_name == "nation") {
            return Nation{};
        } else if (table_name == "supplier") {
            return Supplier{};
        } else if (table_name == "customer") {
            return Customer{};
        } else if (table_name == "orders") {
            return Orders{};
        } else if (table_name == "lineitem") {
            return Lineitem{};
        }
        throw std::runtime_error("Unknown table: " + table_name);
    }

    class RowIterator {
    public:
        RowIterator(uint64_t row_id, std::ifstream& file, uint64_t offset, uint64_t line_size, std::string table_name)
            : row_id_(row_id), file_(file), offset_(offset), line_size_(line_size), table_name_(table_name) {}
        
        RowIterator& operator=(const RowIterator& other) {
            if (this != &other) {
                offset_ = other.offset_;
                line_size_ = other.line_size_;
                table_name_ = other.table_name_;
            }
            return *this;
        }

        RowIterator& operator++() {
            offset_ += line_size_;
            return *this;
        }

        RowIterator operator+(int n) const {
            RowIterator temp = *this;
            temp.offset_ += n * line_size_;
            return temp;
        }



        RowType operator*() {
            file_.seekg(offset_);
            if (table_name_ == "employees") {
                Employees row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Employees));
                return row;
            } else if (table_name_ == "nation") {
                Nation row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Nation));
                return row;
            } else if (table_name_ == "supplier") {
                Supplier row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Supplier));
                return row;
            } else if (table_name_ == "customer") {
                Customer row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Customer));
                return row;
            } else if (table_name_ == "orders") {
                Orders row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Orders));
                return row;
            } else if (table_name_ == "lineitem") {
                Lineitem row;
                file_.read(reinterpret_cast<char*>(&row), sizeof(Lineitem));
                return row;
            }
            
            throw std::runtime_error("Unknown table: " + table_name_);
        }

        json get_row_array() {
            json row = json::array();
            file_.seekg(offset_);
            for (int i = 0; i < table_meta[table_name_].type_size.size(); i++) {
                if (table_meta[table_name_].columns_types[i] == 0) {
                    char buffer[table_meta[table_name_].type_size[i]];
                    file_.read(buffer, table_meta[table_name_].type_size[i]);
                    row.push_back(safe_string(buffer, table_meta[table_name_].type_size[i]));
                } else if (table_meta[table_name_].columns_types[i] == 1) {
                    uint64_t value;
                    file_.read(reinterpret_cast<char*>(&value), sizeof(uint64_t));
                    row.push_back(value);
                } else if (table_meta[table_name_].columns_types[i] == 2) {
                    double value;
                    file_.read(reinterpret_cast<char*>(&value), sizeof(double));
                    row.push_back(value);
                } else {
                    // encrypted bigint
                    unsigned char ciphertext[24];
                    file_.read(reinterpret_cast<char*>(ciphertext), 24);
                    row.push_back(binary_to_hex(ciphertext, 24));
                }
            }
            return row;
        }

        uint64_t get_row_id() {
            return row_id_;
        }

        bool operator!=(const RowIterator& other) const {
            return offset_ != other.offset_;
        }
    private:
        uint64_t row_id_;
        std::ifstream& file_;
        uint64_t offset_;
        uint64_t line_size_;
        std::string table_name_;
    };

    RowIterator row_begin() {
        return RowIterator(0, data_file, 8, line_size, table_name);
    }

    RowIterator row_end() {
        return RowIterator(lines, data_file, 8 + line_size * lines, line_size, table_name);
    }

    class ColumnIterator {
    public:
        ColumnIterator(std::ifstream& file, uint64_t offset, uint64_t row_id, uint8_t type, uint64_t type_size, uint64_t line_size)
            : file_(file), offset_(offset), row_id_(row_id), type_(type), type_size_(type_size), line_size_(line_size) {
                if (type_ == 0) { // varchar
                    buffer = new char[type_size_];
                }
            }
        
        ColumnIterator& operator=(const ColumnIterator& other) {
            if (this != &other) {
                offset_ = other.offset_;
                type_ = other.type_;
                type_size_ = other.type_size_;
            }
            return *this;
        }

        ColumnIterator& operator++() {
            // 行存
            row_id_++;
            offset_ += line_size_;
            return *this;
        }

        std::variant<std::string, uint64_t, double> operator*() {
            file_.seekg(offset_);
            if (type_ == 0) { // varchar
                file_.read(buffer, type_size_);
                return std::string(buffer);
            } else if (type_ == 1) { // bigint
                uint64_t value;
                file_.read(reinterpret_cast<char*>(&value), sizeof(uint64_t));
                return value;
            } else if (type_ == 2) { // double
                // type_ == 2, double
                double value;
                file_.read(reinterpret_cast<char*>(&value), sizeof(double));
                return value;
            } else {
                // 3, encrypted bigint
                unsigned char ciphertext[24];
                file_.read(reinterpret_cast<char*>(ciphertext), type_size_);
                uint64_t value = cipher.decrypt_int64(ciphertext, row_id_);
                return value;
            }
        }

        bool operator!=(const ColumnIterator& other) const {
            return offset_ != other.offset_;
        }

        ~ColumnIterator() {
            if (type_ == 0) {
                delete[] buffer;
            }
        }

    private:
        std::ifstream& file_;
        uint8_t type_;
        uint64_t offset_;
        uint64_t row_id_;
        uint64_t type_size_;
        uint64_t line_size_;
        char *buffer;
    };
    
    ColumnIterator column_begin(const std::string& column_name) {
        // get column id
        uint64_t offset = 8; // skip lines(uint64_t)
        for (int i = 0; i < meta.columns_names.size(); i++) {
            if (meta.columns_names[i] == column_name) {
                return ColumnIterator(data_file, offset, 0, meta.columns_types[i], meta.type_size[i], line_size);
            }
            offset += meta.type_size[i];
        }
        throw std::runtime_error("Column not found: " + column_name);
    }

    ColumnIterator column_end(const std::string& column_name) {
        uint64_t offset = 8; // skip lines(uint64_t)
        for (int i = 0; i < meta.columns_names.size(); i++) {
            if (meta.columns_names[i] == column_name) {
                return ColumnIterator(data_file, offset + line_size * lines, lines, meta.columns_types[i], meta.type_size[i], line_size);
            }
            offset += meta.type_size[i];
        }
        throw std::runtime_error("Column not found: " + column_name);
    }

    std::ifstream data_file;
    std::string table_name;
    TableMeta meta;
    uint64_t line_size;
    uint64_t lines;
};