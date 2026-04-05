#pragma once
#include <fstream>
#include <iostream>
#include <vector>

template <typename K>
uint64_t load_data(const std::string& file, std::vector<K>& data) {
    std::ifstream data_file(file, std::ios::binary);
    if (!data_file.is_open()) {
      std::cerr << "unable to open " << file << std::endl;
      exit(1);
    }
    // Read size.
    uint64_t size;
    data_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    data.resize(size);

    // Read values.
    data_file.read(reinterpret_cast<char*>(data.data()), size * sizeof(K));
    data_file.close();
    return size;
}

enum OpsType { OP_READ, OP_UPDATE, OP_SCAN, OP_INSERT };