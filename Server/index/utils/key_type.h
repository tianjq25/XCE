#ifndef KEY_TYPE_H
#define KEY_TYPE_H

#include <iostream>
#include <map>
#include <vector>

typedef uint64_t Key;
typedef uint64_t Value;
typedef std::pair<Key, Value> Record;
typedef std::vector<Key> KeyVec;
typedef std::vector<std::pair<Key, Value>> DataVec;

#define OPS_SUFFIX "_OPS"
#define INIT_SUFFIX "_INIT"
#define OPS_KEY_SUFFIX "_OPS_KEY"
#define RANGE_LEN_SUFFIX "_RANGE_LEN"


#endif  // !KEY_TYPE_H