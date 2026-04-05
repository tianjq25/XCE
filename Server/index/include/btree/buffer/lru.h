#ifndef B_TREE_LRU_H
#define B_TREE_LRU_H

#include <list>
#include <unordered_map>

template <typename K>
class LRU {
public:
    void put(K block_id) {
        // check if block_id is already in the list
        // if it is, move it to the front
        // if it is not, add it to the front

        if (lru_map.find(block_id) != lru_map.end()) {
            lru_list.erase(lru_map[block_id]);
        }
        lru_list.push_front(block_id);
        lru_map[block_id] = lru_list.begin();
    }

    void evict(K &block_id) {
        // remove the last element from the list
        // and return it
        block_id = lru_list.back();
        lru_list.pop_back();
        lru_map.erase(block_id);
    }

private:
    std::list<K> lru_list;
    std::unordered_map<K, typename std::list<K>::iterator> lru_map;
};


#endif //B_TREE_LRU_H
