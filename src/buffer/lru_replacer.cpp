//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    this->num_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    std::scoped_lock<std::mutex> lru_lock(lru_mutex);
    if (cache.empty()) {
        return false;
    }

    *frame_id = cache.back();
    auto it = --cache.end();
    frame_id_map.erase(*frame_id);
    cache.erase(it);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lru_lock(lru_mutex);
    auto it = frame_id_map.find(frame_id);
    if (it != frame_id_map.end()) {
        cache.erase(it->second);
        frame_id_map.erase(it);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lru_lock(lru_mutex);
    auto it = frame_id_map.find(frame_id);
    if (it != frame_id_map.end()) {
        return;
    }

    if (num_pages <= frame_id_map.size()) {
        frame_id_map.erase(cache.back());
        cache.erase(--cache.end());
    }

    cache.push_front(frame_id);
    frame_id_map[frame_id] = cache.begin();
}

size_t LRUReplacer::Size() { return cache.size(); }

}  // namespace bustub
