/**
 * LRU implementation
 */
#include <include/common/logger.h>
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  auto map_iter = map_.find(value);
  if (map_iter != map_.end()) {
    // The value already exists in the map_
    auto queue_iter = map_iter->second;
    queue_.erase(queue_iter);
    map_.erase(map_iter);
  }

  queue_.push_back(value);
  auto queue_iter = --queue_.end();
  map_.insert({value, queue_iter});
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  bool ret = false;
  if (0 == map_.size()) {
    ret = false;
  } else {
    value = queue_.front();
    queue_.pop_front();
    map_.erase(value);
    ret = true;
  }
  return ret;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  bool ret;
  auto map_iter = map_.find(value);
  if (map_iter == map_.end()) {
    ret = false;
  } else {
    auto queue_iter = map_iter->second;
    queue_.erase(queue_iter);
    map_.erase(map_iter);
    ret = true;
  }
  return ret;
}

template <typename T> size_t LRUReplacer<T>::Size() { return map_.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
