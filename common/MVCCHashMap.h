//
// Created by Yi Lu on 2019-09-02.
// Modified by Haowen Li on 2024-06-20
//

#pragma once

#include <glog/logging.h>

#include <atomic>
#include <list>
#include <unordered_map>

#include "Row.h"
#include "SpinLock.h"

namespace aria {

/*
 *  MVCC Hash Map -- overview --
 *
 *  KeyType -> Row {VersionList, Ptr to buffer}
 *  VersionList -> std::list<std::tuple<uint64_t, ValueType>>,
 *  uint64_t: version, ValueType: value
 *
 *  By default, the first node is a sentinel node, then comes the newest version
 * (the largest value). The upper application (e.g., worker thread) is
 * responsible for data vacuum. Given a vacuum_version, all versions less than
 * or equal to vacuum_version will be garbage collected.
 */

template <std::size_t N, class KeyType, class ValueType>
class MVCCHashMap {
 public:
  // using VersionTupleType = std::tuple<uint64_t, ValueType>;
  // using MappedValueType = std::list<VersionTupleType>;
  // using HashMapType = std::unordered_map<KeyType, MappedValueType>;

  using MappedValueType = Row<ValueType>;
  using VersionTupleType = typename MappedValueType::VersionTupleType;
  using VersionList = typename MappedValueType::VersionList;
  using HashMapType = std::unordered_map<KeyType, MappedValueType>;

  using HasherType = typename HashMapType::hasher;

  // if a particular key exists.
  bool contains_key(const KeyType &key) {
    return apply(
        [&key, this](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }

          // check if the list is empty
          auto &row = it->second;
          auto &l = row.versions;
          return !l.empty();
        },
        bucket_number(key));
  }

  // if a particular key with a specific version exists.
  bool contains_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version, this](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }

          auto &row = it->second;
          auto &l = row.versions;
          for (VersionTupleType &vt : l) {
            if (get_version(vt) == version) {
              return true;
            }
          }
          return false;
        },
        bucket_number(key));
  }

  // remove a particular key.
  bool remove_key(const KeyType &key) {
    return apply(
        [&key, this](HashMapType &map) {
          auto it = map.find(key);

          if (it == map.end()) {
            return false;
          }
          map.erase(it);
          return true;
        },
        bucket_number(key));
  }

  // remove a particular key with a specific version.
  bool remove_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version, this](HashMapType &map) {
          auto it = map.find(key);
          if (it == map.end()) {
            return false;
          }
          auto &row = it->second;
          auto &l = row.versions;

          for (auto lit = l.begin(); lit != l.end(); lit++) {
            if (get_version(*lit) == version) {
              l.erase(lit);
              return true;
            }
          }
          return false;
        },
        bucket_number(key));
  }

  // batch append
  void batch_append(Row<ValueType> &row){
    auto &array = row.versions;

    row.lock.lock();
    // append all <txid, empty_value> inside RowBuffer to the global array
    array.splice(array.begin(), row.buffer->get_pending_version_ids());
    // clear buffer
    row.clear_buffer();
    row.lock.unlock();
  }

  // insert a key with a specific version placeholder and return the reference
  ValueType &insert_key_version_holder(const KeyType &key, uint64_t txid) {
    return apply_ref(
        [&key, txid, this](HashMapType &map) -> ValueType & {
          // lock is already acquired to map
          auto &row = map[key];
          auto &array = row.versions;

          // Get the lock of the row when try to insert key_version holder
          row.lock.lock();
          
          // Case1: the current row is uncontended: append to global version array
          if (!row.lock.try_lock()) {           // successfully acquired the lock of row
            array.emplace_front();              // create and insert a placeholder :<empty txid, empty value> to the global version array
            std::get<0>(array.front()) = txid;  // set txid
            row.lock.unlock(); 
            return std::get<1>(array.front());  // return empty value
          }

          // Case2: the current row may be contended

          // Case2-A: if buffer is allocated
          if (row.buffer) {
            // add pending versions to buffer first
            auto it = row.buffer->add_version(txid);

            // trigger batch append when buffer is full
            if (row.buffer->get_count() >= MAX_SLOTS) {
              batch_append(row);
            }
            // return the empty value that the current txid created
            return std::get<1>(*it);

            
          } else {
            /*
            Case2-B: if buffer is not allocated
            */

            // allocate buffer by using row.allocate_buffer()
            row.allocate_buffer();
            
            // add pending versions to buffer first
            auto it = row.buffer->add_version(txid);

            // trigger batch append when buffer is full
            if (row.buffer->get_count() >= MAX_SLOTS) {
              batch_append(row);
            }
            // return the empty value that the current txid created
            return std::get<1>(*it);
          }
        },
        bucket_number(key));
    ;
  }

  // return the number of versions of a particular key
  std::size_t version_count(const KeyType &key) {
    return apply(
        [&key, this](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          } else {
            auto &row = it->second;
            auto &l = row.versions;
            return l.size();
          }
        },
        bucket_number(key));
  }

  // return the value of a particular key and a specific version
  // nullptr if not exists.
  ValueType *get_key_version(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version, this](HashMapType &map) -> ValueType * {
          auto it = map.find(key);
          if (it == map.end()) {
            return nullptr;
          }
          auto &row = it->second;
          auto &l = row.versions;
          for (VersionTupleType &vt : l) {
            if (get_version(vt) == version) {
              return &get_value(vt);
            }
          }
          return nullptr;
        },
        bucket_number(key));
  }
  // return the value of a particular key and the version older than the
  // specific version nullptr if not exists.
  ValueType *get_key_version_prev(const KeyType &key, uint64_t version) {
    return apply(
        [&key, version, this](HashMapType &map) -> ValueType * {
          auto it = map.find(key);
          if (it == map.end()) {
            return nullptr;
          }
          auto &row = it->second;
          auto &l = row.versions;
          for (VersionTupleType &vt : l) {
            if (get_version(vt) < version) {
              return &get_value(vt);
            }
          }
          return nullptr;
        },
        bucket_number(key));
  }

  // remove all versions less than or equal to vacuum_version
  std::size_t vacuum_key_versions(const KeyType &key, uint64_t vacuum_version) {
    return apply(
        [&key, vacuum_version, this](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          }

          std::size_t size = 0;
          auto &row = it->second;
          auto &l = row.versions;
          auto lit = l.end();

          while (lit != l.begin()) {
            lit--;
            if (get_version(*lit) <= vacuum_version) {
              lit = l.erase(lit);
              size++;
            } else {
              break;
            }
          }
          return size;
        },
        bucket_number(key));
  }

  // remove all versions except the latest one
  std::size_t vacuum_key_keep_latest(const KeyType &key) {
    return apply(
        [&key, this](HashMapType &map) -> std::size_t {
          auto it = map.find(key);
          if (it == map.end()) {
            return 0;
          }

          std::size_t size = 0;
          auto &row = it->second;
          auto &l = row.versions;
          auto lit = l.begin();
          if (lit == l.end()) {
            return 0;
          }

          lit++;
          while (lit != l.end()) {
            lit = l.erase(lit);
            size++;
          }
          return size;
        },
        bucket_number(key));
  }

  // Extend MVCCHashMap.h by Tony: add function to get All kv pairs for print
  // out in testing and debugging
  using KeyValuePairType = std::pair<KeyType, MappedValueType>;
  std::vector<KeyValuePairType> getAllKeyValuePairs() const {
    std::vector<KeyValuePairType> result;
    for (const auto &bucketMap : maps) {
      for (const auto &kvPair : bucketMap) {
        result.emplace_back(kvPair);
      }
    }
    return result;
  }

 private:
  static uint64_t get_version(std::tuple<uint64_t, ValueType> &t) {
    return std::get<0>(t);
  }

  static ValueType &get_value(std::tuple<uint64_t, ValueType> &t) {
    return std::get<1>(t);
  }

 private:
  auto bucket_number(const KeyType &key) { return hasher(key) % N; }

  template <class ApplyFunc>
  auto &apply_ref(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto &result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

  template <class ApplyFunc>
  auto apply(ApplyFunc applyFunc, std::size_t i) {
    DCHECK(i < N) << "index " << i << " is greater than " << N;
    locks[i].lock();
    auto result = applyFunc(maps[i]);
    locks[i].unlock();
    return result;
  }

 private:
  HasherType hasher;
  HashMapType maps[N];
  SpinLock locks[N];
};
}  // namespace aria