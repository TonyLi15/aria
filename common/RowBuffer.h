//
// Created by Haowen Li on 2024/06/20
//

#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

#define MAX_SLOTS 4

namespace aria {

/*
 * RowBuffer -- overview --
 * Consists of {count} and {slots} for pending versions
 * {count} currently implemented in integer type, and it cannot exceed the
 * maximum slots 
 * {slots} currently implemented in VersionList type
 * RowBuffer does not need to be sorted when applying batch append
 */

template <class ValueType>
class RowBuffer {
 public:
  using VersionTupleType = std::tuple<uint64_t, ValueType>;
  using VersionList = std::list<VersionTupleType>;
  RowBuffer() : count(0) {}

  // add pending versions to buffer's slot
  // put empty holder first and set the txid/version
  typename VersionList::iterator add_version(uint64_t version) {
    // stop if counter exceeds the maximum
    if (count >= MAX_SLOTS) return slots.end();
    // put the empty {txid, value} inside slots first
    slots.emplace_front();
    // set corresponding version/txid
    std::get<0>(slots.front()) = version;
    // increment the counter correspondingly
    ++count;
    // check if slot size and counter matches
    assert(slots.size() == count && "Slots size should match count after adding");
    return slots.begin();
  }

  // get the VersionList {slots} -> for performing batch append
  VersionList& get_pending_version_ids() {
    // check if slot size and counter matches
    assert(slots.size() == count && "Slots size should match count before getting");
    return slots;
  }

  // clear buffer's slots
  void clear() {
    // reset the counter
    count = 0;
    // clear all pending version holders inside {slots}
    slots.clear();
    // make sure {slots} is empty after performing clear()
    assert(slots.empty() && "Slots should be empty after clearing");
  }

  // get the value of counter
  int get_count() {
    // make sure the counter is within valid range
    assert(count >= 0 && count <= MAX_SLOTS &&
           "Count should be within valid range when getting count");
    return count;
  }

 private:
  int count;  // number of pending versions
  VersionList slots;  // The list of versions
};

}  // namespace aria