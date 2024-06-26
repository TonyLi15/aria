//
// Created by Haowen Li on 2024/06/20
//

#pragma once

#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <tuple>

#include "RowBuffer.h"
#include "SpinLock.h"

namespace aria {

/*
 * Row -- overview --
 * ValueType: value
 *
 * Consists of {VersionList} and ptr to {buffer}
 *
 * {VersionList} -> std::list of <VersionTupleType>
 * {VersionTupleType} -> std::tuple<uint64_t, ValueType>
 *
 * ptr to {buffer} is initialized only if the data item is contended
 */

template <class ValueType>
class Row {
 public:
  // {txid, value}, value: record
  using VersionTupleType = std::tuple<uint64_t, ValueType>;
  using VersionList = std::list<VersionTupleType>;

  Row() : buffer(nullptr) {}

  // allocate pointer to RowBuffer
  void allocate_buffer() {
    if (!buffer) buffer = std::make_unique<RowBuffer<ValueType>>();
  }

  // deallocate pointer to RowBuffer
  void clear_buffer() {
    if (buffer) {
      buffer->clear();
      buffer = nullptr;
    }
  }

  VersionList versions;  // The list of versions
  std::unique_ptr<RowBuffer<ValueType>> buffer;  // Buffer for pending versions
  SpinLock lock;                   // lock for key data
};

}  // namespace aria