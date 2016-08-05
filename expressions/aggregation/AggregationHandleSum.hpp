/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#ifndef QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_SUM_HPP_
#define QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_SUM_HPP_

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "expressions/aggregation/AggregationConcreteHandle.hpp"
#include "expressions/aggregation/AggregationHandle.hpp"
#include "storage/HashTableBase.hpp"
#include "storage/FastHashTable.hpp"
#include "threading/SpinMutex.hpp"
#include "types/Type.hpp"
#include "types/TypedValue.hpp"
#include "types/operations/binary_operations/BinaryOperation.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {

class ColumnVector;
class StorageManager;
class ValueAccessor;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief Aggregation state for sum.
 */
class AggregationStateSum : public AggregationState {
 public:
  /**
   * @brief Copy constructor (ignores mutex).
   */
  AggregationStateSum(const AggregationStateSum &orig)
      : sum_(orig.sum_),
        null_(orig.null_),
        sum_offset(orig.sum_offset),
        null_offset(orig.null_offset) {
  }

 private:
  friend class AggregationHandleSum;

  AggregationStateSum()
      : sum_(0), null_(true), sum_offset(0),
        null_offset(reinterpret_cast<uint8_t *>(&null_)-reinterpret_cast<uint8_t *>(&sum_)) {
  }

  AggregationStateSum(TypedValue &&sum, const bool is_null)
      : sum_(std::move(sum)), null_(is_null) {
  }

  size_t getPayloadSize() const {
     size_t p1 = reinterpret_cast<size_t>(&sum_);
     size_t p2 = reinterpret_cast<size_t>(&mutex_);
     return (p2-p1);
  }

  // TODO(shoban): We might want to specialize sum_ to use atomics for int types
  // similar to in AggregationStateCount.
  TypedValue sum_;
  bool null_;
  SpinMutex mutex_;

  int sum_offset, null_offset;
};


/**
 * @brief An aggregationhandle for sum.
 **/
class AggregationHandleSum : public AggregationConcreteHandle {
 public:
  ~AggregationHandleSum() override {
  }

  AggregationState* createInitialState() const override {
    return new AggregationStateSum(blank_state_);
  }

  AggregationStateHashTableBase* createGroupByHashTable(
      const HashTableImplType hash_table_impl,
      const std::vector<const Type*> &group_by_types,
      const std::size_t estimated_num_groups,
      StorageManager *storage_manager) const override;

  inline void iterateUnaryInl(AggregationStateSum *state, const TypedValue &value) const {
    DCHECK(value.isPlausibleInstanceOf(argument_type_.getSignature()));
    if (value.isNull()) return;

    SpinMutexLock lock(state->mutex_);
    state->sum_ = fast_operator_->applyToTypedValues(state->sum_, value);
    state->null_ = false;
  }

  inline void iterateUnaryInlFast(const TypedValue &value, uint8_t *byte_ptr) const {
    DCHECK(value.isPlausibleInstanceOf(argument_type_.getSignature()));
    if (value.isNull()) return;
    TypedValue *sum_ptr = reinterpret_cast<TypedValue *>(byte_ptr + blank_state_.sum_offset);
    bool *null_ptr = reinterpret_cast<bool *>(byte_ptr + blank_state_.null_offset);
    *sum_ptr = fast_operator_->applyToTypedValues(*sum_ptr, value);
    *null_ptr = false;
  }

  inline void iterateInlFast(const std::vector<TypedValue> &arguments, uint8_t *byte_ptr) override {
     if (block_update) return;
     iterateUnaryInlFast(arguments.front(), byte_ptr);
  }

  void BlockUpdate() override {
      block_update = true;
  }

  void AllowUpdate() override {
      block_update = false;
  }

  void initPayload(uint8_t *byte_ptr) override {
    TypedValue *sum_ptr = reinterpret_cast<TypedValue *>(byte_ptr + blank_state_.sum_offset);
    bool *null_ptr = reinterpret_cast<bool *>(byte_ptr + blank_state_.null_offset);
    *sum_ptr = blank_state_.sum_;
    *null_ptr = true;
  }

  AggregationState* accumulateColumnVectors(
      const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const override;

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
  AggregationState* accumulateValueAccessor(
      ValueAccessor *accessor,
      const std::vector<attribute_id> &accessor_id) const override;
#endif

  void aggregateValueAccessorIntoHashTable(
      ValueAccessor *accessor,
      const std::vector<attribute_id> &argument_ids,
      const std::vector<attribute_id> &group_by_key_ids,
      AggregationStateHashTableBase *hash_table) const override;

  void mergeStates(const AggregationState &source,
                   AggregationState *destination) const override;

  void mergeStatesFast(const uint8_t *source,
                   uint8_t *destination) const override;

  TypedValue finalize(const AggregationState &state) const override;

  inline TypedValue finalizeHashTableEntry(const AggregationState &state) const {
    return static_cast<const AggregationStateSum&>(state).sum_;
  }

  inline TypedValue finalizeHashTableEntryFast(const uint8_t *byte_ptr) const {
    uint8_t *value_ptr = const_cast<uint8_t*>(byte_ptr);
    TypedValue *sum_ptr = reinterpret_cast<TypedValue *>(value_ptr + blank_state_.sum_offset);
    return *sum_ptr;
  }

  ColumnVector* finalizeHashTable(
      const AggregationStateHashTableBase &hash_table,
      std::vector<std::vector<TypedValue>> *group_by_keys, int index) const override;

  /**
   * @brief Implementation of AggregationHandle::aggregateOnDistinctifyHashTableForSingle()
   *        for SUM aggregation.
   */
  AggregationState* aggregateOnDistinctifyHashTableForSingle(
      const AggregationStateHashTableBase &distinctify_hash_table) const override;

  /**
   * @brief Implementation of AggregationHandle::aggregateOnDistinctifyHashTableForGroupBy()
   *        for SUM aggregation.
   */
  void aggregateOnDistinctifyHashTableForGroupBy(
      const AggregationStateHashTableBase &distinctify_hash_table,
      AggregationStateHashTableBase *aggregation_hash_table, int index) const override;

  void mergeGroupByHashTables(
      const AggregationStateHashTableBase &source_hash_table,
      AggregationStateHashTableBase *destination_hash_table) const override;

  size_t getPayloadSize() const override {
      return blank_state_.getPayloadSize();
  }

 private:
  friend class AggregateFunctionSum;

  /**
   * @brief Initialize handle for type.
   *
   * @param type Type of the sum value.
   **/
  explicit AggregationHandleSum(const Type &type);

  const Type &argument_type_;
  const Type *result_type_;
  AggregationStateSum blank_state_;
  std::unique_ptr<UncheckedBinaryOperator> fast_operator_;
  std::unique_ptr<UncheckedBinaryOperator> merge_operator_;

  bool block_update;

  DISALLOW_COPY_AND_ASSIGN(AggregationHandleSum);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_AGGREGATION_AGGREGATION_HANDLE_SUM_HPP_
