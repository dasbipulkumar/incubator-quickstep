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

#include "expressions/aggregation/AggregationHandleCount.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/HashTable.hpp"
#include "storage/HashTableFactory.hpp"

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
#include "storage/ValueAccessor.hpp"
#include "storage/ValueAccessorUtil.hpp"
#endif

#include "types/TypeFactory.hpp"
#include "types/TypeID.hpp"
#include "types/TypedValue.hpp"
#include "types/containers/ColumnVector.hpp"
#include "types/containers/ColumnVectorUtil.hpp"

#include "glog/logging.h"

namespace quickstep {

class StorageManager;
class Type;
class ValueAccessor;

template <bool count_star, bool nullable_type>
AggregationStateHashTableBase*
AggregationHandleCount<count_star, nullable_type>::createGroupByHashTable(
    const HashTableImplType hash_table_impl,
    const std::vector<const Type *> &group_by_types,
    const std::size_t estimated_num_groups,
    StorageManager *storage_manager) const {
  return AggregationStateHashTableFactory<
      AggregationStateCount>::CreateResizable(hash_table_impl,
                                              group_by_types,
                                              estimated_num_groups,
                                              storage_manager);
}

template <bool count_star, bool nullable_type>
AggregationState*
AggregationHandleCount<count_star, nullable_type>::accumulateColumnVectors(
    const std::vector<std::unique_ptr<ColumnVector>> &column_vectors) const {
  DCHECK(!count_star)
      << "Called non-nullary accumulation method on an AggregationHandleCount "
      << "set up for nullary COUNT(*)";

  DCHECK_EQ(1u, column_vectors.size())
      << "Got wrong number of ColumnVectors for COUNT: "
      << column_vectors.size();

  std::size_t count = 0;
  InvokeOnColumnVector(
      *column_vectors.front(),
      [&](const auto &column_vector) -> void {  // NOLINT(build/c++11)
        if (nullable_type) {
          // TODO(shoban): Iterating over the ColumnVector is a rather slow way
          // to do this. We should look at extending the ColumnVector interface
          // to do a quick count of the non-null values (i.e. the length minus
          // the population count of the null bitmap). We should do something
          // similar for ValueAccessor too.
          for (std::size_t pos = 0; pos < column_vector.size(); ++pos) {
            count += !column_vector.getTypedValue(pos).isNull();
          }
        } else {
          count = column_vector.size();
        }
      });

  return new AggregationStateCount(count);
}

#ifdef QUICKSTEP_ENABLE_VECTOR_COPY_ELISION_SELECTION
template <bool count_star, bool nullable_type>
AggregationState*
AggregationHandleCount<count_star, nullable_type>::accumulateValueAccessor(
    ValueAccessor *accessor,
    const std::vector<attribute_id> &accessor_ids) const {
  DCHECK(!count_star)
      << "Called non-nullary accumulation method on an AggregationHandleCount "
      << "set up for nullary COUNT(*)";

  DCHECK_EQ(1u, accessor_ids.size())
      << "Got wrong number of attributes for COUNT: " << accessor_ids.size();

  const attribute_id accessor_id = accessor_ids.front();
  std::size_t count = 0;
  InvokeOnValueAccessorMaybeTupleIdSequenceAdapter(
      accessor,
      [&accessor_id, &count](auto *accessor) -> void {  // NOLINT(build/c++11)
        if (nullable_type) {
          while (accessor->next()) {
            count += !accessor->getTypedValue(accessor_id).isNull();
          }
        } else {
          count = accessor->getNumTuples();
        }
      });

  return new AggregationStateCount(count);
}
#endif

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::
    aggregateValueAccessorIntoHashTable(
        ValueAccessor *accessor,
        const std::vector<attribute_id> &argument_ids,
        const std::vector<attribute_id> &group_by_key_ids,
        AggregationStateHashTableBase *hash_table) const {
  if (count_star) {
    DCHECK_EQ(0u, argument_ids.size())
        << "Got wrong number of arguments for COUNT(*): "
        << argument_ids.size();
  } else {
    DCHECK_EQ(1u, argument_ids.size())
        << "Got wrong number of arguments for COUNT: " << argument_ids.size();
  }
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::mergeStates(
    const AggregationState &source, AggregationState *destination) const {
  const AggregationStateCount &count_source =
      static_cast<const AggregationStateCount &>(source);
  AggregationStateCount *count_destination =
      static_cast<AggregationStateCount *>(destination);

  count_destination->count_.fetch_add(
      count_source.count_.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::mergeStatesFast(
    const std::uint8_t *source, std::uint8_t *destination) const {
  const std::int64_t *src_count_ptr =
      reinterpret_cast<const std::int64_t *>(source);
  std::int64_t *dst_count_ptr = reinterpret_cast<std::int64_t *>(destination);
  (*dst_count_ptr) += (*src_count_ptr);
}

template <bool count_star, bool nullable_type>
ColumnVector*
AggregationHandleCount<count_star, nullable_type>::finalizeHashTable(
    const AggregationStateHashTableBase &hash_table,
    std::vector<std::vector<TypedValue>> *group_by_keys,
    int index) const {
  return finalizeHashTableHelperFast<
      AggregationHandleCount<count_star, nullable_type>,
      AggregationStateFastHashTable>(
      TypeFactory::GetType(kLong), hash_table, group_by_keys, index);
}

template <bool count_star, bool nullable_type>
AggregationState* AggregationHandleCount<count_star, nullable_type>::
    aggregateOnDistinctifyHashTableForSingle(
        const AggregationStateHashTableBase &distinctify_hash_table) const {
  DCHECK_EQ(count_star, false);
  return aggregateOnDistinctifyHashTableForSingleUnaryHelperFast<
      AggregationHandleCount<count_star, nullable_type>,
      AggregationStateCount>(distinctify_hash_table);
}

template <bool count_star, bool nullable_type>
void AggregationHandleCount<count_star, nullable_type>::
    aggregateOnDistinctifyHashTableForGroupBy(
        const AggregationStateHashTableBase &distinctify_hash_table,
        AggregationStateHashTableBase *aggregation_hash_table,
        std::size_t index) const {
  DCHECK_EQ(count_star, false);
  aggregateOnDistinctifyHashTableForGroupByUnaryHelperFast<
      AggregationHandleCount<count_star, nullable_type>,
      AggregationStateFastHashTable>(
      distinctify_hash_table, aggregation_hash_table, index);
}

// Explicitly instantiate and compile in the different versions of
// AggregationHandleCount we need. Note that we do not compile a version with
// 'count_star == true' and 'nullable_type == true', as that combination is
// semantically impossible.
template class AggregationHandleCount<false, false>;
template class AggregationHandleCount<false, true>;
template class AggregationHandleCount<true, false>;

}  // namespace quickstep
