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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_LIP_FILTER_GENERATOR_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_LIP_FILTER_GENERATOR_HPP_

#include <map>
#include <unordered_map>

#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.hpp"
#include "query_execution/QueryContext.pb.h"
#include "query_optimizer/QueryPlan.hpp"
#include "query_optimizer/physical/LIPFilterConfiguration.hpp"
#include "query_optimizer/physical/Aggregate.hpp"
#include "query_optimizer/physical/HashJoin.hpp"
#include "query_optimizer/physical/Physical.hpp"
#include "query_optimizer/physical/Selection.hpp"


namespace quickstep {

class CatalogAttribute;

namespace optimizer {

/** \addtogroup QueryOptimizer
 *  @{
 */

class LIPFilterGenerator {
 public:
  LIPFilterGenerator(const physical::LIPFilterConfigurationPtr &lip_filter_configuration)
      : lip_filter_configuration_(lip_filter_configuration) {
  }

  void registerAttributeMap(
      const physical::PhysicalPtr &node,
      const std::unordered_map<expressions::ExprId, const CatalogAttribute *> &attribute_substitution_map);

  void addAggregateInfo(const physical::AggregatePtr &aggregate,
                        const QueryPlan::DAGNodeIndex aggregate_operator_index,
                        const QueryContext::aggregation_state_id aggregation_state_id) {
    aggregate_infos_.emplace_back(aggregate, aggregate_operator_index, aggregation_state_id);
  }

  void addHashJoinInfo(const physical::HashJoinPtr &hash_join,
                       const QueryPlan::DAGNodeIndex build_operator_index,
                       const QueryPlan::DAGNodeIndex join_operator_index) {
    builder_op_index_map_.emplace(hash_join, build_operator_index);
    hash_join_infos_.emplace_back(hash_join, build_operator_index, join_operator_index);
  }

  void addSelectionInfo(const physical::SelectionPtr &selection,
                        const QueryPlan::DAGNodeIndex select_operator_index) {
    selection_infos_.emplace_back(selection, select_operator_index);
  }

  void deployLIPFilters(QueryPlan *execution_plan,
                        serialization::QueryContext *query_context_proto) const;

 private:
  struct AggregateInfo {
    AggregateInfo(const physical::AggregatePtr &aggregate_in,
                  const QueryPlan::DAGNodeIndex aggregate_operator_index_in,
                  const QueryContext::aggregation_state_id aggregation_state_id_in)
        : aggregate(aggregate_in),
          aggregate_operator_index(aggregate_operator_index_in),
          aggregation_state_id(aggregation_state_id_in) {
    }
    const physical::AggregatePtr aggregate;
    const QueryPlan::DAGNodeIndex aggregate_operator_index;
    const QueryContext::aggregation_state_id aggregation_state_id;
  };

  struct HashJoinInfo {
    HashJoinInfo(const physical::HashJoinPtr &hash_join_in,
                 const QueryPlan::DAGNodeIndex build_operator_index_in,
                 const QueryPlan::DAGNodeIndex join_operator_index_in)
        : hash_join(hash_join_in),
          build_operator_index(build_operator_index_in),
          join_operator_index(join_operator_index_in) {
    }
    const physical::HashJoinPtr hash_join;
    const QueryPlan::DAGNodeIndex build_operator_index;
    const QueryPlan::DAGNodeIndex join_operator_index;
  };

  struct SelectionInfo {
    SelectionInfo(const physical::SelectionPtr &selection_in,
                  const QueryPlan::DAGNodeIndex select_operator_index_in)
        : selection(selection_in),
          select_operator_index(select_operator_index_in) {
    }
    const physical::SelectionPtr selection;
    const QueryPlan::DAGNodeIndex select_operator_index;
  };

  const physical::LIPFilterConfigurationPtr lip_filter_configuration_;
  std::map<physical::PhysicalPtr, std::map<expressions::ExprId, const CatalogAttribute *>> attribute_map_;
  std::map<physical::PhysicalPtr, QueryPlan::DAGNodeIndex> builder_op_index_map_;
  std::vector<AggregateInfo> aggregate_infos_;
  std::vector<HashJoinInfo> hash_join_infos_;
  std::vector<SelectionInfo> selection_infos_;
};


/** @} */

}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_LIP_FILTER_GENERATOR_HPP_ */
