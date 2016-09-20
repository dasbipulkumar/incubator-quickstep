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

#include "query_optimizer/LIPFilterGenerator.hpp"

#include <map>
#include <utility>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryContext.pb.h"
#include "utility/lip_filter/LIPFilter.hpp"
#include "utility/lip_filter/LIPFilter.pb.h"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {

namespace E = ::quickstep::optimizer::expressions;
namespace P = ::quickstep::optimizer::physical;

void LIPFilterGenerator::registerAttributeMap(
    const P::PhysicalPtr &node,
    const std::unordered_map<E::ExprId, const CatalogAttribute *> &attribute_substitution_map) {
  const auto &build_infos = lip_filter_configuration_->getBuildInfo();
  const auto build_it = build_infos.find(node);
  if (build_it != build_infos.end()) {
    auto &map_entry = attribute_map_[node];
    for (const auto &info : build_it->second) {
      E::ExprId attr_id = info.build_attribute->id();
      map_entry.emplace(attr_id, attribute_substitution_map.at(attr_id));
    }
  }
  const auto &probe_infos = lip_filter_configuration_->getProbeInfo();
  const auto probe_it = probe_infos.find(node);
  if (probe_it != probe_infos.end()) {
    auto &map_entry = attribute_map_[node];
    for (const auto &info : probe_it->second) {
      E::ExprId attr_id = info.probe_attribute->id();
      map_entry.emplace(attr_id, attribute_substitution_map.at(attr_id));
    }
  }
}

void LIPFilterGenerator::deployLIPFilters(QueryPlan *execution_plan,
                                          serialization::QueryContext *query_context_proto) const {
  std::map<std::pair<E::ExprId, P::PhysicalPtr>,
           std::pair<QueryContext::lip_filter_id, QueryPlan::DAGNodeIndex>> lip_filter_builder_map;
  for (const auto &builder : lip_filter_configuration_->getBuildInfo()) {
    const P::PhysicalPtr &builder_node = builder.first;
    const auto &builder_attribute_map = attribute_map_.at(builder_node);
    for (const auto &info : builder.second) {
      const QueryContext::lip_filter_id lip_filter_id = query_context_proto->lip_filters_size();
      serialization::LIPFilter *lip_filter_proto = query_context_proto->add_lip_filters();

      switch (info.filter_type) {
        case LIPFilterType::kSingleIdentityHashFilter:
          lip_filter_proto->set_lip_filter_type(
              serialization::LIPFilterType::SINGLE_IDENTITY_HASH_FILTER);
          lip_filter_proto->SetExtension(
              serialization::SingleIdentityHashFilter::num_bits, info.filter_size);
          break;
        default:
          LOG(FATAL) << "Unsupported LIPFilter type";
          break;
      }

      lip_filter_builder_map.emplace(
          std::make_pair(info.build_attribute->id(), builder_node),
          std::make_pair(lip_filter_id, builder_op_index_map_.at(builder_node)));

      auto *lip_filter_deployment_info_proto =
          query_context_proto->add_lip_filter_deployment_infos();
      lip_filter_deployment_info_proto->set_action_type(serialization::LIPFilterActionType::BUILD);
      lip_filter_deployment_info_proto->set_lip_filter_id(lip_filter_id);

      const CatalogAttribute *target_attr = builder_attribute_map.at(info.build_attribute->id());
      lip_filter_deployment_info_proto->set_attribute_id(target_attr->getID());
      lip_filter_deployment_info_proto->mutable_attribute_type()->CopyFrom(target_attr->getProto());

      std::cerr << "Build " << info.build_attribute->toString()
                << " @" << builder_node << "\n";
    }
  }
}

}  // namespace optimizer
}  // namespace quickstep
