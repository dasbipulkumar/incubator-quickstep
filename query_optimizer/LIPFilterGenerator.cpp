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

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogTypeDefs.hpp"

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
      map_entry.emplace(attr_id, attribute_substitution_map.at(attr_id)->getID());
    }
  }
  const auto &probe_infos = lip_filter_configuration_->getProbeInfo();
  const auto probe_it = probe_infos.find(node);
  if (probe_it != probe_infos.end()) {
    auto &map_entry = attribute_map_[node];
    for (const auto &info : probe_it->second) {
      E::ExprId attr_id = info.probe_attribute->id();
      map_entry.emplace(attr_id, attribute_substitution_map.at(attr_id)->getID());
    }
  }
}

}  // namespace optimizer
}  // namespace quickstep
