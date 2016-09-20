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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIP_FILTER_CONFIGURATION_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIP_FILTER_CONFIGURATION_HPP_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "query_optimizer/expressions/AttributeReference.hpp"
#include "utility/lip_filter/LIPFilter.hpp"
#include "utility/Macros.hpp"

#include "glog/logging.h"

namespace quickstep {
namespace optimizer {
namespace physical {

/** \addtogroup OptimizerPhysical
 *  @{
 */

class Physical;
typedef std::shared_ptr<const Physical> PhysicalPtr;

struct LIPFilterBuildInfo{
  LIPFilterBuildInfo(const expressions::AttributeReferencePtr &build_attribute_in,
                     const std::size_t filter_size_in,
                     const LIPFilterType &filter_type_in)
      : build_attribute(build_attribute_in),
        filter_size(filter_size_in),
        filter_type(filter_type_in) {
  }
  expressions::AttributeReferencePtr build_attribute;
  std::size_t filter_size;
  LIPFilterType filter_type;
};

struct LIPFilterProbeInfo {
  LIPFilterProbeInfo(const expressions::AttributeReferencePtr &probe_attribute_in,
                     const expressions::AttributeReferencePtr &build_attribute_in,
                     const PhysicalPtr &builder_in)
      : probe_attribute(probe_attribute_in),
        build_attribute(build_attribute_in),
        builder(builder_in) {
  }
  expressions::AttributeReferencePtr probe_attribute;
  expressions::AttributeReferencePtr build_attribute;
  PhysicalPtr builder;
};


class LIPFilterConfiguration;
typedef std::shared_ptr<const LIPFilterConfiguration> LIPFilterConfigurationPtr;

class LIPFilterConfiguration {
 public:
  LIPFilterConfiguration() {
  }

  void addBuildInfo(const expressions::AttributeReferencePtr &build_attribute,
                    const PhysicalPtr &builder,
                    const std::size_t filter_size,
                    const LIPFilterType &filter_type) {
    build_info_[builder].emplace_back(
        build_attribute, filter_size, filter_type);
  }

  void addProbeInfo(const expressions::AttributeReferencePtr &probe_attribute,
                    const PhysicalPtr &prober,
                    const expressions::AttributeReferencePtr &build_attribute,
                    const PhysicalPtr &builder) {
    probe_info_[prober].emplace_back(
        probe_attribute, build_attribute, builder);
  }

  const std::map<PhysicalPtr, std::vector<LIPFilterBuildInfo>>& getBuildInfo() const {
    return build_info_;
  }

  const std::map<PhysicalPtr, std::vector<LIPFilterProbeInfo>>& getProbeInfo() const {
    return probe_info_;
  }

 private:
  std::map<PhysicalPtr, std::vector<LIPFilterBuildInfo>> build_info_;
  std::map<PhysicalPtr, std::vector<LIPFilterProbeInfo>> probe_info_;

  DISALLOW_COPY_AND_ASSIGN(LIPFilterConfiguration);
};

/** @} */

}  // namespace physical
}  // namespace optimizer
}  // namespace quickstep

#endif /* QUICKSTEP_QUERY_OPTIMIZER_PHYSICAL_LIP_FILTER_CONFIGURATION_HPP_ */
