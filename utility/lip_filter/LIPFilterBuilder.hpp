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

#ifndef QUICKSTEP_UTILITY_LIP_FILTER_LIP_FILTER_BUILDER_HPP_
#define QUICKSTEP_UTILITY_LIP_FILTER_LIP_FILTER_BUILDER_HPP_

#include <vector>

#include "utility/Macros.hpp"

#include "catalog/CatalogTypedefs.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

class LIPFilterBuilder {
 public:
  LIPFilterBuilder(const std::vector<LIPFilter *> &lip_filters,
                   const std::vector<attribute_id> &attr_ids,
                   const std::vector<std::size_t> &attr_sizes) {
    DCHECK_EQ(lip_filters.size(), attr_ids.size());
    DCHECK_EQ(lip_filters.size(), attr_sizes.size());

    build_entries_.reserve(lip_filters.size());
    for (std::size_t i = 0; i < lip_filters.size(); ++i) {
      build_entries_.emplace_back(lip_filters[i], attr_ids[i], attr_sizes[i]);
    }
  }

 private:
  struct BuildEntry {
    BuildEntry(LIPFilter *lip_filter_in,
               const attribute_id attr_id_in,
               const std::size_t attr_size_in)
        : lip_filter(lip_filter_in),
          attr_id(attr_id_in),
          attr_size(attr_size_in) {
    }
    LIPFilter *lip_filter;
    const attribute_id attr_id;
    const std::size_t attr_size;
  };

  std::vector<BuildEntry> build_entries_;

  DISALLOW_COPY_AND_ASSIGN(LIPFilterBuilder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_LIP_FILTER_LIP_FILTER_HPP_
