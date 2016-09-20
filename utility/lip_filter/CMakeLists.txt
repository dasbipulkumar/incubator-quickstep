# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

QS_PROTOBUF_GENERATE_CPP(utility_lipfilter_LIPFilter_proto_srcs
                         utility_lipfilter_LIPFilter_proto_hdrs
                         LIPFilter.proto)

# Declare micro-libs:
add_library(quickstep_utility_lipfilter_LIPFilter LIPFilter.cpp LIPFilter.hpp)
add_library(quickstep_utility_lipfilter_LIPFilterAdaptiveProber ../../empty_src.cpp LIPFilterAdaptiveProber.hpp)
add_library(quickstep_utility_lipfilter_LIPFilterBuilder ../../empty_src.cpp LIPFilterBuilder.hpp)
add_library(quickstep_utility_lipfilter_LIPFilterDeploymentInfo ../../empty_src.cpp LIPFilterDeploymentInfo.hpp)
add_library(quickstep_utility_lipfilter_LIPFilterFactory LIPFilterFactory.cpp LIPFilterFactory.hpp)
add_library(quickstep_utility_lipfilter_LIPFilter_proto
            ${utility_lipfilter_LIPFilter_proto_srcs})

# Link dependencies:
target_link_libraries(quickstep_utility_lipfilter_LIPFilter
                      quickstep_utility_Macros)
target_link_libraries(quickstep_utility_lipfilter_LIPFilterAdaptiveProber
                      quickstep_catalog_CatalogTypedefs
                      quickstep_utility_Macros)
target_link_libraries(quickstep_utility_lipfilter_LIPFilterBuilder
                      quickstep_catalog_CatalogTypedefs
                      quickstep_utility_Macros)
target_link_libraries(quickstep_utility_lipfilter_LIPFilterDeploymentInfo
                      quickstep_catalog_CatalogTypedefs
                      quickstep_utility_Macros
                      quickstep_utility_lipfilter_LIPFilter)
target_link_libraries(quickstep_utility_lipfilter_LIPFilterFactory
                      quickstep_utility_lipfilter_LIPFilter_proto
                      quickstep_utility_Macros)
target_link_libraries(quickstep_utility_lipfilter_LIPFilter_proto
                      ${PROTOBUF_LIBRARY}
                      quickstep_types_Type_proto)
