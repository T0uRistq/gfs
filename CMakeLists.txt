# Copyright 2018 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cmake build file for C++ gfs example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building gfs.

cmake_minimum_required(VERSION 3.8)

project(GFS C CXX)

include(../cmake/common.cmake)

# Proto file
get_filename_component(gfs_proto "../../protos/gfs.proto" ABSOLUTE)
get_filename_component(gfs_proto_path "${gfs_proto}" PATH)

# Generated sources
set(gfs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gfs.pb.cc")
set(gfs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gfs.pb.h")
set(gfs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gfs.grpc.pb.cc")
set(gfs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gfs.grpc.pb.h")
add_custom_command(
      OUTPUT "${gfs_proto_srcs}" "${gfs_proto_hdrs}" "${gfs_grpc_srcs}" "${gfs_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${gfs_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${gfs_proto}"
      DEPENDS "${gfs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# gfs_grpc_proto
add_library(gfs_grpc_proto
  ${gfs_grpc_srcs}
  ${gfs_grpc_hdrs}
  ${gfs_proto_srcs}
  ${gfs_proto_hdrs})
target_link_libraries(gfs_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Targets gfs_(client|server)
foreach(_target
  gfs_client gfs_server)
  add_executable(${_target} "${_target}.cc")
  target_link_libraries(${_target}
    gfs_grpc_proto
    absl::flags
    absl::flags_parse
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()
