/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <fstream>
#include <memory>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD
#include "examples/protos/gfs.grpc.pb.h"
#else
#include "gfs.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::PingRequest;
using gfs::PingReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::GFS;

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

const int kChunkSize = 1 << 26; // bytes

// Logic and data behind the server's behavior.
class GFSServiceImpl final : public GFS::Service {
 public:
  GFSServiceImpl(std::string path) {
    this->full_path = path;
  }

  Status ClientServerPing(ServerContext* context, const PingRequest* request,
                          PingReply* reply) override {
    std::string prefix("Ping ");
    reply->set_message(prefix + request->name());
    return Status::OK;
  }

  Status ReadChunk(ServerContext* context, const ReadChunkRequest* request,
                   ReadChunkReply* reply) override {
    int chunkhandle = request->chunkhandle();
    int offset = request->offset();
    int length = request->length();
    std::string chunk_data(length, '0');
    std::string filename = this->full_path + "/" +
                           std::to_string(request->chunkhandle());

    if (length + offset > kChunkSize) {
      std::cout << "Can't read chunk_size<" << length + offset << std::endl;
      reply->set_bytes_read(0);
      return Status::OK;
    }

    std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);
    if (!infile.is_open()) {
      std::cout << "Can't open file for reading: " << filename << std::endl;
      reply->set_bytes_read(0);
    } else {
      infile.seekg(offset, std::ios::beg);
      infile.read(&chunk_data[0], length);
      infile.close();
      reply->set_bytes_read(infile.gcount());
      reply->set_data(chunk_data);
    }

    return Status::OK;
  }

  Status WriteChunk(ServerContext* context, const WriteChunkRequest* request,
                    WriteChunkReply* reply) override {
    std::cout << "Got server WriteChunk for chunkhandle="
              << request->chunkhandle() << " and data=" << request->data()
              << std::endl;
    // TODO: handle this critical section

    int chunkhandle = request->chunkhandle();
    int offset = request->offset();
    std::string data = request->data();
    int length = data.length();
    std::string filename = this->full_path + "/" +
                           std::to_string(request->chunkhandle());

    if (length + offset > kChunkSize) {
      std::cout << "Can't write chunk_size<" << length + offset << std::endl;
      reply->set_bytes_written(0);
      return Status::OK;
    }

    std::ofstream outfile(filename.c_str(), std::ios::out | std::ios::binary);
    if (!outfile.is_open()) {
      std::cout << "Can't open file for writing: " << filename << std::endl;
      reply->set_bytes_written(0);
    } else {
      outfile.seekp(offset, std::ios::beg);
      outfile.write(data.c_str(), length);
      outfile.close();
      reply->set_bytes_written(length);
    }

    return Status::OK;
  }

 private:
  std::string full_path;
};

void RunServer(uint16_t port, std::string path) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
  GFSServiceImpl service(path);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  RunServer(absl::GetFlag(FLAGS_port), argv[1]);
  return 0;
}
