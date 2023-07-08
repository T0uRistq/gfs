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
#include <thread>
#include <csignal>

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
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::RemoveChunksRequest;
using gfs::RemoveChunksReply;
using gfs::SerializedWriteRequest;
using gfs::SerializedWriteReply;
using gfs::AppendRequest;
using gfs::AppendReply;
using gfs::PushDataRequest;
using gfs::PushDataReply;
using gfs::HeartBeatRequest;
using gfs::HeartBeatReply;
using gfs::GFS;

ABSL_FLAG(uint16_t, port, 50052, "Server port for the service");
ABSL_FLAG(std::string, dir, "/tmp/gfs", "Root directory for the service");

const std::string master_address = "localhost:50051";
const int kChunkSize = 1 << 6; // bytes
const int kHBPeriod = 2; // seconds;

typedef struct WriteChunkInfo {
  int64_t uuid;
  int chunkhandle;
  int offset;
  bool padded_chunk;
} WriteChunkInfo;

// Logic and data behind the server's behavior.
class GFSServiceImpl final : public GFS::Service {
 public:
  GFSServiceImpl(std::string path, std::string server_address) {
    this->full_path = path;
    this->location_me = server_address;
    this->version_number = 1;
    this->am_i_dead = false;
    this->metadata_file = "metadata" + location_me;
    stub_master = gfs::GFSMaster::NewStub(grpc::CreateChannel(
        master_address,
        grpc::InsecureChannelCredentials()));

    // Send initial heartbeat to inform about this server.
    HeartBeatRequest request;
    HeartBeatReply reply;
    grpc::ClientContext context;
    request.set_location(location_me);
    Status status = stub_master->HeartBeat(&context, request, &reply);
    if (status.ok()) {
      std::cout << "Successfully registered with master" << std::endl;
    } else {
      std::cout << "Failed to register with master" << std::endl;
    }

    heartbeat_thread = std::thread(std::bind(
        &GFSServiceImpl::ServerMasterHeartBeat, this));
  }

  ~GFSServiceImpl() {
    // Signal thread to shutdown.
    am_i_dead_mutex.lock();
    am_i_dead = true;
    am_i_dead_mutex.unlock();

    // Wait for thread to exit.
    heartbeat_thread.join();
  }

  Status ReadChunk(ServerContext* context,
                   const ReadChunkRequest* request,
                   ReadChunkReply* reply) {
    int chunkhandle = request->chunkhandle();
    int offset = request->offset();
    int length = request->length();
    std::string chunk_data(length, '0');
    std::string filename = this->full_path + "/"
                           + std::to_string(chunkhandle);

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

  int PerformLocalWriteChunk(const WriteChunkInfo& wc_info) {
    int64_t uuid = wc_info.uuid;
    int chunkhandle = wc_info.chunkhandle;
    int offset = wc_info.offset;

    std::lock_guard<std::mutex> guard(buffercache_mutex);

    if (buffercache.find(uuid) == buffercache.end()) {
      std::cout << "LocalWriteChunk: Chunk data doesn't exists in buffercache"
                << std::endl;
      return 0; // 0 bytes written
    }

    std::string data = buffercache[uuid];
    int length = data.length();
    std::string filename = this->full_path + "/" + \
                          std::to_string(chunkhandle);

    if ((length + offset > kChunkSize)) {
      std::cout << "Write exceeds chunk size: " << length + offset << std::endl;
      return 0;
    }

    // Open in r/w if it exists; Otherwise it's a new file
    std::ofstream outfile(filename.c_str(),
                          std::ios::in | std::ios::out | std::ios::binary);
    if (!outfile.is_open()) {
      outfile.clear();
      outfile.open(filename.c_str(), std::ios::out | std::ios::binary);
      assert(offset == 0);
    }
    if (!outfile.is_open()) {
      std::cout << "can't open file for writing: " << filename << std::endl;
      return 0;
    } else {
      outfile.seekp(offset, std::ios::beg);
      outfile.write(data.c_str(), length);
      outfile.close();

      // Write succeeded, so we can remove the buffercache entry
      buffercache.erase(uuid);
      return length;
    }
  }

  int SendSerializedWriteChunk(WriteChunkInfo& wc_info,
                               const std::string location) {

    SerializedWriteRequest request;
    SerializedWriteReply reply;
    grpc::ClientContext context;

    // Create a connection to replica ChunkServer
    std::unique_ptr<gfs::GFS::Stub> stub = gfs::GFS::NewStub(
        grpc::CreateChannel(location, grpc::InsecureChannelCredentials()));

    // Prepare Request -> Perform RPC -> Read reply -> Return
    request.set_uuid(wc_info.uuid);
    request.set_chunkhandle(wc_info.chunkhandle);
    request.set_offset(wc_info.offset);
    request.set_padded_chunk(wc_info.padded_chunk);

    Status status = stub->SerializedWrite(&context, request, &reply);

    if (status.ok()) {
      std::cout << "SerializedWrite bytes_written = " << reply.bytes_written() <<
                " at location: " << location << std::endl;
      return reply.bytes_written();
    } else {
      std::cout << "SerializedWrite failed at location: " << location << std::endl;
      return 0;
    }

  }

  Status SerializedWrite(ServerContext* context,
                         const SerializedWriteRequest* request,
                         SerializedWriteReply* reply) {
    WriteChunkInfo wc_info;
    int bytes_written;

    wc_info.uuid = request->uuid();
    wc_info.chunkhandle = request->chunkhandle();
    wc_info.offset = request->offset();
    wc_info.padded_chunk = request->padded_chunk();

    if (wc_info.padded_chunk) {
      if(!PadChunk(wc_info)) {
        return Status(grpc::INTERNAL, "Couldn't complete PadChunk");
      }
    }

    bytes_written = PerformLocalWriteChunk(wc_info);
    reply->set_bytes_written(bytes_written);

    if (bytes_written) {
      ReportChunkInfo(wc_info.chunkhandle);
    }

    return Status::OK;
  }

  Status WriteChunk(ServerContext* context,
                    const WriteChunkRequest* request,
                    WriteChunkReply* reply) {

    std::lock_guard<std::mutex> guard(write_mutex);
    std::cout << "Got server WriteChunk for chunkhandle="
              << request->chunkhandle() << std::endl;

    WriteChunkInfo wc_info;
    wc_info.uuid = request->uuid();
    wc_info.chunkhandle = request->chunkhandle();
    wc_info.offset = request->offset();
    wc_info.padded_chunk = false;


    int bytes_written = PerformLocalWriteChunk(wc_info);

    // If the local write succeeded, send SerializedWrites to secondaries
    if (bytes_written) {
      for (const auto& location : request->locations()) {
        std::cout << "Chunkserver location: " << location << std::endl;
        if (location != location_me
            && !SendSerializedWriteChunk(wc_info, location)) {
          bytes_written = 0;
          std::cout << "SerializedWrite failed for location: " << location
                    << std::endl;
          break;
        }
      }
    }

    reply->set_bytes_written(bytes_written);

    if (bytes_written) {
      ReportChunkInfo(wc_info.chunkhandle);
    }

    return Status::OK;
  }

  bool PadChunk(const WriteChunkInfo& wc_info)
  {
    int64_t uuid = wc_info.uuid;
    int offset = wc_info.offset;

    std::lock_guard<std::mutex> guard(buffercache_mutex);

    if (buffercache.find(uuid) == buffercache.end()) {
      std::cout << "PadChunk: Chunk data doesn't exists in buffercache"
                << std::endl;
      return false;
    }

    std::string padding_data(kChunkSize - offset, '0');
    std::cout << "padding_data.len = " << padding_data.length() << std::endl;
    buffercache[uuid] = padding_data;

    return true;
  }

  Status Append(ServerContext* context,
                const AppendRequest* request,
                AppendReply* reply) {
    int bytes_written, length, existing_length = 0;
    WriteChunkInfo wc_info;

    std::lock_guard<std::mutex> guard(write_mutex);

    std::cout << "Got server Append for chunkhandle = " << \
              request->chunkhandle() << std::endl;

    wc_info.uuid = request->uuid();
    wc_info.chunkhandle = request->chunkhandle();
    wc_info.padded_chunk = false;
    length = request->length();

    // TODO: Length can be in an in-memory structure and avoid us from having
    // to read the file
    std::string filename = this->full_path + "/" + \
                          std::to_string(wc_info.chunkhandle);
    std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);
    if (infile) {
      assert(infile.is_open() != 0);
      infile.seekg(0, std::ios::end);
      existing_length = infile.tellg();
      infile.seekg(0, std::ios::beg);
      infile.close();
    }

    wc_info.offset = existing_length;
    if (existing_length + length > kChunkSize) {
      if(!PadChunk(wc_info)) {
        return Status(grpc::INTERNAL,
                      "Couldn't complete Append. PadChunk failed");
      }
      wc_info.padded_chunk = true;
    }

    bytes_written = PerformLocalWriteChunk(wc_info);

    // If the local write succeeded, send SerializedWrites to secondaries
    if (bytes_written) {
      for (const auto& location : request->locations()) {
        std::cout << "Chunkserver location: " << location << std::endl;
        if (location != location_me
            && !SendSerializedWriteChunk(wc_info, location)) {
          bytes_written = 0;
          std::cout << "SerializedWrite failed for location: " << location
                    << std::endl;
          break;
        }
      }
      ReportChunkInfo(wc_info.chunkhandle);
    }

    if (!bytes_written) {
      return Status(grpc::INTERNAL, "Couldn't complete Append");
    }

    reply->set_bytes_written(bytes_written);
    reply->set_offset(existing_length);

    if (wc_info.padded_chunk) {
      return Status(grpc::RESOURCE_EXHAUSTED, "Reached end of chunk");
    }

    return Status::OK;
  }

  Status PushData(ServerContext* context,
                  const PushDataRequest* request,
                  PushDataReply* reply) {
    std::string data = request->data();
    int64_t uuid = request->uuid();

    std::cout << "Got server PushData with uuid="
              << uuid << /*" and data=" << data <<*/ std::endl;

    std::lock_guard<std::mutex> guard(buffercache_mutex);

    if (buffercache.find(uuid) != buffercache.end()) {
      std::cout << "Chunk data already exists" << std::endl;
    }

    buffercache[uuid] = data;
    return Status::OK;
  }

  Status RemoveChunks(ServerContext* context,
                      const RemoveChunksRequest* request,
                      RemoveChunksReply* reply) {
    for (int64_t chunkhandle : request->chunkhandles()) {
      // TODO: Actually perform deletion.
      std::cout << "Got RemoveChunks request for " << chunkhandle << std::endl;
    }
    return Status(grpc::UNIMPLEMENTED, "Not implemented.");
  }

  void ReportChunkInfo(int chunkhandle) {
    // Acquire the metadata mutex
    std::lock_guard<std::mutex> guard(metadata_mutex);

    // Check if we already have a recent write to the chunkhandle
    // if not write to the metadata map
    if (metadata.find(chunkhandle) == metadata.end())
      metadata[chunkhandle] = version_number; // TODO: implement version
  }

  // This function takes the in-memory metadata and sends it over to the Master.
  // If there haven't been any writes in the last interval, it still sends an
  // empty HeartBeat RPC so that the Master knows that the Server is alive
  void ServerMasterHeartBeat() {
    int key, value;
    std::string filename = this->full_path + "/" + \
                          this->metadata_file;
    std::ifstream infile(filename.c_str(), std::ios::in | std::ios::binary);

    // At startup, check whether we have a metadata file
    // and populate the metadata map with it.
    if (!infile.is_open()) {
      std::cout << "There's no metadata file for reading: " << filename
                << std::endl;
    } else {
      metadata_mutex.lock();
      while (infile >> key >> value) {
        metadata[key] = value;
      }
      metadata_mutex.unlock();
      infile.close();
    }

    while (true) {
      // std::cout << "HeartBeat thread woke up" << std::endl;
      am_i_dead_mutex.lock();
      if (am_i_dead) break;
      am_i_dead_mutex.unlock();

      HeartBeatRequest request;
      HeartBeatReply reply;
      grpc::ClientContext context;

      if (metadata.size() != 0) {
        // Acquire the metadata mutex
        metadata_mutex.lock();

        for (auto& ch : metadata) {
          auto *chunk_info = request.add_chunks();
          chunk_info->set_chunkhandle(ch.first);
          std::cout << "chunkhandle metadata: " << ch.first << std::endl;

          // Also write metadata to disk for crash recovery
          std::string filename = this->full_path + "/" + \
                                this->metadata_file;

          std::ofstream outfile(filename.c_str(),
                                std::ios::app | std::ios::binary);
          if (!outfile.is_open()) {
            std::cout << "can't open file for writing: "
                      << filename << std::endl;
            continue;
          }
          outfile << ch.first << " " << ch.second << "\n";
          outfile.close();
        }
        metadata.clear();
        metadata_mutex.unlock();
      }
      request.set_location(location_me);

      Status status = stub_master->HeartBeat(&context, request, &reply);
      if (status.ok()) {
        // std::cout << "New chunkhandle hearbeat sent " << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(kHBPeriod));
    }
  }


 private:
  std::string full_path;
  std::unique_ptr<gfs::GFSMaster::Stub> stub_master;
  std::string metadata_file;
  std::map<int64_t, std::string> buffercache;
  std::map<int, int> metadata; // int chunkhandle, int version_no
  std::mutex buffercache_mutex, am_i_dead_mutex, metadata_mutex, write_mutex;
  std::string location_me;
  int version_number;
  bool am_i_dead;
  std::thread heartbeat_thread;
};

std::unique_ptr<Server> server;

void RunServer(uint16_t port, std::string path) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
  GFSServiceImpl service(path, server_address);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  server = builder.BuildAndStart();
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void HandleTerminate(int sig) {
  if (server) {
    std::cout << "Shutting down" << std::endl;
    server->Shutdown();
  }
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  RunServer(absl::GetFlag(FLAGS_port), absl::GetFlag(FLAGS_dir));
  std::signal(SIGINT, HandleTerminate);
  std::signal(SIGTERM, HandleTerminate);
  return 0;
}
