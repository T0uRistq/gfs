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
#include <memory>
#include <string>
#include <random>
#include <thread>
#include <google/protobuf/timestamp.pb.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/gfs.grpc.pb.h"
#else
#include "gfs.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindPathRequest;
using gfs::FindPathReply;
using gfs::FindLocationsRequest;
using gfs::FindLocationsReply;
using gfs::ReadChunkRequest;
using gfs::ReadChunkReply;
using gfs::WriteChunkRequest;
using gfs::WriteChunkReply;
using gfs::AppendRequest;
using gfs::AppendReply;
using gfs::PushDataRequest;
using gfs::PushDataReply;
using gfs::MoveFileRequest;
using gfs::MoveFileReply;
using gfs::RemoveFileRequest;
using gfs::RemoveFileReply;
using gfs::GetFileLengthRequest;
using gfs::GetFileLengthReply;
using gfs::GFS;
using gfs::GFSMaster;
using google::protobuf::Timestamp;

ABSL_FLAG(std::string, target, "localhost:50051", "Master address");

const int kChunkSize = 1 << 6; // 64 MB (B)
const int kAppendSize = 1 << 4; // 16 MB (B)
const int kMaxRetries = 5;

std::mt19937 rng(std::chrono::steady_clock::now().time_since_epoch().count());

class GFSClient {
 public:
  GFSClient(std::shared_ptr<Channel> master_channel)
      : stub_master_(GFSMaster::NewStub(master_channel)) {}

  Status ReadChunk(const int chunkhandle,
                   const int offset,
                   const int length,
                   const std::string& location,
                   std::string *buf) {
    ReadChunkRequest request;
    request.set_chunkhandle(chunkhandle);
    request.set_offset(offset);
    request.set_length(length);

    ReadChunkReply reply;
    ClientContext context;
    Status status = GetChunkserverStub(location)->ReadChunk(&context, request, &reply);

    // Act upon its status.
    if (status.ok()) {
      if (reply.bytes_read() == 0) {
        return Status(grpc::NOT_FOUND, "Data not found at chunkserver.");
      } else if (reply.bytes_read() != length) {
        std::cout << "Warning: ReadChunk read " << reply.bytes_read() << " bytes but asked for "
                  << length << "." << std::endl;
      }
      *buf = reply.data();
    }
    return status;
  }
  
  Status WriteChunk(const int chunkhandle,
                    const std::string data,
                    const int offset,
                    const std::vector<std::string>& locations,
                    const std::string& primary_location) {
    int64_t uuid = rng();
    for (const auto& location : locations) {
      Status status = PushData(GetChunkserverStub(location), uuid, data);
      if (!status.ok())
        return status;
    }

    return SendWriteToChunkServer(chunkhandle, offset, locations, uuid, primary_location);
  }

  Status FindLeaseHolder(FindLeaseHolderReply *reply,
                         const std::string& filename,
                         int64_t chunk_id) {
    FindLeaseHolderRequest request;
    request.set_filename(filename);
    request.set_chunk_index(chunk_id);

    ClientContext context;
    return stub_master_->FindLeaseHolder(&context, request, reply);
  }

  void FindPath(const std::string& prefix) {
    FindPathRequest request;
    request.set_prefix(prefix);

    FindPathReply reply;
    ClientContext context;
    Status status = stub_master_->FindPath(&context, request, &reply);

    if (status.ok()) {
      std::cout << "Found "<< reply.files_size() << " matches: " << std::endl; 
      for (const auto& file_metadata : reply.files())
        std::cout << file_metadata.filename() << std::endl;
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
    }
  }

  Status Remove(const std::string& filename) {
    RemoveFileRequest request;
    request.set_filename(filename);

    RemoveFileReply reply;
    ClientContext context;
    return stub_master_->RemoveFile(&context, request, &reply);
  }

  Status Move(const std::string& old_filename,
              const std::string& new_filename) {
    MoveFileRequest request;
    request.set_old_filename(old_filename);
    request.set_new_filename(new_filename);

    MoveFileReply reply;
    ClientContext context;
    return stub_master_->MoveFile(&context, request, &reply);
  }

  Status Read(std::string* buf,
              const std::string& filename,
              const int64_t offset,
              const int length) {
    int64_t chunk_index = offset / kChunkSize;
    int64_t chunk_offset = offset - (chunk_index * kChunkSize);
    // Keep trying to read from a random chunkserver until successful.
    for (int i = 0; i < kMaxRetries; i++) {
      // Only use FindLocations cache on the first try.
      bool use_cache = (i == 0);
      FindLocationsReply find_locations_reply;
      Status status = FindLocations(&find_locations_reply, filename,
                                    chunk_index, use_cache);
      if (!status.ok()) {
        return status;
      }
      if (find_locations_reply.locations_size() == 0) {
        return Status(grpc::NOT_FOUND, "Unable to find replicas for chunk.");
      }
      int sz = find_locations_reply.locations_size();
      const std::string& location = find_locations_reply.locations(rng() % sz);
      status = ReadChunk(find_locations_reply.chunkhandle(), chunk_offset,
                         length, location, buf);
      if (status.ok()) {
        return status;
      }
      std::cout << "Tried to read chunkhandle "
                << find_locations_reply.chunkhandle()
                << " from " << location << " but read failed with error "
                << status.error_message()
                << ". Retrying." << std::endl;

      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return Status(grpc::ABORTED, "ReadChunk failed too many times.");
  }

  Status Write(const std::string& buf, const std::string& filename, const int64_t offset) {
    int64_t chunk_index = offset / kChunkSize;
    int64_t chunk_offset = offset - (chunk_index * kChunkSize);
    FindLeaseHolderReply lease_holder;
    Status status = FindLeaseHolder(&lease_holder, filename, chunk_index);
    if (!status.ok())
      return status;
    std::vector<std::string> locations;
    for (const auto& location : lease_holder.locations())
      locations.push_back(location);

    return WriteChunk(lease_holder.chunkhandle(), buf, chunk_offset,
                      locations, lease_holder.primary_location());
  }

  // Returns offset in file that data was appended to
  int Append(const std::string& buf,
             const std::string& filename,
             int chunk_index) {
    int offset = -1;

    if (buf.length() > kAppendSize) {
      std::cout << "Append size (" << buf.length() << " is > " <<
                kAppendSize << "MB" << std::endl;
      return 0;
    }

    FindLeaseHolderReply lease_holder;
    Status status = FindLeaseHolder(&lease_holder, filename, chunk_index);
    if (!status.ok()) {
      std::cout << "FindLeaseHolder failed" << std::endl;
      return 0;
    }
    std::vector<std::string> locations;
    for (const auto& location : lease_holder.locations()) {
      locations.push_back(location);
    }

    int64_t uuid = rng();
    for (const auto& location : locations) {
      PushData(GetChunkserverStub(location), uuid, buf);
    }

    AppendRequest request;
    AppendReply reply;
    ClientContext context;

    request.set_chunkhandle(lease_holder.chunkhandle());
    request.set_length(buf.length());
    request.set_uuid(uuid);

    for (const auto& location : locations) {
      if (location != lease_holder.primary_location()) {
        request.add_locations(location);
      }
    }

    status = GetChunkserverStub(lease_holder.primary_location())->Append(&context, request, &reply);

    if (status.ok()) {
      offset = reply.offset();
      return offset;
    } else if (status.error_code() == grpc::RESOURCE_EXHAUSTED) {
      std::cout << "Reached end of chunk; We'll try another Append" << std::endl;
      offset = Append(buf, filename, ++chunk_index);
      if (offset != -1) {
        std::cout << "Append Succeeded in 2nd try; Offset = " << offset << std::endl;
      }
      return offset;
    } else {
      std::cout << "Append failure" << std::endl;
      return -1;
    }
    return offset;
  }

  Status PushData(gfs::GFS::Stub* stub,
                  const int64_t& uuid,
                  const std::string& data) {
    PushDataRequest request;
    PushDataReply reply;
    ClientContext context;

    request.set_data(data);
    request.set_uuid(uuid);

    Status status = stub->PushData(&context, request, &reply);

    if (!status.ok()) {
      std::cout << "PushData failed; " << status.error_message() << std::endl;
    }
    return status;
  }

  Status SendWriteToChunkServer(const int chunkhandle,
                                const int64_t offset,
                                const std::vector<std::string>& locations,
                                const int64_t& uuid,
                                const std::string& primary_location) {
    WriteChunkRequest request;
    WriteChunkReply reply;
    ClientContext context;

    request.set_chunkhandle(chunkhandle);
    request.set_offset(offset);
    request.set_uuid(uuid);

    for (const auto& location : locations) {
      if (location != primary_location) {
        request.add_locations(location);
      }
    }

    Status status = GetChunkserverStub(primary_location)->WriteChunk(&context, request, &reply);

    if (status.ok()) {
      std::cout << "Write Chunk written_bytes = " << reply.bytes_written()
                << std::endl;
    } else {
      std::cout << "Write Chunk failed " << std::endl;
    }
    return status;
  }

  Status FindLocations(FindLocationsReply *reply,
                       const std::string& filename,
                       int64_t chunk_index,
                       bool use_cache) {
    auto cache_key = std::make_pair(filename, chunk_index);
    if (use_cache) {
      auto it = find_locations_cache_.find(cache_key);
      if (it != find_locations_cache_.end()) {
        *reply = it->second;
        return Status::OK;
      }
    }

    FindLocationsRequest request;
    request.set_filename(filename);
    request.set_chunk_index(chunk_index);

    ClientContext context;
    Status status = stub_master_->FindLocations(&context, request, reply);
    if (status.ok()) {
      find_locations_cache_[cache_key] = *reply;
    }
    return status;
  }

  int GetFileLength(const std::string& filename) {
    GetFileLengthRequest request;
    request.set_filename(filename);

    GetFileLengthReply reply;
    ClientContext context;
    Status status = stub_master_->GetFileLength(&context, request, &reply);
    if (status.ok()) {
      std::cout << "File " << filename << " num_chunks = " << reply.num_chunks() << std::endl;
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
    }
    return reply.num_chunks();
  }

  gfs::GFS::Stub* GetChunkserverStub(const std::string& location) {
    gfs::GFS::Stub* result;
    grpc::ChannelArguments argument;
    argument.SetMaxReceiveMessageSize(100 << 20);
    auto it = stub_cs_.find(location);
    if (it != stub_cs_.end()) {
      result = it->second.get();
    } else {
      auto stub = gfs::GFS::NewStub(
          grpc::CreateCustomChannel(location, grpc::InsecureChannelCredentials(), argument));
      result = stub.get();
      stub_cs_[location] = std::move(stub);
    }
    return result;
  }
  
 private:
  std::unique_ptr<GFSMaster::Stub> stub_master_;
  std::map<std::pair<std::string, int64_t>, FindLocationsReply> find_locations_cache_;
  std::map<std::string, std::unique_ptr<gfs::GFS::Stub>> stub_cs_;
};

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  std::string target_str = absl::GetFlag(FLAGS_target);
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).
  GFSClient gfs_client(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));

  std::cout << "Usage: <command> <arg1> <arg2> <arg3>...\n"
            << "Options:\n"
            << "\tread\t<filepath>\t<offset>\t<length>\n"
            << "\twrite\t<filepath>\t<offset>\t<data>\n"
            << "\tappend\t<filepath>\t<data>\n"
            << "\tls\t<prefix>\n"
            << "\tmv\t<filepath>\t<new_filepath>\n"
            << "\trm\t<filepath>\n"
            << "\tquit"
            << std::endl;

  while (true) {
    std::cout << "> ";
    // Read an entire line and then read tokens from the line.
    std::string line;
    std::getline(std::cin, line);
    std::istringstream line_stream(line);

    std::string cmd;
    line_stream >> cmd;
    if (cmd == "read") {
      std::string filepath;
      int offset, length;
      if (line_stream >> filepath >> offset >> length) {
        std::string buf;
        Status status = gfs_client.Read(&buf, filepath, offset, length);
        std::cout << "Read status: " << status.ok()
                  << " data: " << buf << std::endl;
        continue;
      }
    } else if (cmd == "write") {
      std::string filepath, data;
      int offset;
      if (line_stream >> filepath >> offset >> data) {
        Status status = gfs_client.Write(data, filepath, offset);
        std::cout << "Write status: " << status.ok() << std::endl;
        continue;
      } 
    } else if (cmd == "append") {
      std::string filepath, data;
      if (line_stream >> filepath >> data) {
        gfs_client.Append(data, filepath,
                          gfs_client.GetFileLength(filepath) - 1);
        continue;
      }
    } else if (cmd == "ls") {
      std::string prefix;
      if (line_stream >> prefix) {
        gfs_client.FindPath(prefix);
      } else {
        // No prefix given, list all files.
        gfs_client.FindPath("");
      }
      continue;
    } else if (cmd == "mv") {
      std::string old_filename, new_filename;
      if (line_stream >> old_filename >> new_filename) {
        Status status = gfs_client.Move(old_filename, new_filename);
        std::cout << "Move status: " << status.ok() << std::endl;
        continue;
      }
    } else if (cmd == "rm") {
      std::string filename;
      if (line_stream >> filename) {
        Status status = gfs_client.Remove(filename);
        std::cout << "Remove status: " << status.ok() << std::endl;
        continue;
      }
    } else if (cmd == "quit") {
      break;
    }
    std::cout << "Invalid command" << std::endl;
  }
  return 0;
}
