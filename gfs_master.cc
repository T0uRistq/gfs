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
#include <random>
#include <vector>

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

#include "sqlite3.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::FindLocationsRequest;
using gfs::FindLocationsReply;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindPathRequest;
using gfs::FindPathReply;
using gfs::AppendRequest;
using gfs::AppendReply;
using gfs::GetFileLengthRequest;
using gfs::GetFileLengthReply;
using gfs::HeartBeatRequest;
using gfs::HeartBeatReply;
using gfs::MoveFileRequest;
using gfs::MoveFileReply;
using gfs::RemoveFileRequest;
using gfs::RemoveFileReply;
using gfs::RemoveChunksRequest;
using gfs::RemoveChunksReply;
using gfs::GFSMaster;

ABSL_FLAG(uint16_t, port, 50051, "Master port for the service");
ABSL_FLAG(std::string, path, "/tmp/gfs/db", "Path to SQLite database file that master uses");

const int kLeaseDuration = 10;
const int kMinChunkServers = 2;

std::mt19937 rng(std::chrono::steady_clock::now().time_since_epoch().count());

const char * const DB_INIT_QUERIES[] = {
  // The file table maps each filename to a unique file ID
  "CREATE TABLE IF NOT EXISTS file (file_id INTEGER PRIMARY KEY, filename TEXT NOT NULL)",
  // Enforce uniqueness of filename
  "CREATE UNIQUE INDEX IF NOT EXISTS file_filename ON file (filename)",

  // The chunk table maps each chunkhandle to a file_id
  // For each file, successive chunks have increasing chunkhandles
  "CREATE TABLE IF NOT EXISTS chunk ("
    "chunkhandle INTEGER PRIMARY KEY, "
    "file_id INTEGER NOT NULL REFERENCES file(file_id) ON DELETE CASCADE, "
    "chunk_index INTEGER NOT NULL)",
  // Enforce uniqueness of (file_id, chunk_index)
  "CREATE UNIQUE INDEX IF NOT EXISTS chunk_file_id_chunk_index ON chunk (file_id, chunk_index)",
};

// Logic and data behind the server's behavior.
class GFSMasterImpl final : public GFSMaster::Service {
 public:
  GFSMasterImpl(std::string storage_path) {
    sqlite3_open(storage_path.c_str(), &db_);
    for (const char *query : DB_INIT_QUERIES)
      sqlite3_exec(db_, query, nullptr, nullptr, nullptr);
  }

  ~GFSMasterImpl() {
    sqlite3_close(db_);
  }

  Status FindLocations(ServerContext* context,
                       const FindLocationsRequest* request,
                       FindLocationsReply* reply) {
  const std::string& filename = request->filename();
  const int64_t chunk_index = request->chunk_index();

  int64_t file_id = GetFileId(filename);
  if (file_id == -1)
    return Status(grpc::NOT_FOUND, "File does not exist.");

  int64_t chunkhandle = GetChunkhandle(file_id, chunk_index);
  if (chunkhandle == -1)
    return Status(grpc::NOT_FOUND, "Chunk index does not exist.");

  std::vector<std::string> locations = GetLocations(chunkhandle);
  if (locations.empty())
    return Status(grpc::NOT_FOUND, "Chunk exists but unable to find locations.\
		    Probably need to wait for HeartBeat from chunkserver.");
  
  reply->set_chunkhandle(chunkhandle);
  for (const auto& location : locations) {
    reply->add_locations(location);
  }
  return Status::OK;
}

  Status FindLeaseHolder(ServerContext* context,
                         const FindLeaseHolderRequest* request,
                         FindLeaseHolderReply* reply) {
    const std::string& filename = request->filename();
    const int64_t chunk_index = request->chunk_index();

    // Try to insert file, ignoring if it already exists
    sqlite3_exec(
      db_,
      ("INSERT OR IGNORE INTO file (filename) VALUES (\""
        + filename + "\")").c_str(),
      nullptr, nullptr, nullptr);

    int64_t file_id = GetFileId(filename);
    if (file_id == -1) {
      return Status(grpc::NOT_FOUND, "File does not exist.");
    }

    if (chunk_index > 1 && GetChunkhandle(file_id, chunk_index-1) == -1) {
      return Status(grpc::FAILED_PRECONDITION, "Chunk index - 1 does not exist.");
    }

    // Try to insert chunk, ignoring if it already exists
    sqlite3_exec(
      db_,
      ("INSERT OR IGNORE INTO chunk (file_id, chunk_index) VALUES (\""
        + std::to_string(file_id) + "\", \""
        + std::to_string(chunk_index) + "\")").c_str(),
      nullptr, nullptr, nullptr);

    // Get chunkhandle
    int64_t chunkhandle = GetChunkhandle(file_id, chunk_index);
    if (chunkhandle == -1) {
      // This should never happen because the chunk was created if necessary.
      return Status(grpc::NOT_FOUND, "Chunk index does not exist.");
    }
    
    std::vector<std::string> locations = GetLocations(chunkhandle);
    if (locations.empty()) {
      return Status(grpc::NOT_FOUND, "Chunk exists but unable to find \
        locations. Probably need to wait for HeartBeat from chunkserver.");
    }
    
    for (const auto& location : locations) {
      reply->add_locations(location);
    }
    reply->set_primary_location(locations.at(0));
    reply->set_chunkhandle(chunkhandle);
    return Status::OK;
  }

  Status FindPath(ServerContext* context,
                  const FindPathRequest* request,
                  FindPathReply* reply) {
    std::string like_query = request->prefix() + "%";

    // Get file list from sqlite.
    sqlite3_stmt *list_files_stmt;
    sqlite3_prepare_v2(db_,
      "SELECT filename FROM file WHERE filename LIKE ? ORDER BY filename",
      -1, &list_files_stmt, nullptr);
    sqlite3_bind_text(list_files_stmt,
      1, like_query.c_str(), like_query.length(), SQLITE_STATIC);
    while (sqlite3_step(list_files_stmt) == SQLITE_ROW) {
      const unsigned char *filename_bytes = sqlite3_column_text(
        list_files_stmt, 0);
      int filename_length = sqlite3_column_bytes(list_files_stmt, 0);
      std::string filename(reinterpret_cast<const char*>(filename_bytes),
                           filename_length);
      auto *file_metadata = reply->add_files();
      file_metadata->set_filename(filename);
    }
    sqlite3_finalize(list_files_stmt);
    return Status::OK;
  }

  Status GetFileLength(ServerContext* context,
                       const GetFileLengthRequest* request,
                       GetFileLengthReply* reply) {
  int64_t file_id = GetFileId(request->filename());
  if (file_id == -1) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }

  // Get number of chunks from sqlite.
  sqlite3_stmt *select_count_stmt;
  sqlite3_prepare_v2(db_,
      "SELECT COUNT(*) FROM chunk WHERE file_id=?",
      -1, &select_count_stmt, nullptr);
  sqlite3_bind_int64(select_count_stmt, 1, file_id);
  sqlite3_step(select_count_stmt);
  int64_t num_chunks = sqlite3_column_int64(select_count_stmt, 0);
  sqlite3_finalize(select_count_stmt);
  reply->set_num_chunks(num_chunks);
  return Status::OK;
}

Status MoveFile(ServerContext* context,
                const MoveFileRequest* request,
                MoveFileReply* reply) {
  // Attempt to move file in sqlite database.
  sqlite3_stmt *move_file_stmt;
  sqlite3_prepare_v2(db_,
      "UPDATE file SET filename=? WHERE filename=?",
      -1, &move_file_stmt, nullptr);
  sqlite3_bind_text(move_file_stmt, 1,
      request->new_filename().c_str(), request->new_filename().length(),
      SQLITE_STATIC);
  sqlite3_bind_text(move_file_stmt, 2,
      request->old_filename().c_str(), request->old_filename().length(),
      SQLITE_STATIC);
  sqlite3_step(move_file_stmt);
  sqlite3_finalize(move_file_stmt);
  if (sqlite3_changes(db_) == 0) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }
  return Status::OK;
}

Status RemoveFile(ServerContext* context,
                  const RemoveFileRequest* request,
                  RemoveFileReply* reply) {
  // Attempt to move file in sqlite database.
  // Chunks are automatically deleted by sqlite (ON DELETE CASCADE).
  sqlite3_stmt *delete_file_stmt;
  sqlite3_prepare_v2(db_,
      "DELETE FROM file WHERE filename=?",
      -1, &delete_file_stmt, nullptr);
  sqlite3_bind_text(delete_file_stmt, 1,
      request->filename().c_str(), request->filename().length(),
      SQLITE_STATIC);
  sqlite3_step(delete_file_stmt);
  sqlite3_finalize(delete_file_stmt);
  if (sqlite3_changes(db_) == 0) {
    return Status(grpc::NOT_FOUND, "File does not exist.");
  }
  return Status::OK;

}

Status HeartBeat(ServerContext* context,
                 const HeartBeatRequest* request,
                 HeartBeatReply* response) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!chunk_servers_.count(request->location())) {
    std::cout << "Found out about new chunkserver: "
              << request->location() << std::endl;
    // Create connection to chunkserver that can be reused
    chunk_servers_[request->location()].stub = gfs::GFS::NewStub(
      grpc::CreateChannel(request->location(),
      grpc::InsecureChannelCredentials()));
  }
  // Set new lease expiry for chunkserver.
  chunk_servers_[request->location()].lease_expiry = time(nullptr)
                                                     + kLeaseDuration;
  for (const auto& chunk : request->chunks()) {
    auto& locations = chunk_locations_[chunk.chunkhandle()];
    bool already_know = false;
    for (const auto& location : locations) {
      if (location.location == request->location()) {
        already_know = true;
        break;
      }
    }
    if (!already_know) {
      ChunkLocation location;
      location.location = request->location();
      location.version = 0; // TODO: implement versions.
      locations.push_back(location);
      std::cout << "Found out that chunkserver " << request->location()
                << " stores chunk " << chunk.chunkhandle() << std::endl;
    }
  }
  return Status::OK;
}

int64_t GetFileId(const std::string& filename) {
  sqlite3_stmt *select_file_stmt;
  sqlite3_prepare_v2(db_,
      "SELECT file_id FROM file WHERE filename=?",
      -1, &select_file_stmt, nullptr);
  sqlite3_bind_text(select_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC);
  if (sqlite3_step(select_file_stmt) != SQLITE_ROW) return -1;
  int64_t file_id = sqlite3_column_int64(select_file_stmt, 0);
  sqlite3_finalize(select_file_stmt);
  return file_id;
}

int64_t GetChunkhandle(int64_t file_id, int64_t chunk_index) {
  sqlite3_stmt *select_chunk_stmt;
  sqlite3_prepare_v2(db_,
      "SELECT chunkhandle FROM chunk WHERE file_id=? AND chunk_index=?",
      -1, &select_chunk_stmt, nullptr);
  sqlite3_bind_int64(select_chunk_stmt, 1, file_id);
  sqlite3_bind_int64(select_chunk_stmt, 2, chunk_index);
  if (sqlite3_step(select_chunk_stmt) != SQLITE_ROW) return -1;
  int64_t chunkhandle = sqlite3_column_int64(select_chunk_stmt, 0);
  sqlite3_finalize(select_chunk_stmt);
  return chunkhandle;
}

std::vector<std::string> GetLocations(int64_t chunkhandle) {
  std::vector<std::string> result;
  std::lock_guard<std::mutex> guard(mutex_);
  auto& locations = chunk_locations_[chunkhandle];
  if (locations.size() < kMinChunkServers)
    RereplicateChunk(chunkhandle, &locations);
  for (const auto& chunk_location : locations)
    result.push_back(chunk_location.location);
  return result;
}

void RemoveChunks() {
  // Get list of chunkhandles from database
  sqlite3_stmt *select_chunkhandles_stmt;
  sqlite3_prepare_v2(db_,
      "SELECT chunkhandle FROM chunk",
      -1, &select_chunkhandles_stmt, nullptr);
  std::set<int64_t> db_chunkhandles;
  while (sqlite3_step(select_chunkhandles_stmt) == SQLITE_ROW) {
    int64_t chunkhandle = sqlite3_column_int64(select_chunkhandles_stmt, 0);
    db_chunkhandles.insert(chunkhandle);
  }
  sqlite3_finalize(select_chunkhandles_stmt);
  std::map<std::string, std::vector<int64_t> > chunkhandles_to_delete;

  // Rereplicate chunks if necessary
  for (auto it = chunk_locations_.begin(); it != chunk_locations_.end(); ) {
    int64_t chunkhandle = it->first;
    auto& locations = it->second;

    for (auto location = locations.begin(); location != locations.end(); ) {
      if (chunk_servers_.count(location->location) == 0) {
        // This chunkserver's lease expired.
        location = locations.erase(location);
      } else {
        ++location;
      }
    }

    if (!db_chunkhandles.count(chunkhandle)) {
      // This chunkhandle does not exist in the database.
      // Instruct all chunkservers to delete chunk.
      for (const auto& location : locations) {
        chunkhandles_to_delete[location.location].push_back(chunkhandle);
      }
      std::cout << "Chunkhandle " << chunkhandle
                << " no longer exists." << std::endl;
      // Delete chunkhandle from memory.
      it = chunk_locations_.erase(it);
      continue;
    }
  }

  // Tell chunkservers to delete chunks if necessary.
  for (const auto& it : chunkhandles_to_delete) {
    const std::string& location = it.first;
    const std::vector<int64_t> chunkhandles = it.second;
    RemoveChunksRequest req;
    RemoveChunksReply rep;
    grpc::ClientContext cont;
    for (int64_t chunkhandle : chunkhandles) {
      req.add_chunkhandles(chunkhandle);
    }

    gfs::GFS::Stub& stub = *chunk_servers_[location].stub;
    Status status = stub.RemoveChunks(&cont, req, &rep);
    std::cout << "RemoveChunks request to " << location
              << ": " << status.ok() << std::endl;
  }
}



 private:
  sqlite3 *db_;
  struct ChunkLocation {
    std::string location; // "ip:port"
    int64_t version;
  };
  // Map from chunkhandle to location.
  // The first entry in the vector is the primary.
  std::map<int64_t, std::vector<ChunkLocation>> chunk_locations_;

  struct ChunkServer {
    time_t lease_expiry;
    std::unique_ptr<gfs::GFS::Stub> stub;
  };
  // Map from "ip:port" to chunkserver info.
  std::map<std::string, ChunkServer> chunk_servers_;

  std::mutex mutex_;

  void RereplicateChunk(int64_t chunkhandle,
                        std::vector<ChunkLocation>* locations) {
  if (locations->size() < kMinChunkServers) {
    if (chunk_servers_.size() >= kMinChunkServers) {
      // Randomly pick chunkservers to add.
      std::vector<std::string> all_locations;
      for (const auto& location : chunk_servers_) {
        all_locations.push_back(location.first);
      }
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(0, chunk_servers_.size() - 1);
      while (locations->size() < kMinChunkServers) {
        const std::string& try_location = all_locations[dis(gen)];
        bool already_in_list = false;
        for (const auto& location : *locations) {
          if (location.location == try_location) {
            already_in_list = true;
            break;
          }
        }
        if (!already_in_list) {
          ChunkLocation chunk_location;
          chunk_location.location = try_location;
          chunk_location.version = 0; // TODO: implement version
          locations->push_back(chunk_location);
          std::cout << "Added new location " << try_location
                    << " for chunkhandle " << chunkhandle << std::endl;
        }
      }
    } else {
      std::cout << "ERROR: Number of known chunkservers is less than "
                << kMinChunkServers << std::endl;
    }
  }
}
};

void RunServer(uint16_t port, std::string path) {
  std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
  GFSMasterImpl service(path);

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
  std::cout << "Master listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  RunServer(absl::GetFlag(FLAGS_port), absl::GetFlag(FLAGS_path));
  return 0;
}
