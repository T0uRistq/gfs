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

#include "sqlite3.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using gfs::FindLeaseHolderRequest;
using gfs::FindLeaseHolderReply;
using gfs::FindPathRequest;
using gfs::FindPathReply;
using gfs::GFSMaster;

ABSL_FLAG(uint16_t, port, 50052, "Master port for the service");

const char * const DB_INIT_QUERIES[] = {
  // The file table maps each filename to a unique file ID
  "CREATE TABLE IF NOT EXISTS file (file_id INTEGER PRIMARY KEY, filename TEXT NOT NULL)",
  // Enforce uniqueness of filename
  "CREATE UNIQUE INDEX IF NOT EXISTS file_filename ON file (filename)",

  // The chunk table maps each chunkhandle to a file_id
  // For each file, successive chunks have increasing chunkhandles
  "CREATE TABLE IF NOT EXISTS chunk (chunkhandle INTEGER PRIMARY KEY, "
    "file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL)",
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

    // Get file id
    sqlite3_stmt *select_file_stmt;
    sqlite3_prepare_v2(db_,
      "SELECT file_id FROM file WHERE filename=?",
      -1, &select_file_stmt, nullptr);
    sqlite3_bind_text(select_file_stmt,
      1, filename.c_str(), filename.length(), SQLITE_STATIC);
    sqlite3_step(select_file_stmt);
    int64_t file_id = sqlite3_column_int64(select_file_stmt, 0);
    sqlite3_finalize(select_file_stmt);

    // Try to insert chunk, ignoring if it already exists
    sqlite3_exec(
      db_,
      ("INSERT OR IGNORE INTO chunk (file_id, chunk_index) VALUES (\""
        + std::to_string(file_id) + "\", \""
        + std::to_string(chunk_index) + "\")").c_str(),
      nullptr, nullptr, nullptr);

    // Get chunkhandle
    sqlite3_stmt *select_chunk_stmt;
    sqlite3_prepare_v2(db_,
      "SELECT chunkhandle FROM chunk WHERE file_id=? AND chunk_index=?",
      -1, &select_chunk_stmt, nullptr);
    sqlite3_bind_int64(select_chunk_stmt, 1, file_id);
    sqlite3_bind_int64(select_chunk_stmt, 2, chunk_index);
    sqlite3_step(select_chunk_stmt);
    int64_t chunkhandle = sqlite3_column_int64(select_chunk_stmt, 0);
    sqlite3_finalize(select_chunk_stmt);
    
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
      const unsigned char *filename_bytes = sqlite3_column_text(list_files_stmt, 0);
      int filename_length = sqlite3_column_bytes(list_files_stmt, 0);
      std::string filename(reinterpret_cast<const char*>(filename_bytes), filename_length);
      auto *file_metadata = reply->add_files();
      file_metadata->set_filename(filename);
    }
    sqlite3_finalize(list_files_stmt);
    return Status::OK;
  }

 private:
  sqlite3 *db_;
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
  RunServer(absl::GetFlag(FLAGS_port), argv[1]);
  return 0;
}
