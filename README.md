# GFS-like filesystem

## How to build and run

Build and install grpc as shown in [C++ Quick Start][].

[C++ Quick Start]: https://grpc.io/docs/languages/cpp/quickstart

### To build

```shell
cd ~/grpc/examples/cpp
git clone https://github.com/raja-19/gfs.git
cd gfs
./run.sh
```

### To run

```shell
cmake/build/gfs_master --path <path to db>                        # runs master
cmake/build/gfs_server --dir <path to dir> --port <port number>   # runs server
        ..
cmake/build/gfs_client                                            # runs client
```
Use ```gfs_(master/client/sever) --help``` to see details.
