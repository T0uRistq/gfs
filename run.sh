#!/bin/bash

mkdir -p cmake/build
cd cmake/build
cmake -DCMAKE_PREFIX_PATH=$HOME/.local ../..
make -j 4
