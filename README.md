<h1 align="center">HydraKV</h1>
<p align="center">
  An LSM-tree key-value store for DRAM, persistent memory, and SSDs.
</p>

---

## 1 Overview

HydraKV is a research implementation based on **LevelDB**. It combines hot/cold data management in DRAM, PM table compaction, and Region-based SSTable management on SSDs.

This repository contains:

1. **The HydraKV storage engine**, including the hot/cold detector, PM tables, a PM-resident B+ tree for SSD data locations, and Region compaction.
2. **Benchmark drivers**, including `db_bench` and the YCSB C++ client (`ycsbc`).
3. **Workloads and scripts** for configuring and running storage experiments.

---

## 2 System Requirements

| Component | Requirement | Notes |
| --- | --- | --- |
| Operating system | Ubuntu 22.04 LTS or a compatible Linux distribution | The implementation uses Linux NUMA and PMDK APIs. |
| Compiler | GCC / G++ 11 or newer | Build with C++17. |
| CMake | 3.16 or newer | Required by the bundled GoogleTest dependency. |
| GNU Make | Required | Used to build the YCSB client. |
| PMDK | `libpmem` and `libpmemobj` development packages | Requires allocation-class support through `pmemobj_ctl_*` and `pmemobj_xalloc`. |
| NUMA | `libnuma` and `numactl` | PM table allocations use configured NUMA nodes. |
| Compression libraries | Snappy and zlib development packages | Required by the YCSB link configuration. |
| Storage | PMEM with a DAX filesystem and an SSD | Prepare writable storage paths before running benchmarks. |

Install the prerequisites on Ubuntu:

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake git \
  libnuma-dev numactl \
  libpmem-dev libpmemobj-dev \
  libsnappy-dev zlib1g-dev
```

---

## 3 Getting Started

Obtain the source tree in a directory named `HydraKV`. Run the following commands from the repository root.

### 3.1 Build HydraKV

Configure a static Release build with `-O3` optimization and debugging symbols:

```bash
cmake -S . -B build-linux \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=17 \
  -DCMAKE_CXX_FLAGS_RELEASE='-O3 -g -DNDEBUG' \
  -DBUILD_SHARED_LIBS=OFF \
  -DLEVELDB_BUILD_TESTS=ON \
  -DLEVELDB_BUILD_BENCHMARKS=ON \
  -DHAVE_CRC32C=OFF \
  -DHAVE_ZSTD=OFF \
  -DHAVE_TCMALLOC=OFF

cmake --build build-linux --target db_bench -j"$(nproc)"
```

`LEVELDB_BUILD_TESTS=ON` makes the GoogleTest and GoogleMock targets available to `db_bench`. The build command above does not run the test suite.

### 3.2 Build the YCSB Client

Build the engine first, then compile and link the client against `build-linux/libleveldb.a`:

```bash
make -B -C ycsbc -j"$(nproc)" \
  CFLAGS='-std=c++17 -O3 -g -DNDEBUG -Wall -pthread'
```

The YCSB Makefile resolves the source and library paths automatically. No project-root environment variable is required.

| Output | Location |
| --- | --- |
| Static storage library | `build-linux/libleveldb.a` |
| Microbenchmark executable | `build-linux/db_bench` |
| YCSB client executable | `ycsbc/ycsbc` |

---

## 4 Running Benchmarks

Run the examples from the HydraKV repository root, using fresh test storage for each independent load experiment.

Create the output directory:

```bash
mkdir -p logs
```

### 4.1 db_bench

Run sequential loading, print database statistics, and perform random reads:

```bash
./build-linux/db_bench \
  --db=/mnt/ssd/leveldb \
  --benchmarks=fillseq,stats,readrandom \
  --num=50000000 \
  --reads=50000000 \
  --value_size=1024 \
  --threads=1 \
  --write_buffer_size=67108864 \
  --max_file_size=67108864 \
  --histogram=1 \
  --vlog=0 \
  2>&1 | tee logs/db_bench_1k_50g.log
```

### 4.2 YCSB

Run a workload by specifying its configuration file:

```bash
./ycsbc/ycsbc \
  -filename /mnt/ssd/leveldb \
  -db leveldb \
  -configpath 0 \
  -P ./ycsbc/workloads/workloadb1.spec
```

Replace the `-P` path with another workload specification from `ycsbc/workloads/` as needed.

### 4.3 Benchmark Output

The `db_bench` example captures console output in `logs/` and enables latency histograms. The YCSB client reports Load and Run throughput separately in KTPS.

Engine diagnostics are written to `/mnt/ssd/leveldb/LOG`. PMDK allocation errors are printed to standard error.

---

## 5 Repository Layout

```text
HydraKV/
├── benchmarks/       # db_bench sources
├── db/               # read/write paths, PM compaction, and Region management
├── include/leveldb/  # public interfaces and index definitions
├── index/            # B+ tree, asynchronous index updates, and iterators
├── NVM/              # NUMA memory accounting
├── table/            # SSTable construction and access
├── util/             # allocators, PMDK pool support, and platform utilities
├── ycsbc/            # YCSB C++ client and LevelDB binding
│   └── workloads/    # workload specification files
├── scripts/          # platform setup and benchmark helper scripts
├── third_party/      # bundled GoogleTest and Google Benchmark
├── CMakeLists.txt    # engine and microbenchmark build configuration
└── LICENSE           # upstream LevelDB license
```
