#pragma once

#include <string>

namespace leveldb {

struct NVMOption {
  std::string pmem_path = "/mnt/pmem";
  std::string sst_path="/mnt/ssd/leveldb";
  //size_t nvm_write_buffer_size = 6* 1024 * 1024 * 1024;
  // char buf[100];
  // snprintf(buf, sizeof(buf), "%s/_PM_sstable.pool",
  //          NVMOption.pmem_path.c_str());
  // std::string pol_path(buf, strlen(buf));


};

}  // namespace leveldb