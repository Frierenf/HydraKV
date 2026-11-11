//
// Created by user on 2026/5/23.
//
// region_manager.cc
#include "db/region_manager.h"
#include "db/version_edit.h"
#include "db/dbformat.h"
#include "util/coding.h"
#include "leveldb/env.h"
#include <algorithm>
#include <cstdio>
#include <thread>
#include <iostream>

namespace leveldb {

//const uint32_t REGION_KEY_RANGE = 10000000;   // 与 A 版本相同
//const int kNumRegions = config::kNumLevels;  // 假设 config::kNumLevels 已定义
const uint64_t REGION_KEY_RANGE = 5000000;

// region 0 保留不用。
// 如果 recordcount = 50000000，REGION_KEY_RANGE = 2500000，
// 需要 20 个有效 region，所以 kNumRegions 至少要 21。
// 这里设置 64，方便后续实验。
const int kNumRegions = 64;

RegionMetaManager g_region_meta_manager;

int RegionMetaManager::NumRegions() const {
  return kNumRegions;
}

bool RegionMetaManager::AdjustRegionBoundaryForCompaction(
    int region_id, bool shrink_right, const InternalKey& boundary_key) {
  std::lock_guard<std::mutex> lock(region_mutex_);

  if (region_id <= 0 || region_id >= kNumRegions) {
    return false;
  }

  const int neighbor_id = shrink_right ? region_id + 1 : region_id - 1;

  if (neighbor_id <= 0 || neighbor_id >= kNumRegions) {
    return false;
  }

  auto UserKeyToUint64 = [](const Slice& key, uint64_t* value) -> bool {
    if (key.empty()) {
      return false;
    }

    uint64_t v = 0;
    for (size_t i = 0; i < key.size(); ++i) {
      const char ch = key.data()[i];
      if (ch < '0' || ch > '9') {
        return false;
      }
      v = v * 10 + static_cast<uint64_t>(ch - '0');
    }

    *value = v;
    return true;
  };

  auto FormatUserKey = [](uint64_t value) -> std::string {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%016llu",
                  static_cast<unsigned long long>(value));
    return std::string(buf);
  };

  auto MakeInternalKey = [&](uint64_t value) -> InternalKey {
    return InternalKey(FormatUserKey(value), 1, kTypeValue);
  };

  uint64_t boundary = 0;
  if (!UserKeyToUint64(boundary_key.user_key(), &boundary)) {
    return false;
  }

  Region& target = regions_[region_id];
  Region& neighbor = regions_[neighbor_id];

  uint64_t target_min = 0;
  uint64_t target_max = 0;
  uint64_t neighbor_min = 0;
  uint64_t neighbor_max = 0;

  if (!UserKeyToUint64(target.region_smallest.user_key(), &target_min) ||
      !UserKeyToUint64(target.region_largest.user_key(), &target_max) ||
      !UserKeyToUint64(neighbor.region_smallest.user_key(), &neighbor_min) ||
      !UserKeyToUint64(neighbor.region_largest.user_key(), &neighbor_max)) {
    return false;
  }

  if (shrink_right) {
    // 当前 Region 右边界左移，右邻居向左扩张。
    //
    // 原来：
    //   target   = [target_min, target_max]
    //   neighbor = [neighbor_min, neighbor_max]
    //
    // 调整后：
    //   target   = [target_min, boundary]
    //   neighbor = [boundary + 1, neighbor_max]
    if (boundary <= target_min || boundary >= target_max) {
      return false;
    }

    if (boundary + 1 > neighbor_max) {
      return false;
    }

    target.region_largest = MakeInternalKey(boundary);
    neighbor.region_smallest = MakeInternalKey(boundary + 1);
  } else {
    // 当前 Region 左边界右移，左邻居向右扩张。
    //
    // 原来：
    //   neighbor = [neighbor_min, neighbor_max]
    //   target   = [target_min, target_max]
    //
    // 调整后：
    //   neighbor = [neighbor_min, boundary - 1]
    //   target   = [boundary, target_max]
    if (boundary <= target_min || boundary >= target_max) {
      return false;
    }

    if (boundary == 0) {
      return false;
    }

    target.region_smallest = MakeInternalKey(boundary);
    neighbor.region_largest = MakeInternalKey(boundary - 1);
  }

  return true;
}

void RegionMetaManager::RemoveFileFromAllRegions(uint64_t file_number) {
  std::lock_guard<std::mutex> lock(region_mutex_);
  for (auto& region : regions_) {
    auto it = std::remove_if(region.region_metas.begin(), region.region_metas.end(),
                             [file_number](const FileMetaData& meta) {
                               return meta.number == file_number;
                             });
    if (it != region.region_metas.end()) {
      region.region_metas.erase(it, region.region_metas.end());
      region.region_num_sst = region.region_metas.size();
      // 可选：打印日志
      //Log(options_.info_log, "Removed file #%llu from region %d",
          //(unsigned long long)file_number, region.region_id);
    }
  }
}

int RegionMetaManager::FindRegionByFileNumber(uint64_t file_number) const {
  std::lock_guard<std::mutex> lock(region_mutex_);
  for (const auto& region : regions_) {
    for (const auto& meta : region.region_metas) {
      if (meta.number == file_number) {
        return region.region_id;
      }
    }
  }
  return -1;  // 未找到
}

// ---------- Region 方法 ----------
void Region::AddFileMeta(const FileMetaData& meta) {
  region_metas.push_back(meta);
  region_num_sst++;
}

void Region::Clear() {
  region_metas.clear();
  region_num_sst = 0;
  region_id = -1;
}

bool Region::IsEmpty() const {
  return region_metas.empty();
}

uint32_t Region::GetFileCount() const {
  return region_num_sst;
}

// ---------- RegionMetaManager 实现 ----------
RegionMetaManager::RegionMetaManager()
    : regions_(kNumRegions), is_left_adjust_(false), is_right_adjust_(false) {
  // 初始化 region 0 为空
  regions_[0].region_id = 0;
  // 对于 i >= 1，分配固定键范围
  for (int i = 1; i < kNumRegions; ++i) {
    Region& r = regions_[i];
    r.region_id = i;
    uint32_t start_key = (i-1) * REGION_KEY_RANGE;
    uint32_t end_key   = i * REGION_KEY_RANGE - 1;
    char minkey[32], maxkey[32];
    std::snprintf(minkey, sizeof(minkey), "%016u", start_key);
    std::snprintf(maxkey, sizeof(maxkey), "%016u", end_key);
    r.region_smallest = InternalKey(minkey, 1, kTypeValue);
    r.region_largest  = InternalKey(maxkey, 1, kTypeValue);
  }
  printf("[Region] Initialized %d regions, key range per region = %u\n",
         kNumRegions, REGION_KEY_RANGE);
  fflush(stdout);
}

void RegionMetaManager::UpdateRegionMetaforFlush(int level,
                                                 const InternalKey& smallest,
                                                 const InternalKey& largest,
                                                 const FileMetaData& meta) {
  // B 版本中 L0 flush 不应调用此方法，但保留空实现或 assert
  // 若被调用，说明错误
  assert(false);
}

void RegionMetaManager::UpdateRegionMetaforCompaction(int region_id,
                                                      const FileMetaData& meta) {
  std::lock_guard<std::mutex> lock(region_mutex_);

  // region 0 当前没有分配有效 key range，真正的 SSD Region 从 1 开始
  if (region_id <= 0 || region_id >= kNumRegions) {
    fprintf(stderr,
            "[Region] Ignore invalid region_id=%d for file #%llu\n",
            region_id,
            (unsigned long long)meta.number);
    return;
  }

  Region& r = regions_[region_id];

  // 关键修正：
  // 不允许用 SSTable 的 smallest/largest 覆盖 Region 的逻辑边界。
  // r.region_smallest / r.region_largest 表示固定的 Region key range，
  // 只能由初始化逻辑或显式的 Region 边界调整函数修改，
  // 不能由某个输出 SSTable 的实际 key 范围修改。
  //
  // 原来的错误逻辑是：
  // if (r.IsEmpty()) {
  //   r.region_smallest = meta.smallest;
  //   r.region_largest  = meta.largest;
  // } else {
  //   ...
  // }
  //
  // 这样会导致 Region1 原本是 [0, 2499999]，
  // 但第一个进入 Region1 的文件如果是 [1000, 5000]，
  // Region1 的逻辑边界就会被错误改成 [1000, 5000]。

  // 防御性检查：正常情况下，输出 SSTable 的 key range 应该落在该 Region 边界内。
  // 这里只打印 warning，不强制 return，避免因为边界格式或调试阶段误差导致系统直接中断。
  if (meta.smallest.user_key().compare(r.region_smallest.user_key()) < 0 ||
      meta.largest.user_key().compare(r.region_largest.user_key()) > 0) {
    fprintf(stderr,
            "[Region] Warning: file #%llu range [%s .. %s] is outside "
            "region %d boundary [%s .. %s]\n",
            (unsigned long long)meta.number,
            meta.smallest.user_key().ToString().c_str(),
            meta.largest.user_key().ToString().c_str(),
            region_id,
            r.region_smallest.user_key().ToString().c_str(),
            r.region_largest.user_key().ToString().c_str());
  }

  // 避免同一个文件被重复加入 Region 元数据。
  for (const auto& old_meta : r.region_metas) {
    if (old_meta.number == meta.number) {
      return;
    }
  }

  // 只更新 Region 内的文件集合，不修改 Region 的逻辑 key 边界。
  r.AddFileMeta(meta);
}

void RegionMetaManager::RemoveFileInRegion(int region_id, uint64_t file_number) {
  std::lock_guard<std::mutex> lock(region_mutex_);
  if (region_id < 0 || region_id >= kNumRegions) return;
  Region& r = regions_[region_id];
  auto it = std::remove_if(r.region_metas.begin(), r.region_metas.end(),
                           [file_number](const FileMetaData& m) { return m.number == file_number; });
  if (it != r.region_metas.end()) {
    r.region_metas.erase(it, r.region_metas.end());
    r.region_num_sst = r.region_metas.size();
  }
}

static bool ExtractNumericSuffixFromSlice(const Slice& key, uint64_t* number) {
  if (number == nullptr) return false;

  const char* data = key.data();
  size_t n = key.size();

  if (n == 0) return false;

  // 从末尾向前找连续数字。
  // 支持：
  // 0000000040000159
  // user0000000040000159
  // user40000159
  size_t start = n;
  while (start > 0) {
    unsigned char c = static_cast<unsigned char>(data[start - 1]);
    if (c < '0' || c > '9') break;
    start--;
  }

  if (start == n) {
    return false;
  }

  uint64_t value = 0;
  for (size_t i = start; i < n; ++i) {
    unsigned char c = static_cast<unsigned char>(data[i]);
    value = value * 10 + static_cast<uint64_t>(c - '0');
  }

  *number = value;
  return true;
}

int RegionMetaManager::FindRegionContainingKey(const Slice& key) const {
  Slice user_key = key;

  // 兼容两种情况：
  // 1. key 是 InternalKey
  // 2. key 是纯 user key
  ParsedInternalKey ikey;
  if (ParseInternalKey(key, &ikey)) {
    user_key = ikey.user_key;
  }

  uint64_t key_id = 0;
  if (!ExtractNumericSuffixFromSlice(user_key, &key_id)) {
    fprintf(stderr,
            "[Region] Failed to extract numeric suffix from key=%s\n",
            key.ToString().c_str());
    fflush(stderr);
    return -1;
  }

  auto UserKeyToUint64 = [](const Slice& k, uint64_t* value) -> bool {
    if (k.empty()) {
      return false;
    }

    uint64_t v = 0;
    for (size_t i = 0; i < k.size(); ++i) {
      const char ch = k.data()[i];
      if (ch < '0' || ch > '9') {
        return false;
      }
      v = v * 10 + static_cast<uint64_t>(ch - '0');
    }

    *value = v;
    return true;
  };

  std::lock_guard<std::mutex> lock(region_mutex_);

  for (size_t i = 1; i < regions_.size(); ++i) {
    const Region& r = regions_[i];

    if (r.region_id <= 0) {
      continue;
    }

    uint64_t region_min = 0;
    uint64_t region_max = 0;

    if (!UserKeyToUint64(r.region_smallest.user_key(), &region_min) ||
        !UserKeyToUint64(r.region_largest.user_key(), &region_max)) {
      continue;
    }

    if (key_id >= region_min && key_id <= region_max) {
      return r.region_id;
    }
  }

  fprintf(stderr,
          "[Region] key=%s key_id=%llu does not belong to any dynamic region, "
          "regions_size=%zu\n",
          user_key.ToString().c_str(),
          static_cast<unsigned long long>(key_id),
          regions_.size());
  fflush(stderr);

  return -1;
}

// ----- 辅助方法：键与整数的转换 -----
uint32_t RegionMetaManager::KeyToUint32(const InternalKey& key) {
  Slice ukey = key.user_key();
  // 假设用户键是十进制数字字符串，前面补零到16位
  std::string str(ukey.data(), ukey.size());
  return std::stoul(str);
}

void RegionMetaManager::adjustLeft(Region& prev, Region& curr, uint32_t split_key) {
  char new_largest[32];
  std::snprintf(new_largest, sizeof(new_largest), "%016u", split_key);
  prev.region_largest = InternalKey(new_largest, 1, kTypeValue);

  char new_smallest[32];
  std::snprintf(new_smallest, sizeof(new_smallest), "%016u", split_key + 1);
  curr.region_smallest = InternalKey(new_smallest, 1, kTypeValue);
}

void RegionMetaManager::adjustRight(Region& curr, Region& next, uint32_t split_key) {
  char new_largest[32];
  std::snprintf(new_largest, sizeof(new_largest), "%016u", split_key);
  curr.region_largest = InternalKey(new_largest, 1, kTypeValue);

  char new_smallest[32];
  std::snprintf(new_smallest, sizeof(new_smallest), "%016u", split_key + 1);
  next.region_smallest = InternalKey(new_smallest, 1, kTypeValue);
}

void RegionMetaManager::decideAdjustDirection(Region& curr, Region& next,
                                              int level,
                                              uint32_t split_near_start,
                                              uint32_t split_near_end) {
  Region& prev = GetRegion(level-1);
  uint64_t prev_size = 0, next_size = 0;
  for (const auto& m : prev.region_metas) prev_size += m.file_size;
  for (const auto& m : next.region_metas) next_size += m.file_size;

  if (prev_size >= next_size) {
    adjustRight(curr, next, split_near_end);
    is_right_adjust_ = true;
  } else {
    adjustLeft(prev, curr, split_near_start);
    is_left_adjust_ = true;
  }
}

void RegionMetaManager::AdjustEmptyRegion(Region& region, Region& next_region, int level) {
  uint32_t start = KeyToUint32(region.region_smallest);
  uint32_t end   = KeyToUint32(region.region_largest);
  uint32_t split_near_start = start + (end - start) * 1/4;
  uint32_t split_near_end   = start + (end - start) * 3/4;

  if (level == 1) {
    adjustRight(region, next_region, split_near_end);
    is_right_adjust_ = true;
  } else if (level == kNumRegions-1) {
    Region& prev = GetRegion(level-1);
    adjustLeft(prev, region, split_near_start);
    is_left_adjust_ = true;
  } else {
    decideAdjustDirection(region, next_region, level,
                          split_near_start, split_near_end);
  }
}

void RegionMetaManager::AdjustNonEmptyRegion(Region& region, Region& next_region) {
  if (region.region_metas.empty()) return;
  // 找到区域内所有文件的最大键
  InternalKey max_key = region.region_metas[0].largest;
  for (size_t i = 1; i < region.region_metas.size(); ++i) {
    if (region.region_metas[i].largest.user_key().compare(max_key.user_key()) > 0)
      max_key = region.region_metas[i].largest;
  }
  if (max_key.user_key().compare(region.region_largest.user_key()) == 0) return;

  uint32_t curr_max = KeyToUint32(max_key);
  uint32_t region_max = KeyToUint32(region.region_largest);
  uint32_t split_key = (curr_max + region_max) / 2;
  adjustRight(region, next_region, split_key);
}

void RegionMetaManager::AdjustKeyRangeInRegion(int level) {
  if (level <= 0 || level >= kNumRegions-1) return;
  std::lock_guard<std::mutex> lock(region_mutex_);
  is_left_adjust_ = is_right_adjust_ = false;

  Region& region = GetRegion(level);
  Region& next   = GetRegion(level+1);

  if (region.IsEmpty())
    AdjustEmptyRegion(region, next, level);
  else
    AdjustNonEmptyRegion(region, next);
}

void RegionMetaManager::AdjustKeyRangeInRegionAsync(int level) {
  std::thread([this, level]() {
    try {
      AdjustKeyRangeInRegion(level);
    } catch (const std::exception& e) {
      std::cerr << "Region adjustment failed: " << e.what() << std::endl;
    }
  }).detach();
}

Region& RegionMetaManager::GetRegion(int level) {
  // 调用者需保证 level 有效
  return regions_[level];
}

const Region& RegionMetaManager::GetRegion(int level) const {
  return regions_[level];
}

void RegionMetaManager::PrintRegionInfo() const {
  std::lock_guard<std::mutex> lock(region_mutex_);
  for (int i = 0; i < kNumRegions; ++i) {
    const Region& r = regions_[i];
    if (r.region_id == -1) continue;
    std::cout << "Region " << i << ": ["
              << r.region_smallest.user_key().ToString() << " .. "
              << r.region_largest.user_key().ToString()
              << "] " << r.region_num_sst << " files\n";
  }
}


}  // namespace leveldb