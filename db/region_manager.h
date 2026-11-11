//
// Created by user on 2026/5/23.
//

// region_manager.h
#ifndef STORAGE_LEVELDB_DB_REGION_MANAGER_H_
#define STORAGE_LEVELDB_DB_REGION_MANAGER_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "leveldb/slice.h"
#include <atomic>
#include <mutex>
#include <vector>
#include <unordered_map>

namespace leveldb {

// 前向声明，FileMetaData 定义在 version_edit.h 中
class FileMetaData;

// 每个 Region 的元数据（键范围 + 归属的文件列表）
class Region {
 public:
  int region_id;
  InternalKey region_smallest;
  InternalKey region_largest;
  uint32_t region_num_sst;
  std::vector<FileMetaData> region_metas;

  Region() : region_id(-1), region_num_sst(0) {}
  void AddFileMeta(const FileMetaData& meta);
  void Clear();
  bool IsEmpty() const;
  uint32_t GetFileCount() const;
};

// Region 管理器（全局单例）
class RegionMetaManager {
 public:
  int NumRegions() const;
  RegionMetaManager();

  bool AdjustRegionBoundaryForCompaction(int region_id,
                                         bool shrink_right,
                                         const InternalKey& boundary_key);

  void RemoveFileFromAllRegions(uint64_t file_number);
  int FindRegionByFileNumber(uint64_t file_number) const;
  // 普通 flush 使用 (B 版本中 L0 不调用此方法)
  void UpdateRegionMetaforFlush(int level, const InternalKey& smallest,
                                const InternalKey& largest,
                                const FileMetaData& meta);

  // Compaction 输出时使用
  void UpdateRegionMetaforCompaction(int region_id, const FileMetaData& meta);

  // 删除文件时从 region 中移除
  void RemoveFileInRegion(int region_id, uint64_t file_number);

  // 查找给定 internal key 所属的 region_id
  int FindRegionContainingKey(const Slice& internal_key) const;

  // 动态调整 region 边界（同步 + 异步）
  void AdjustKeyRangeInRegionAsync(int level);
  void AdjustKeyRangeInRegion(int level);

  // 获取 region（不加锁，调用者需确保线程安全）
  Region& GetRegion(int level);
  const Region& GetRegion(int level) const;

  bool GetLeftAdjustStatus() const { return is_left_adjust_; }
  bool GetRightAdjustStatus() const { return is_right_adjust_; }

  void PrintRegionInfo() const;

 private:
  std::vector<Region> regions_;
  mutable std::mutex region_mutex_;     // 保护 regions_
  bool is_left_adjust_;
  bool is_right_adjust_;

  // 调整辅助函数
  void adjustLeft(Region& prev, Region& curr, uint32_t split_key);
  void adjustRight(Region& curr, Region& next, uint32_t split_key);
  void decideAdjustDirection(Region& curr, Region& next, int level,
                             uint32_t split_near_start, uint32_t split_near_end);
  void AdjustEmptyRegion(Region& region, Region& next_region, int level);
  void AdjustNonEmptyRegion(Region& region, Region& next_region);

  // 将 InternalKey 转为 uint32_t（键必须是十进制整数）
  static uint32_t KeyToUint32(const InternalKey& key);
};

extern RegionMetaManager g_region_meta_manager;

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_REGION_MANAGER_H_
