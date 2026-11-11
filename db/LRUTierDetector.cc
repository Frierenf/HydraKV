//
// Created by wyy on 2025/12/2.
//
//
// Created by wyy on 2025/12/1.
//
//
// Created by wyy on 2025/11/20.
//
// lru_tier_detector.cpp

#include "LRUTierDetector.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <util/coding.h>
namespace  leveldb {
configs configs_;
LRUTierDetector key_detector;
LRUTierDetector::LRUTierDetector()
    : config_(configs_),current_time_(0),current_memory_usage_(0),current_run(false) {
  // 计算容量（基于4字节键和16字节记录）
  size_t record_size = KeyRecord::MEMORY_SIZE;
  size_t max_records = config_.max_memory_bytes / record_size;

  // 设置容量范围限制
  min_hot_capacity_ = max_records / 10;    // 最小10%
  max_hot_capacity_ = max_records / 2;     // 最大50%
  min_cold_capacity_ = max_records / 2;    // 最小50%
  max_cold_capacity_ = max_records - min_hot_capacity_; //最大90%

  // 分配容量给热表和冷表
  hot_tier_.capacity = max_records / 10;
  cold_tier_.capacity = max_records - hot_tier_.capacity;

  std::cout << "LRU Tier Detector initialized:" << std::endl;
  std::cout << "  Total memory: " << (config_.max_memory_bytes / (1024 * 1024)) << "MB" << std::endl;
  std::cout << "  Hot tier capacity: " << hot_tier_.capacity << " keys ("
            << (hot_tier_.capacity * record_size / 1024) << "KB)" << std::endl;
  std::cout << "  Cold tier capacity: " << cold_tier_.capacity << " keys ("
            << (cold_tier_.capacity * record_size / 1024) << "KB)" << std::endl;
  std::cout << "  Record size: " << record_size << " bytes" << std::endl;
}

LRUTierDetector::~LRUTierDetector() {
  Clear();
}

void LRUTierDetector::RecordAccess(const std::string& key_str) {
  GetTierAndRecordAccess(key_str);
}
MemoryTier LRUTierDetector::GetTierAndRecordAccess(const std::string& key_str) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  uint32_t key = fast_stoi(key_str.data());
  current_time_ = GetCurrentTime();
  total_access_count_++;
  ++window_access_count_;

  // 查找键是否存在
  KeyRecord* record = nullptr;
  MemoryTier found_tier = No_Found;

  // 先在热表中查找
  auto hot_it = hot_tier_.key_map.find(key);
  if (hot_it != hot_tier_.key_map.end()) {
    record = *(hot_it->second);
    found_tier = HOT_TIER;
    hits_++;
    hot_tier_.access_count++;
    ++window_hot_access_count_;
  } else {
    // 在冷表中查找
    auto cold_it = cold_tier_.key_map.find(key);
    if (cold_it != cold_tier_.key_map.end()) {
      record = *(cold_it->second);
      found_tier = COLD_TIER;
      hits_++;
      cold_tier_.access_count++;
    } else {
      found_tier = No_Found;
      misses_++;
    }
  }

  if (record != nullptr) {
    // 更新现有记录
    UpdateKeyRecord(record);

    // 移动到对应层级的LRU头部
    if (found_tier == HOT_TIER) {
      MoveToFront(hot_tier_, record);
    } else {
      MoveToFront(cold_tier_, record);
      // Promote immediately when this access reaches a hotness threshold.
      if(config_.auto_migration && CheckPromotionConditions(record)) {
        PromoteToHot(key);
      }
    }
  } else {
    // 创建新记录（默认在冷表）
    record = CreateRecord(key);
    if (record != nullptr) {
      // 如果冷表已满，先淘汰一个
      if (cold_tier_.is_full()) {
        EvictFromColdTier();
      }
      // 添加新记录
      cold_tier_.lru_list.push_front(record);
      cold_tier_.key_map[key] = cold_tier_.lru_list.begin();
      cold_tier_.memory_usage += KeyRecord::MEMORY_SIZE;
      current_memory_usage_ += KeyRecord::MEMORY_SIZE;
    }
  }
  if (MaybeRunMaintenance()) {
    // Maintenance can move or evict records; look up the final tier without
    // dereferencing a record that may have been freed.
    if (hot_tier_.key_map.find(key) != hot_tier_.key_map.end()) {
      return HOT_TIER;
    }
    return cold_tier_.key_map.find(key) != cold_tier_.key_map.end()
               ? COLD_TIER : No_Found;
  }
  return record != nullptr ? static_cast<MemoryTier>(record->tier) : No_Found;
}
void LRUTierDetector::MayBeUpdateStatistics() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  MaybeRunMaintenance();
}

bool LRUTierDetector::MaybeRunMaintenance() {
  // Both reads and writes reach this path. Zero disables automatic maintenance.
  if (config_.maintenance_interval != 0 &&
      window_access_count_ >= config_.maintenance_interval) {
    Maintenance();
    return true;
  }
  return false;
}
MemoryTier LRUTierDetector::GetKeyTier(const std::string& key_str) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  uint32_t key = fast_stoi(key_str.data());
  if (hot_tier_.key_map.find(key) != hot_tier_.key_map.end()) {
    return HOT_TIER;
  } /*else if (cold_tier_.key_map.find(key) != cold_tier_.key_map.end()) {
    return COLD_TIER;
  }*/ else {
     return COLD_TIER; // 默认返回冷表
  }

}

bool LRUTierDetector::ShouldPromoteToHot(std::string key_str) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  uint32_t key = fast_stoi(key_str.data());
  auto cold_it = cold_tier_.key_map.find(key);
  if (cold_it == cold_tier_.key_map.end()) {
    return false; // 键不在冷表中
  }

  KeyRecord* record = *(cold_it->second);
  return CheckPromotionConditions(record);
}

bool LRUTierDetector::ShouldDemoteToCold(std::string key_str) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  uint32_t key = fast_stoi(key_str.data());
  auto hot_it = hot_tier_.key_map.find(key);
  if (hot_it == hot_tier_.key_map.end()) {
    return false; // 键不在热表中
  }

  KeyRecord* record = *(hot_it->second);
  return CheckDemotionConditions(record);
}
bool LRUTierDetector::PromoteToHot(uint32_t key) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  auto cold_it = cold_tier_.key_map.find(key);
  if (cold_it == cold_tier_.key_map.end()) {
    return false;
  }

  KeyRecord* record = *(cold_it->second);

  // 如果热表已满，需要先淘汰
  if (hot_tier_.is_full()) {
    // 淘汰热表中最旧的记录到冷表
    if (!hot_tier_.lru_list.empty()) {
      KeyRecord* old_record = hot_tier_.lru_list.back();
      MigrateKey(old_record, HOT_TIER, COLD_TIER);
    }
  }

  // 迁移键到热表
  MigrateKey(record, COLD_TIER, HOT_TIER);
  promotions_++;

  return true;
}

bool LRUTierDetector::DemoteToCold(uint32_t key) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  auto hot_it = hot_tier_.key_map.find(key);
  if (hot_it == hot_tier_.key_map.end()) {
    return false;
  }

  KeyRecord* record = *(hot_it->second);

  // 如果冷表已满，需要先淘汰
  if (cold_tier_.is_full()) {
    EvictFromColdTier();
  }

  // 迁移键到冷表
  MigrateKey(record, HOT_TIER, COLD_TIER);
  demotions_++;

  return true;
}

std::vector<uint32_t> LRUTierDetector::GetPromotionCandidates(size_t max_count) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  std::vector<uint32_t> candidates;


  // 从冷表头部开始（最近访问的）
  for (auto it = cold_tier_.lru_list.begin();
       it != cold_tier_.lru_list.end() && candidates.size() < max_count;
       ++it) {
    KeyRecord* record = *it;
    if (CheckPromotionConditions(record)) {
      candidates.push_back(record->key);
    }
       }

  return candidates;
}

std::vector<uint32_t> LRUTierDetector::GetDemotionCandidates(size_t max_count) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  std::vector<uint32_t> candidates;
  // 从热表尾部开始（最久未访问的）
  for (auto it = hot_tier_.lru_list.rbegin();
       it != hot_tier_.lru_list.rend() && candidates.size() < max_count;
       ++it) {
    KeyRecord* record = *it;
    if (CheckDemotionConditions(record)) {
      candidates.push_back(record->key);
    }

       }

  return candidates;
}

std::vector<uint32_t> LRUTierDetector::GetHotTierKeys(size_t max_count) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  std::vector<uint32_t> hot_keys;

  for (auto it = hot_tier_.lru_list.begin();
       it != hot_tier_.lru_list.end() && hot_keys.size() < max_count;
       ++it) {
    hot_keys.push_back((*it)->key);
       }

  return hot_keys;
}

std::vector<uint32_t> LRUTierDetector::GetColdTierKeys(size_t max_count) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  std::vector<uint32_t> cold_keys;

  for (auto it = cold_tier_.lru_list.begin();
       it != cold_tier_.lru_list.end() && cold_keys.size() < max_count;
       ++it) {
    cold_keys.push_back((*it)->key);
       }

  return cold_keys;
}

void LRUTierDetector::RemoveKey(uint32_t key) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  // 在热表中查找
  auto hot_it = hot_tier_.key_map.find(key);
  if (hot_it != hot_tier_.key_map.end()) {
    KeyRecord* record = *(hot_it->second);
    hot_tier_.lru_list.erase(hot_it->second);
    hot_tier_.key_map.erase(hot_it);
    hot_tier_.memory_usage -= KeyRecord::MEMORY_SIZE;
    DeleteRecord(record);
    return;
  }

  // 在冷表中查找
  auto cold_it = cold_tier_.key_map.find(key);
  if (cold_it != cold_tier_.key_map.end()) {
    KeyRecord* record = *(cold_it->second);
    cold_tier_.lru_list.erase(cold_it->second);
    cold_tier_.key_map.erase(cold_it);
    cold_tier_.memory_usage -= KeyRecord::MEMORY_SIZE;
    DeleteRecord(record);
  }
}

bool LRUTierDetector::ContainsKey(uint32_t key) const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  return (hot_tier_.key_map.find(key) != hot_tier_.key_map.end()) ||
         (cold_tier_.key_map.find(key) != cold_tier_.key_map.end());
}
void LRUTierDetector::DynamicCapacityAdjustment() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  if (window_access_count_ == 0) return;

  const double hot_hit_rate = static_cast<double>(window_hot_access_count_) /
                             static_cast<double>(window_access_count_);

  // 只调整容量数值，不执行迁移
  if (hot_hit_rate >= 0.75 && hot_tier_.capacity < max_hot_capacity_) {
    size_t increase = std::min(hot_tier_.capacity / 20, max_hot_capacity_ - hot_tier_.capacity);
    if (increase > 0) {
      hot_tier_.capacity += increase;
      cold_tier_.capacity -= increase;
      std::cout << "Increased hot tier capacity to " << hot_tier_.capacity << std::endl;
    }
  }
  else if (hot_hit_rate <= 0.25 && hot_tier_.capacity > min_hot_capacity_) {
    size_t decrease = std::min(hot_tier_.capacity / 20, hot_tier_.capacity - min_hot_capacity_);
    if (decrease > 0) {
      hot_tier_.capacity -= decrease;
      cold_tier_.capacity += decrease;
      std::cout << "Decreased hot tier capacity to " << hot_tier_.capacity << std::endl;
    }
  }
}
void LRUTierDetector::Maintenance() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  current_time_ = GetCurrentTime();

  DynamicCapacityAdjustment();
  window_access_count_ = 0;
  window_hot_access_count_ = 0;

  if (config_.auto_migration) {
    AutoMigration();
  }

  // UpdateStatistics();
}

void LRUTierDetector::Clear() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  // 清空热表
  for (auto& record : hot_tier_.lru_list) {
    DeleteRecord(record);
  }
  hot_tier_.lru_list.clear();
  hot_tier_.key_map.clear();
  hot_tier_.memory_usage = 0;

  // 清空冷表
  for (auto& record : cold_tier_.lru_list) {
    DeleteRecord(record);
  }
  cold_tier_.lru_list.clear();
  cold_tier_.key_map.clear();
  cold_tier_.memory_usage = 0;

  current_memory_usage_ = 0;
  total_access_count_ = 0;
  window_access_count_ = 0;
  window_hot_access_count_ = 0;
  hot_tier_.access_count = 0;
  cold_tier_.access_count = 0;
  promotions_ = 0;
  demotions_ = 0;
  hits_ = 0;
  misses_ = 0;
}

Stats LRUTierDetector::GetStats() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  Stats stats;

  stats.total_keys = hot_tier_.size() + cold_tier_.size();
  stats.hot_tier_keys = hot_tier_.size();
  stats.cold_tier_keys = cold_tier_.size();
  stats.memory_usage_bytes = current_memory_usage_;
  stats.total_access_count = total_access_count_.load();
  stats.tier_promotions = promotions_.load();
  stats.tier_demotions = demotions_.load();
  stats.hit_count = hits_.load();
  stats.miss_count = misses_.load();

  if (total_access_count_ > 0) {
    uint64_t hot_access = hot_tier_.access_count.load();
    uint64_t cold_access = cold_tier_.access_count.load();
    stats.hot_tier_hit_ratio = static_cast<double>(hot_access) / total_access_count_;
    stats.cold_tier_hit_ratio = static_cast<double>(cold_access) / total_access_count_;
  }

  return stats;
}
void LRUTierDetector::UpdateConfig(const configs& new_config) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  config_ = new_config;
  window_access_count_ = 0;
  window_hot_access_count_ = 0;

  // 重新计算容量
  size_t record_size = KeyRecord::MEMORY_SIZE;
  size_t max_records = config_.max_memory_bytes / record_size;
  hot_tier_.capacity = std::min(config_.hot_tier_capacity, max_records / 10);
  cold_tier_.capacity = max_records - hot_tier_.capacity;
}

// 私有方法实现
uint32_t LRUTierDetector::GetCurrentTime() const {
  auto now = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
}

void LRUTierDetector::UpdateKeyRecord(KeyRecord* record) {
  if (record->access_count < (std::numeric_limits<uint16_t>::max)()) {
    ++record->access_count;
  }

  // 更新连续访问计数
  if (record->last_access_time == current_time_) {
    record->consecutive_access = std::min(record->consecutive_access + 1, 127);
  } else {
    record->consecutive_access = 1;
  }
  record->last_access_time = current_time_;
}

void LRUTierDetector::MoveToFront(TierList& tier, KeyRecord* record) {
  tier.lru_list.splice(tier.lru_list.begin(), tier.lru_list, tier.key_map[record->key]);
  //tier.access_count++;
}

LRUTierDetector::KeyRecord* LRUTierDetector::CreateRecord(uint32_t key) {
  return new KeyRecord(key, current_time_);
}

void LRUTierDetector::DeleteRecord(KeyRecord* record) {
  delete record;
  current_memory_usage_ -= KeyRecord::MEMORY_SIZE;
}

void LRUTierDetector::EvictFromColdTier() {
  if (cold_tier_.lru_list.empty()) return;
  // 淘汰冷表中最旧的记录
  KeyRecord* record = cold_tier_.lru_list.back();
  cold_tier_.lru_list.pop_back();
  cold_tier_.key_map.erase(record->key);
  cold_tier_.memory_usage -= KeyRecord::MEMORY_SIZE;
  DeleteRecord(record);
}

void LRUTierDetector::MigrateKey(KeyRecord* record, MemoryTier from_tier, MemoryTier to_tier) {
  TierList& from_list = (from_tier == HOT_TIER) ? hot_tier_ : cold_tier_;
  TierList& to_list = (to_tier == HOT_TIER) ? hot_tier_ : cold_tier_;

  // 从原层级移除
  auto it = from_list.key_map.find(record->key);
  if (it != from_list.key_map.end()) {
    from_list.lru_list.erase(it->second);
    from_list.key_map.erase(it);
    from_list.memory_usage -= KeyRecord::MEMORY_SIZE;
  }

  // 添加到新层级
  record->tier = to_tier;
  if (to_tier == HOT_TIER) {
    record->promote_time = current_time_;
  }

  to_list.lru_list.push_front(record);
  to_list.key_map[record->key] = to_list.lru_list.begin();
  to_list.memory_usage += KeyRecord::MEMORY_SIZE;
}

bool LRUTierDetector::CheckPromotionConditions(const KeyRecord* record) const {
  // 访问次数达到阈值
  if (record->access_count >= config_.access_threshold) {
    return true;
  }

  // 连续访问次数较多
  if (record->consecutive_access >= 3) {
    return true;
  }

  // 最近频繁访问
  uint32_t time_diff = current_time_ - record->last_access_time;
  if (time_diff < 60 && record->access_count >= 4) { // 1分钟内访问4次
    return true;
  }

  return false;
}

bool LRUTierDetector::CheckDemotionConditions(const KeyRecord* record) const {
  // 在热表中时间较长但访问较少
  uint32_t time_in_hot = current_time_ - record->promote_time;
  if (time_in_hot > config_.demotion_age && record->access_count <= 2) {
    return true;
  }

  // 长期未访问
  uint32_t time_since_access = current_time_ - record->last_access_time;
  if (time_since_access > config_.demotion_age * 2) {
    return true;
  }

  return false;
}

void LRUTierDetector::AutoMigration() {
  // 1. 先处理热表超容：降级到冷表
  while (hot_tier_.size() > hot_tier_.capacity && !hot_tier_.lru_list.empty()) {
    KeyRecord* record = hot_tier_.lru_list.back();
    DemoteToCold(record->key);  // 内部会检查容量
  }

  // 2. 处理冷表超容：直接淘汰
  while (cold_tier_.size() > cold_tier_.capacity && !cold_tier_.lru_list.empty()) {
    KeyRecord* record = cold_tier_.lru_list.back();
    RemoveKey(record->key);
  }

  // 3. 自动提升符合条件的冷表键
  auto promotion_candidates = GetPromotionCandidates(30);
  for (uint32_t key : promotion_candidates) {
    PromoteToHot(key);  // 内部会检查热表容量
  }

  // 4. 自动降级符合条件的热表键
  auto demotion_candidates = GetDemotionCandidates(30);
  for (uint32_t key : demotion_candidates) {
    DemoteToCold(key);  // 内部会检查冷表容量
  }
}
void LRUTierDetector::PrintStats() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  Stats stats = GetStats();
  std::cout << "=========================================" << std::endl;
  std::cout << "=== LRU Tier Detector Statistics ===" << std::endl;
  std::cout << "Total keys: " << stats.total_keys
            << " (Hot: " << stats.hot_tier_keys
            << ", Cold: " << stats.cold_tier_keys << ")" << std::endl;
  std::cout << "Memory usage: " << (stats.memory_usage_bytes / 1048576) << "MB" << std::endl;
  std::cout << "Total accesses: " << stats.total_access_count << std::endl;
  std::cout << "Hit ratio: " << (stats.hit_count * 100.0 / (stats.hit_count + stats.miss_count)) << "%" << std::endl;
  std::cout << "Hot tier hit ratio: " << (stats.hot_tier_hit_ratio * 100) << "%" << std::endl;
  std::cout << "Cold tier hit ratio: " << (stats.cold_tier_hit_ratio * 100) << "%" << std::endl;
  std::cout << "Promotions: " << stats.tier_promotions << std::endl;
  std::cout << "Demotions: " << stats.tier_demotions << std::endl;
  std::cout << "Hot tier usage: " << (stats.hot_tier_keys * 100.0 / hot_tier_.capacity) << "%" << std::endl;
  std::cout << "Cold tier usage: " << (stats.cold_tier_keys * 100.0 / cold_tier_.capacity) << "%" << std::endl;
}
}
