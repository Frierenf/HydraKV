//
// Created by wyy on 2025/12/2.
//

#ifndef LRUTIERDETECTOR_H
#define LRUTIERDETECTOR_H
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <list>
#include <mutex>
#include <chrono>
#include <atomic>
#include <iostream>
namespace  leveldb {
// 配置参数
struct configs {
    size_t max_memory_bytes;              // 64MB总内存
    size_t hot_tier_capacity ;            // 热表键容量(约1.5MB)
    size_t cold_tier_capacity;            // 冷表键容量(约62.5MB)
    uint32_t access_threshold;            // 提升到热表的访问阈值
    uint32_t demotion_age;                // 降级时间阈值(秒)
    double hot_ratio;                      // 热键比例
    bool auto_migration;                  // 自动迁移
    size_t maintenance_interval;        // 每n次访问
    configs():max_memory_bytes(64*1024*1024),hot_tier_capacity(100000),
                    cold_tier_capacity(4000000),access_threshold(4 ),demotion_age(30),hot_ratio(0.1),
                                    auto_migration(true),maintenance_interval(2000000){}
};
extern configs configs_;
// 存储层级
enum MemoryTier {
    COLD_TIER = 0,    // 冷内存表
    HOT_TIER = 1 ,     // 热内存表
    No_Found= 2
};
// 统计信息
struct Stats {
    size_t total_keys = 0;
    size_t hot_tier_keys = 0;
    size_t cold_tier_keys = 0;
    size_t memory_usage_bytes = 0;
    uint64_t total_access_count = 0;
    uint64_t tier_promotions = 0;
    uint64_t tier_demotions = 0;
    uint64_t hit_count = 0;
    uint64_t miss_count = 0;
    double hot_tier_hit_ratio = 0.0;
    double cold_tier_hit_ratio = 0.0;
};

class LRUTierDetector {
public:

    explicit LRUTierDetector();
    ~LRUTierDetector();

    // 禁止拷贝
    LRUTierDetector(const LRUTierDetector&) = delete;
    LRUTierDetector& operator=(const LRUTierDetector&) = delete;

    // 记录键访问
    void RecordAccess(const std::string& key_str);
    MemoryTier GetTierAndRecordAccess(const std::string& key_str);
    // 获取键的存储层级
    MemoryTier GetKeyTier(const std::string& key_str);
    auto MayBeUpdateStatistics()-> void;
    // 检查键是否应该提升到热表
    bool ShouldPromoteToHot(std::string key_str);

    // 检查键是否应该降级到冷表
    bool ShouldDemoteToCold(std::string key_str);

    // 手动提升键到热表
    bool PromoteToHot(uint32_t key);

    // 手动降级键到冷表
    bool DemoteToCold(uint32_t key);

    // 获取需要提升的键列表
    std::vector<uint32_t> GetPromotionCandidates(size_t max_count = 100);
    void DynamicCapacityAdjustment();

    // 获取需要降级的键列表
    std::vector<uint32_t> GetDemotionCandidates(size_t max_count = 100);

    // 获取热表中的键列表
    std::vector<uint32_t> GetHotTierKeys(size_t max_count = 100);

    // 获取冷表中的键列表
    std::vector<uint32_t> GetColdTierKeys(size_t max_count = 100);

    // 删除键记录
    void RemoveKey(uint32_t key);

    // 检查键是否存在
    bool ContainsKey(uint32_t key) const;

    // 定期维护和层级迁移
    void Maintenance();

    // 清空所有记录
    void Clear();

    // 获取统计信息
    Stats GetStats() const;

    // 打印统计信息
    void PrintStats() const;

    // 动态更新配置
    void UpdateConfig(const configs& new_config);

private:
    // 内部记录结构 - 紧凑设计
    struct KeyRecord {
        uint32_t key;                   // 4 字节
        uint32_t last_access_time;      // 4 字节
        uint32_t promote_time;          // 4 字节
        uint16_t access_count;          // 2 字节
        uint8_t tier;                   // 1字节
        uint8_t consecutive_access;     // 1字节
        //uint8_t padding;            // 1字节

        KeyRecord(uint32_t k, uint32_t current_time)
            : key(k),
              access_count(1),
              last_access_time(current_time),
              promote_time(0),
              tier(COLD_TIER),
              consecutive_access(1){}

        static constexpr size_t MEMORY_SIZE = 16; // 16字节对齐
    };

    // 分层LRU链表
    struct TierList {
        std::list<KeyRecord*> lru_list;  // LRU链表，头部最新，尾部最旧
        std::unordered_map<uint32_t, std::list<KeyRecord*>::iterator> key_map;
        size_t memory_usage = 0;
        size_t capacity = 0;
        std::atomic<uint64_t> access_count{0};

        // 获取链表大小
        size_t size() const { return lru_list.size(); }

        // 检查是否已满
        bool is_full() const { return size() >= capacity; }
    };

    configs config_;
    TierList hot_tier_;    // 热LRU表
    TierList cold_tier_;   // 冷LRU表

    // Public operations call one another during migration and maintenance.
    mutable std::recursive_mutex mutex_;
    std::atomic<uint64_t> total_access_count_{0};
    uint64_t window_access_count_ = 0;
    uint64_t window_hot_access_count_ = 0;
    std::atomic<uint64_t> promotions_{0};
    std::atomic<uint64_t> demotions_{0};
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    uint32_t current_time_ ;
    bool current_run;

    // 内存管理
    size_t current_memory_usage_;

    uint64_t min_hot_capacity_ = 0;    // 最小10%
    uint64_t max_hot_capacity_ = 0;    // 最大50%
    uint64_t min_cold_capacity_ = 0;   // 最小50%
    uint64_t max_cold_capacity_ = 0;   //最大90%
    // 内部方法
    bool MaybeRunMaintenance();
    uint32_t GetCurrentTime() const;
    void UpdateKeyRecord(KeyRecord* record);
    void MoveToFront(TierList& tier, KeyRecord* record);
    KeyRecord* CreateRecord(uint32_t key);
    void DeleteRecord(KeyRecord* record);
    void EvictFromColdTier();
    void MigrateKey(KeyRecord* record, MemoryTier from_tier, MemoryTier to_tier);
    bool CheckPromotionConditions(const KeyRecord* record) const;
    bool CheckDemotionConditions(const KeyRecord* record) const;
    void AutoMigration();
};
extern LRUTierDetector key_detector;
}
#endif //LRUTIERDETECTOR_H
