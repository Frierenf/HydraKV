// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <cstdio>
#include <deque>
#include <set>
#include <string>
//#include "mod/learned_index.h"
#include <mod/Vlog.h>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "db/version_bt.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "leveldb/datatable.h"
#include "db/region_manager.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class Version_sst;
class VersionEdit;
class VersionSet;
class VersionSet_sst;
class Compaction;

class MyStats {
 public:
  uint64_t bytes_read;
  uint64_t bytes_write;
  double bytes_flush_write;
  int value_size_;
  uint32_t hot_cache_count_;
  uint32_t memtable_count_;
  uint32_t pmtable_count_;
  uint32_t disk_count_;
  std::vector<double> compaction_time;
  std::vector<double> compaction_bytes;
  double total_compaction_time;
  double total_compaction_bytes;
  double total_flush_time;
  double total_stall_time;
  double total_compact_to_flush_time;
  double get_mem_total_time;
  double get_disk_total_time;
  double get_total_time;
  uint32_t cold_flush_count;
  uint32_t hot_flush_count;
  uint32_t lru_tier_hot_hit_count_;
  uint32_t lru_tier_cold_hit_count_;
  std::unordered_map<uint32_t,std::unordered_map<uint32_t,uint32_t>> hashtable;
  std::unordered_map<uint32_t,uint32_t> key_set;
  std::vector<uint64_t> dram_heatmap_;
  std::vector<uint64_t> ssd_heatmap_[config::kNumLevels];

  MyStats():bytes_write(0),bytes_read(0),bytes_flush_write(0),value_size_(0),hot_cache_count_(0),memtable_count_(0),pmtable_count_(0),
              disk_count_(0),total_compaction_bytes(0),total_compaction_time(0),
              total_flush_time(0),total_stall_time(0),total_compact_to_flush_time(0),get_mem_total_time(0),get_disk_total_time(0),get_total_time(0),cold_flush_count(0),hot_flush_count(0),
              lru_tier_hot_hit_count_(0),lru_tier_cold_hit_count_(0) {

    dram_heatmap_.resize(5000);
    for(int i = 0 ; i< config::kNumLevels;i++) {
      ssd_heatmap_[i].resize(5000);
    }
  };

  void RecordHashTable(uint32_t level,uint32_t key);

  void PrintStats(size_t done_);

  void Reset() {
    bytes_read = 0;
    bytes_write = 0;
    bytes_flush_write = 0;
  }
};

extern MyStats my_stats;

class DBImpl : public DB {
 public:

    uint64_t mem_stall_time_;
    uint64_t L0_stop_stall_time_;  
    uint64_t l0_slow_tall_time_ ;

  // DBImpl(const Options& options, const std::string& dbname_disk, const std::string& dbname_mem);
 DBImpl(const Options& options, const std::string& dbname_);
  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  bool HaveBalancedDistribution();
  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);


  Version_sst* GetCurrentVersion();
  void ReturnCurrentVersion(Version_sst* version);

 private:
  friend class DB;
  struct CompactionState;
  struct CompactionState_sst;
  struct Writer;

  int GetRegionIdForKey(const Slice& internal_key);
  static const char* PMHotnessName(TBALE_HOTNESS hotness);
  const char* PMCompactionHotnessName(Compaction* c) const;
  std::string PMLevelFilesString() const EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void PMTraceLog(const char* format, ...);
  void PMTraceCheckLevels() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };


    // ssd에 쓰기 

  struct CompactionStattossd {
    int64_t count;
    int64_t files_deleted;
    int64_t files_created;
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
    int64_t total_stalls;

    CompactionStattossd() : count(0), files_deleted(0), files_created(0), micros(0), bytes_read(0), bytes_written(0), total_stalls(0) { }

    void Add(const CompactionStattossd& c) {
      this->count++;
      this->files_deleted += c.files_deleted;
      this->files_created += c.files_created;
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };




  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void deleteObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void CompactHotMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // nvm
  Index* b_index_;
  Status WriteLeveltoSsTable(PMtable* mem, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);    

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  //nvm 용 
  void MaybeScheduleCompaction(int level) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db, int level);
  void BackgroundCall(int level);
  void BackgroundCompaction(int level) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // SSD region compaction uses an independent background entry.
  // It must not share level == config::kNumLevels - 1 with PM -> SSD.
  void MaybeScheduleSstCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWorkSst(void* db, int unused_level);
  void BackgroundCallSst();
  void BackgroundSstCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

Status DoCompactionForLevel(int level) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
Status Compactpmtable(int level) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

//kv separate
adgMod::VLog* vlog;
std::atomic<int> version_count;

void HandleOverflowToSSD() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact,uint64_t smallest_snapshot)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status CompactionToSsd(CompactionState* compact,uint64_t smallest_snapshot)
       EXCLUSIVE_LOCKS_REQUIRED(mutex_);


  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

// ====================== sst
  Status DoCompactionWork_sst(CompactionState_sst* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction_sst(CompactionState_sst* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
 Status FinishCompactionOutputFile(CompactionState_sst* compact, Iterator* input);
 Status InstallCompactionResults_sst(CompactionState_sst* compact)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
 Status OpenCompactionOutputFile(CompactionState_sst* compact);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_;  // options_.comparator == &internal_comparator_
  const bool owns_info_log_;
  const bool owns_cache_;
  const std::string dbname_;
  const std::string dbname_ssd_;

  // table_cache_ provides its own synchronization
  TableCache* const table_cache_;

  // Lock over the persistent DB state.  Non-null iff successfully acquired.
  FileLock* db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  std::atomic<bool> shutting_down_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mutex_);
  log::Writer* log_;
  uint32_t seed_ GUARDED_BY(mutex_);  // For sampling.
  MemTable* hot_mem_;
  MemTable* hot_imm_ GUARDED_BY(mutex_);  // Memtable being compacted
  std::atomic<bool> has_hot_imm_;         // So bg thread can detect non-null imm_

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);


  SnapshotList snapshots_ GUARDED_BY(mutex_);
  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_[config::kNumLevels] GUARDED_BY(mutex_);
  bool background_sst_compaction_scheduled_ GUARDED_BY(mutex_);


  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);
  
 
  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);
  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);


  //n for read pmtable
  VersionSet* const versions_ GUARDED_BY(mutex_);
  // for read sstable
  public:
  VersionSet_sst* const versions_sst GUARDED_BY(mutex_);
  private:
  CompactionStattossd _stats_ [config::kNumLevels] GUARDED_BY(mutex_);

  FILE* pm_trace_file_;
  port::Mutex pm_trace_mu_;
  uint64_t pm_trace_cid_;


};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_

