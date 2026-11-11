// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_bt.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include <algorithm>
#include <atomic>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/global.h"
#include "leveldb/index.h"
#include "leveldb/persistant_pool.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"

#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

#include "LRUTierDetector.h"
#include "mod/Vlog.h"
#include "mod/util.h"
#include "numa.h"
// #include "mod/learned_index.h"

#define FULL_TIERED_COMPACTION 0
#ifndef ENABLE_PM_TRACE
#define ENABLE_PM_TRACE 0
#endif
namespace leveldb {

extern LRUTierDetector key_detector;

const int kNumNonTableCacheFiles = 10;

MyStats my_stats;

void MyStats::PrintStats(size_t done_=1024) {
  //system( "du -m /media/test");
  const char* cmd = "du -b /mnt/ssd";
  FILE * stream = popen(cmd,"r");
  char buff[256];
  float f = 0.0;
  if(fgets(buff,sizeof(buff),stream) != nullptr) {
    sscanf(buff,"%f",&f);
  }
  pclose(stream);

  fprintf(stdout, "flush到磁盘: %.0fMB,\t\t 写入磁盘: %.0fMB,\t\t 写放大: %.2f \n",
          (bytes_flush_write / 1048576.0),
          (total_compaction_bytes/ 1048576.0),
          static_cast<float>(total_compaction_bytes + bytes_flush_write) / static_cast<float>(bytes_flush_write));

  fprintf(stdout,"原始写入磁盘: %.0fMB,\t\t 新写入磁盘量： %.0fMB,\t\t 空间放大： %.2f \n",
          (static_cast<float>((value_size_ + 16 )* done_)/1048576.0),
          f/1048576.0,static_cast<float>(f) / static_cast<float>((value_size_ + 16 )* done_));
  fprintf(stdout,"Hot Cache Hit Count: %d,\t\t MemTable Hit Count： %d,\t\t PMTable Hit Count： %d,\t\t Disk Hit Count： %d \n",
          hot_cache_count_,memtable_count_,pmtable_count_,disk_count_);
  fprintf(stdout,"Total Compaction Time: %2fms,\t\t Total Compaction Bytes： %2fMB \n",
          total_compaction_time,total_compaction_bytes/1048576.0);
  fprintf(stdout,"Compaction 带宽：%2f MB/s\n",
          (total_compaction_bytes/1048576.0)/(total_compaction_time/1000.0));
  fprintf(stdout,"Real compaction time: %2fms \n",total_compaction_time - total_compact_to_flush_time);
  fprintf(stdout,"Total Flush Time: %2fms,\t\t Total Flush Bytes： %2fMB \n",
          total_flush_time+total_compact_to_flush_time,bytes_flush_write/1048576.0);
  fprintf(stdout,"Total stall time: %2fms \n",total_stall_time);
  fprintf(stdout,"Get Total time: %2fus \n",get_total_time);
  fprintf(stdout,"Get MemTable time: %2fus \n",get_mem_total_time);
  fprintf(stdout,"Get Disk time: %2fus \n",get_disk_total_time);
  fprintf(stdout,"Total hot flush count: %d \n",hot_flush_count);
  fprintf(stdout,"Total cold flush count: %d \n",cold_flush_count);
  fprintf(stdout,"Hot tier Hit Count: %d,\t\t Cold tier Hit Count： %d\n",
          lru_tier_hot_hit_count_,lru_tier_cold_hit_count_);
  std::cout<<"*******************search each level count******************"<<std::endl;
  for(auto iter = hashtable.begin(); iter != hashtable.end();iter++) {
    std::cout<<"Level "<<iter->first<<": "<<std::endl;

    std::vector<uint32_t> data_hot_set(12, 0);  // 初始化大小为12，初始值为0
    for (auto it = iter->second.begin(); it != iter->second.end(); it++) {
      switch (it->second) {
        case 0:
          data_hot_set[0]++;
          break;
        case 1:
          data_hot_set[1]++;
          break;
        case 2:
          data_hot_set[2]++;
          break;
        case 3:
          data_hot_set[3]++;
          break;
        case 4:
          data_hot_set[4]++;
          break;
        case 5:
          data_hot_set[5]++;
          break;
        case 6:
          data_hot_set[6]++;
          break;
        case 7:
          data_hot_set[7]++;
          break;
        case 8:
          data_hot_set[8]++;
          break;
        case 9:
          data_hot_set[9]++;
          break;
        case 10:
          data_hot_set[10]++;
          break;
        default:
          data_hot_set[11]++;
          break;  // 默认情况下也需要 break
      }
    }
    for (int i = 0; i < data_hot_set.size(); i++) {
      std::cout << "key出现 " << i << "次的数量：" << data_hot_set[i] << "个" << std::endl;
    }
  }
  std::cout<<"******************* end ******************"<<std::endl;
  std::vector<uint32_t> data_hot_set(12, 0);  // 初始化大小为12，初始值为0
  uint64_t num= 0;
  for(auto it = key_set.begin(); it != key_set.end(); it++) {
    switch (it->second) {
      case 0:
        data_hot_set[0]++;
        break;
      case 1:
        data_hot_set[1]++;
        num++;
        break;
      case 2:
        data_hot_set[2]++;
        break;
      case 3:
        data_hot_set[3]++;
        break;
      case 4:
        data_hot_set[4]++;
        break;
      case 5:
        data_hot_set[5]++;
        break;
      case 6:
        data_hot_set[6]++;
        break;
      case 7:
        data_hot_set[7]++;
        break;
      case 8:
        data_hot_set[8]++;
        break;
      case 9:
        data_hot_set[9]++;
        break;
      case 10:
        data_hot_set[10]++;
        break;
      default:
        data_hot_set[11]++;
        break;  // 默认情况下也需要 break
    }
  }
  for (int i = 0; i < data_hot_set.size(); i++) {
    std::cout << "key出现 " << i << "次的数量：" << data_hot_set[i] << "个" << std::endl;
  }
  std::cout << "key更新1次的数量: " << num <<std::endl;
  std::cout << "key更新2次以上的数量: " << key_set.size() - num <<std::endl;
  std::cout << "key更新占比: " << 1 - (double)num/(double)key_set.size()<<std::endl;
}

void MyStats::RecordHashTable(uint32_t level,uint32_t key) {
  hashtable[level][key]++;
}

// for checking the read

uint64_t mem_hits = 0;
uint64_t imm_hits = 0;
uint64_t PMtable_hits = 0;
uint64_t Btree_hits = 0;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

struct DBImpl::CompactionState_sst {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    PMtable* dt;
    InternalKey smallest, largest;
    int region_id;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState_sst(Compaction_sst* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0),
        target_region_id(-1) {}

  Compaction_sst* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  // 当前 SSD region compaction 调度选中的 region。
  // region_id 表示 key-range 分区，不是 level。
  // 输出文件应直接继承该值，不能再用 level + 1 兜底。
  int target_region_id;
};

struct DBImpl::CompactionState {
  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    PMtable* dt;
    InternalKey smallest, largest;

    TBALE_HOTNESS hotness;
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        trace_cid(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;
  uint64_t trace_cid;

  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);
  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

const char* DBImpl::PMHotnessName(TBALE_HOTNESS hotness) {
  switch (hotness) {
    case HOT_TABLE:
      return "HOT";
    case COLD_TABLE:
      return "COLD";
    default:
      return "UNKNOWN";
  }
}

const char* DBImpl::PMCompactionHotnessName(Compaction* c) const {
  if (c == nullptr || c->num_input_files(0) <= 0) {
    return "UNKNOWN";
  }
  bool has_hot = false;
  bool has_cold = false;
  bool has_unknown = false;
  for (int i = 0; i < c->num_input_files(0); i++) {
    FileMetaData* f = c->input(0, i);
    if (f == nullptr) {
      has_unknown = true;
    } else if (f->hotness == HOT_TABLE) {
      has_hot = true;
    } else if (f->hotness == COLD_TABLE) {
      has_cold = true;
    } else {
      has_unknown = true;
    }
  }
  if (has_hot && has_cold) return "MIXED";
  if (has_hot) return "HOT";
  if (has_cold) return "COLD";
  return has_unknown ? "UNKNOWN" : "UNKNOWN";
}

std::string DBImpl::PMLevelFilesString() const {
  std::string result;
  Version* v = versions_->current();
  char buf[64];
  for (int i = 0; i < config::kNumLevels; i++) {
    std::snprintf(buf, sizeof(buf), "%sL%d=%d", (i == 0 ? "" : " "), i,
                  v->NumFiles(i));
    result.append(buf);
  }
  return result;
}

void DBImpl::PMTraceLog(const char* format, ...) {
#if ENABLE_PM_TRACE
  MutexLock l(&pm_trace_mu_);
  if (pm_trace_file_ == nullptr) return;

  va_list ap;
  va_start(ap, format);
  std::vfprintf(pm_trace_file_, format, ap);
  va_end(ap);

  std::fprintf(pm_trace_file_, "\n");

  // 正式实验不建议每条日志都 fflush，影响性能很大
  // std::fflush(pm_trace_file_);
#else
  (void)format;
#endif
}

#if !ENABLE_PM_TRACE
#define PMTraceLog(...) do {} while (0)
#endif

void DBImpl::PMTraceCheckLevels() {
  mutex_.AssertHeld();
  const int pm_last_level = config::kNumLevels - 2;
  PMTraceLog("[PM->SSD check] kNumLevels=%d pm_last_level=%d",
             config::kNumLevels, pm_last_level);
  for (int physical = 0; physical < config::kNumLevels; physical++) {
    bool needs = false;
    bool scheduled = false;
    int arrival = physical + 1;
    if (physical == 0) {
      needs = (imm_ != nullptr || hot_imm_ != nullptr) ||
              (arrival < config::kNumLevels && versions_->NeedsCompaction(arrival));
      scheduled = background_compaction_scheduled_[0] ||
                  (arrival < config::kNumLevels &&
                   background_compaction_scheduled_[arrival]);
    } else if (physical < config::kNumLevels - 1) {
      needs = versions_->NeedsCompaction(arrival);
      scheduled = background_compaction_scheduled_[arrival];
    }
    PMTraceLog("[PM level] L%d files=%d needs=%d scheduled=%d", physical,
               versions_->current()->NumFiles(physical), needs ? 1 : 0,
               scheduled ? 1 : 0);
  }
}

// DBImpl::DBImpl(const Options& raw_options, const std::string& dbname_disk,
// const std::string& dbname_mem)
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),

      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(raw_options.nvm_option.sst_path,
                               &internal_comparator_, &internal_filter_policy_,
                               raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      // dbname_(dbname),
      dbname_ssd_(raw_options.nvm_option.sst_path),
      dbname_(raw_options.nvm_option.pmem_path),
      mem_stall_time_(0),
      L0_stop_stall_time_(0),
      l0_slow_tall_time_(0),
      table_cache_(
          new TableCache(dbname_ssd_, &options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      hot_mem_(nullptr),
      hot_imm_(nullptr),
      has_hot_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      b_index_(raw_options.index),
      // background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_sst(new VersionSet_sst(dbname_ssd_, &options_, table_cache_,
                                      &internal_comparator_)),
      versions_(new VersionSet(dbname_, &options_, &internal_comparator_)),
      pm_trace_file_(nullptr),
      pm_trace_cid_(0) {

#if ENABLE_PM_TRACE
  std::string pm_trace_path = dbname_ + "/pm_compaction_trace.log";
  pm_trace_file_ = std::fopen(pm_trace_path.c_str(), "a");
  if (pm_trace_file_ != nullptr) {
    std::setvbuf(pm_trace_file_, nullptr, _IOLBF, 0);
    PMTraceLog("[PM-TRACE-OPEN] path=%s pm_dir=%s ssd_dir=%s",
               pm_trace_path.c_str(), dbname_.c_str(), dbname_ssd_.c_str());
  }
#endif

  b_index_ = CreateBtreeIndex();

  for (int i = 0; i < config::kNumLevels; i++) {
    background_compaction_scheduled_[i] = false;
  }
  background_sst_compaction_scheduled_ = false;
  NvmNodeSizeInit(options_);
}

DBImpl::~DBImpl() {
  // Wait for background work to finish.

  // if (adgMod::KV_S >= 1) {
  //   CompactMemTable();

  // }
  // HandleOverflowToSSD();
  //  fprintf(stderr,
  //          "mem_hits %lu, imm_hits %lu, PMtable_hits %lu, B+tree_hits %lu \n",
  //          mem_hits, imm_hits, PMtable_hits, Btree_hits);
  //
  //  NvmUsagePrint();
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);

  // nvram::close_pool();
  // b_index_->Break();
  int counter = 0;
  while (counter != config::kNumLevels) {
    if (background_compaction_scheduled_[counter]) {
      background_work_finished_signal_.Wait();
    } else {
      counter++;
    }
  }
  while (background_sst_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }

  mutex_.Unlock();
#if ENABLE_PM_TRACE
  PMTraceLog("[PM-TRACE-CLOSE]");
  {
    MutexLock l(&pm_trace_mu_);
    if (pm_trace_file_ != nullptr) {
      std::fclose(pm_trace_file_);
      pm_trace_file_ = nullptr;
    }
  }
#endif

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
  if (adgMod::KV_S == 1) {
    delete vlog;
    env_->RemoveFile(dbname_ssd_ + "/vlog.txt");
    delete adgMod::file_data;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_ssd_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_ssd_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::deleteObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_sst->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_ssd_, &filenames);
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;

  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_sst->LogNumber()) ||
                  (number == versions_sst->PrevLogNumber()));
          break;
        case kDescriptorFile:
          keep = (number >= versions_sst->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          table_cache_->Evict(number);

          // 从 Region 管理器中移除该文件的元数据
          g_region_meta_manager.RemoveFileFromAllRegions(number);

          Log(options_.info_log, "Delete type===%d #%lld\n",
              static_cast<int>(type), static_cast<unsigned long long>(number));
        }
      }
    }
  }

  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->DeleteFile(dbname_ssd_ + "/" + filename);
  }
  mutex_.Lock();
}

void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // printf("error in RemoveObsoleteFiles\n");
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_ssd_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  for (std::string& filename : filenames) {
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kCurrentFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        files_to_delete.push_back(std::move(filename));

        if (type == kTableFile) {
          table_cache_->Evict(number);
          // 添加这行：从 Region 管理器中移除该文件
          //g_region_meta_manager.RemoveFileFromAllRegions(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ssd_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_ssd_);
  // recovery test
  // 수정해

  assert(db_lock_ == nullptr);
  Status s = env_->LockFile(LockFileName(dbname_ssd_), &db_lock_);

  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_ssd_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_ssd_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(dbname_ssd_,
                                     "exists (error_if_exists is true)");
    }
  }

  s = versions_sst->Recover(save_manifest);

  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_sst->LogNumber();
  const uint64_t prev_log = versions_sst->PrevLogNumber();
  std::vector<std::string> filenames;

  s = env_->GetChildren(dbname_ssd_, &filenames);

  if (!s.ok()) {
    return s;
  }

  std::set<uint64_t> expected;
  versions_sst->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf,
                              TableFileName(dbname_ssd_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_sst->MarkFileNumberUsed(logs[i]);
  }

  if (versions_sst->LastSequence() < max_sequence) {
    versions_sst->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // ()) the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_,
                         options_.write_buffer_size + 2 * 1024 * 1024);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_,
                            options_.write_buffer_size + 2 * 1024 * 1024);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLeveltoSsTable(PMtable* pt, VersionEdit* edit) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_sst->NewFileNumber();
  pending_outputs_.insert(meta.number);
  PMTraceLog("[PM-TO-SSD-BUILD-BEGIN] sstable=%llu input_mem=%llu",
             (unsigned long long)meta.number,
             (unsigned long long)(pt == nullptr ? 0 : pt->ApproximateMemoryUsage()));
  Iterator* iter = pt->NewIterator();
  // printf("WriteLeveltoSsTable111\n");
  Status s;
  {
    mutex_.Unlock();
    // printf("WriteLeveltoSsTable111\n");
    s = BuildTable(dbname_ssd_, env_, options_, table_cache_, iter, &meta);
    // printf("WriteLeveltoSsTable222\n");
    mutex_.Lock();
  }
  //  printf("WriteLeveltoSsTable222\n");
  Log(options_.info_log, "sstable #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);

  if (s.ok() && meta.file_size > 0) {
    edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
  } else {
    printf("error in WriteLeveltoSsTable\n");
  }
  versions_sst->LogAndApply(edit, &mutex_);

  PMTraceLog("[PM-TO-SSD-BUILD-END] sstable=%llu bytes=%llu cost_ms=%.3f status=%s",
             (unsigned long long)meta.number,
             (unsigned long long)meta.file_size,
             (env_->NowMicros() - start_micros) / 1000.0,
             s.ToString().c_str());

  CompactionStattossd stats;
  stats.count = 1;
  stats.files_created = 1;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  my_stats.bytes_flush_write += meta.file_size;
  _stats_[0].Add(stats);

  return s;
}

// memtable -> pmtable
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit) {
  // printf("WriteLevel0Table\n");
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  const TBALE_HOTNESS flush_hotness = edit->is_hot_table ? HOT_TABLE : COLD_TABLE;
  pending_outputs_.insert(meta.number);
  PMTraceLog("[PM-FLUSH-INNER-BEGIN] temp=%s file=%llu",
             PMHotnessName(flush_hotness),
             (unsigned long long)meta.number);
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();

    PMtable* newdt = new PMtable(internal_comparator_, mem, options_);
    // PMtable* newdt = new PMtable(internal_comparator_, dbname_, mem,
    // options_);
    meta.dt = newdt;

    uint32_t len;
    const char* p = newdt->table_.smallest->key;
    p = GetVarint32Ptr(p, p + 5, &len);
    meta.smallest.DecodeFrom(Slice(p, len));

    p = newdt->table_.largest[0]->key;
    p = GetVarint32Ptr(p, p + 5, &len);
    meta.largest.DecodeFrom(Slice(p, len));

    meta.file_size = newdt->ApproximateMemoryUsage();
    s = Status::OK();
    if (s.ok() && meta.file_size > 0) {
      //      edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest,
      //                    meta.dt);
      if(edit->is_hot_table) {
        edit->AddFile(0, meta.number, meta.file_size, meta.smallest,
                      meta.largest, meta.dt,HOT_TABLE);
      } else {
        edit->AddFile(0, meta.number, meta.file_size, meta.smallest,
                      meta.largest, meta.dt,COLD_TABLE);
      }
    }
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());
  // delete iter;
  pending_outputs_.erase(meta.number);

  PMTraceLog("[PM-FLUSH-INNER-END] temp=%s file=%llu size=%llu cost_ms=%.3f status=%s",
             PMHotnessName(flush_hotness),
             (unsigned long long)meta.number,
             (unsigned long long)meta.file_size,
             (env_->NowMicros() - start_micros) / 1000.0,
             s.ToString().c_str());

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.dt->table_.wa;
  my_stats.bytes_flush_write += meta.dt->table_.wa;
  stats_[0].Add(stats);

  return s;
}
void DBImpl::CompactHotMemTable() {
  mutex_.AssertHeld();
  assert(hot_imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  edit.is_hot_table = true;
  const uint64_t flush_start = env_->NowMicros();
  PMTraceLog("hot memtable flush to PM start!!!!!!!!!");
  PMTraceLog("[PM-FLUSH-BEGIN] temp=HOT %s", PMLevelFilesString().c_str());
  Status s = WriteLevel0Table(hot_imm_, &edit);
  PMTraceLog("Hot MemTable flush:%.3fms status=%s",
             (env_->NowMicros() - flush_start) / 1000.0,
             s.ToString().c_str());
  PMTraceLog("[PM-FLUSH-END] temp=HOT cost_ms=%.3f status=%s %s",
             (env_->NowMicros() - flush_start) / 1000.0,
             s.ToString().c_str(), PMLevelFilesString().c_str());
  edit.is_hot_table = false;

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  /*if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }*/
  s = versions_->LogAndApply(&edit, &mutex_);
  if (s.ok()) {
    // Commit to the new state
    hot_imm_->Unref();
    hot_imm_ = nullptr;
    has_hot_imm_.store(false, std::memory_order_release);
    //RemoveObsoleteFiles();
  } /*else {

    RecordBackgroundError(s);
  }*/
}
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;

  edit.is_hot_table = false;
  const uint64_t flush_start = env_->NowMicros();
  PMTraceLog("cold memtable flush to PM start!!!!!!!!!");
  PMTraceLog("[PM-FLUSH-BEGIN] temp=COLD %s", PMLevelFilesString().c_str());
  Status s = WriteLevel0Table(imm_, &edit);
  PMTraceLog("Cold MemTable flush:%.3fms status=%s",
             (env_->NowMicros() - flush_start) / 1000.0,
             s.ToString().c_str());
  PMTraceLog("[PM-FLUSH-END] temp=COLD cost_ms=%.3f status=%s %s",
             (env_->NowMicros() - flush_start) / 1000.0,
             s.ToString().c_str(), PMLevelFilesString().c_str());

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    // std::cout << "LogAndApply in CompactMemTable" << std::endl;
    s = versions_->LogAndApply(&edit, &mutex_);
  }
  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      for (int i = 0; i < config::kNumLevels; i++) {
        MaybeScheduleCompaction(i);
      }
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

//void DBImpl::RecordBackgroundError(const Status& s) {
//  mutex_.AssertHeld();
//  if (bg_error_.ok()) {
//    bg_error_ = s;
//    background_work_finished_signal_.SignalAll();
//  }
//}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    Log(options_.info_log,
        "[BG-ERROR] %s",
        s.ToString().c_str());

    fprintf(stderr,
            "[BG-ERROR] %s\n",
            s.ToString().c_str());
    fflush(stderr);

    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction(int level) {
  mutex_.AssertHeld();
  PMTraceCheckLevels();

  if (level < 0 || level >= config::kNumLevels) {
    return;
  }

  if (background_compaction_scheduled_[level]) {
    return;
  }

  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (!bg_error_.ok()) {
    return;
  }

  if (level == 0) {
    // MemTable -> PM L0
    if (imm_ == nullptr && hot_imm_ == nullptr) {
      return;
    }
  } else if (level > 0 && level < config::kNumLevels - 1) {
    // PM internal compaction
    if (!versions_->NeedsCompaction(level)) {
      return;
    }
  } else if (level == config::kNumLevels - 1) {
    // PM last level -> SSD
    // 注意：这里仍然只看 versions_，不要看 versions_sst。
    if (!versions_->NeedsCompaction(level)) {
      return;
    }
  } else {
    return;
  }

  background_compaction_scheduled_[level] = true;
  env_->Schedule(&DBImpl::BGWork, this, level);
}

void DBImpl::BGWork(void* db, int level) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall(level);
}

void DBImpl::BackgroundCall(int level) {
  MutexLock l(&mutex_);

  assert(level >= 0 && level < config::kNumLevels);
  assert(background_compaction_scheduled_[level]);

  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction(level);
  }

  background_compaction_scheduled_[level] = false;

  // PM chain reschedule.
  MaybeScheduleCompaction(level);

  if (level + 1 < config::kNumLevels) {
    MaybeScheduleCompaction(level + 1);
  }

  // SSD region compaction is independent and lower priority.
  // If PM still has flush/compaction pressure, this function will return directly.
  MaybeScheduleSstCompaction();

  background_work_finished_signal_.SignalAll();
}

void DBImpl::BackgroundCompaction(int level) {
  mutex_.AssertHeld();

  const uint64_t bg_start = env_->NowMicros();
  const int physical_level = (level > 0) ? level - 1 : 0;

  PMTraceLog("[PM-BG-BEGIN] arrival=%d physical=%d %s",
             level,
             physical_level,
             PMLevelFilesString().c_str());

  if (level == 0 && imm_ != nullptr) {
    // MemTable -> PM L0
    CompactMemTable();

  } else if (level == 0 && hot_imm_ != nullptr) {
    // Hot MemTable -> PM L0
    CompactHotMemTable();

  } else if (level > 0 && level < config::kNumLevels - 1) {
    // ========== PM internal compaction ==========
    PMTraceLog("[PM-SCHEDULED-COMPACT] arrival=%d physical=%d",
               level,
               level - 1);

    uint64_t pm_total_start = env_->NowMicros();

    Compaction* c = versions_->PickCompaction(level);
    Status status;

    if (c == nullptr) {
      Log(options_.info_log,
          "[PMCompaction] level=%d no compaction picked",
          level);
    } else {
      CompactionState* compact = new CompactionState(c);

      if (snapshots_.empty()) {
        compact->smallest_snapshot = versions_->LastSequence();
      } else {
        compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
      }

      std::cout << "Before compaction, level 0 fileNum: "
                << versions_->current()->NumFiles(0) << std::endl;

      SequenceNumber smallest_snapshot = compact->smallest_snapshot;

      uint64_t pm_work_start = env_->NowMicros();
      status = DoCompactionWork(compact, smallest_snapshot);
      uint64_t pm_work_end = env_->NowMicros();

      Log(options_.info_log,
          "[PMCompactionWorkCost] level=%d cost_ms=%.3f status=%s",
          level,
          (pm_work_end - pm_work_start) / 1000.0,
          status.ToString().c_str());

      std::cout << "After compaction, level 0 fileNum: "
                << versions_->current()->NumFiles(0) << std::endl;

      if (!status.ok()) {
        RecordBackgroundError(status);
      }

      uint64_t pm_cleanup_start = env_->NowMicros();
      CleanupCompaction(compact);
      c->ReleaseInputs();
      uint64_t pm_cleanup_end = env_->NowMicros();

      Log(options_.info_log,
          "[PMCompactionCleanupCost] level=%d cost_ms=%.3f",
          level,
          (pm_cleanup_end - pm_cleanup_start) / 1000.0);

      uint64_t obsolete_start = env_->NowMicros();
      RemoveObsoleteFiles();
      uint64_t obsolete_end = env_->NowMicros();

      Log(options_.info_log,
          "[PMRemoveObsoleteCost] level=%d cost_ms=%.3f",
          level,
          (obsolete_end - obsolete_start) / 1000.0);

    }

    delete c;

    Log(options_.info_log,
        "[PMCompactionTotalCost] level=%d total_cost_ms=%.3f",
        level,
        (env_->NowMicros() - pm_total_start) / 1000.0);

    if (!status.ok() && !shutting_down_.load(std::memory_order_acquire)) {
      Log(options_.info_log,
          "Compaction error: %s",
          status.ToString().c_str());
    }

  } else if (level == config::kNumLevels - 1) {
    // ========== PM last level -> SSD ==========
    // 这个分支只负责把 PM 最后一层下刷到 SSD。
    // 不允许在这里做 SSD region compaction。

    uint64_t pm_to_ssd_total_start = env_->NowMicros();

    PMTraceLog("[PM-TO-SSD-SCHEDULED] arrival=%d physical=%d %s",
               level,
               physical_level,
               PMLevelFilesString().c_str());

    Compaction* c = versions_->PickCompaction(level);
    Status status;

    if (c == nullptr) {
      Log(options_.info_log,
          "[PMToSSD] level=%d no compaction picked",
          level);
    } else {
      CompactionState* compact = new CompactionState(c);

      if (snapshots_.empty()) {
        compact->smallest_snapshot = versions_->LastSequence();
      } else {
        compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
      }

      Log(options_.info_log,
          "[PMToSSD] begin level=%d pm_last_level_files=%d ssd_l0_files=%d",
          level,
          versions_->current()->NumFiles(config::kNumLevels - 2),
          versions_sst->current()->NumFiles(0));

      status = CompactionToSsd(compact, compact->smallest_snapshot);

      CleanupCompaction(compact);
      c->ReleaseInputs();

      uint64_t obsolete_start = env_->NowMicros();
      RemoveObsoleteFiles();
      uint64_t obsolete_end = env_->NowMicros();

      Log(options_.info_log,
          "[PMToSSDRemoveObsoleteCost] level=%d cost_ms=%.3f",
          level,
          (obsolete_end - obsolete_start) / 1000.0);

      Log(options_.info_log,
          "[PMToSSD] end level=%d status=%s ssd_l0_files=%d",
          level,
          status.ToString().c_str(),
          versions_sst->current()->NumFiles(0));

      if (!status.ok()) {
        RecordBackgroundError(status);
      }

    }

    delete c;

    Log(options_.info_log,
        "[PMToSSDTotalCost] level=%d total_cost_ms=%.3f",
        level,
        (env_->NowMicros() - pm_to_ssd_total_start) / 1000.0);
  }

  PMTraceLog("[PM-BG-END] arrival=%d physical=%d cost_ms=%.3f %s",
             level,
             physical_level,
             (env_->NowMicros() - bg_start) / 1000.0,
             PMLevelFilesString().c_str());
}
//dairegion
void DBImpl::MaybeScheduleSstCompaction() {
  mutex_.AssertHeld();

  if (background_sst_compaction_scheduled_) {
    return;
  }

  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (!bg_error_.ok()) {
    return;
  }

  if (imm_ != nullptr || hot_imm_ != nullptr) {
    return;
  }

  for (int level = 1; level < config::kNumLevels; ++level) {
    if (versions_->NeedsCompaction(level)) {
      return;
    }
  }

  const bool has_l0_pressure =
      versions_sst->current()->NumFiles(0) >= config::kL0_CompactionTrigger;

  const bool has_region_pressure =
      versions_sst->NeedsCompaction() &&
      !versions_sst->current()->GetActiveRegions().empty();

  if (!has_l0_pressure && !has_region_pressure) {
    return;
  }

  background_sst_compaction_scheduled_ = true;
  env_->Schedule(&DBImpl::BGWorkSst, this, 0);
}
//SSD L1cengzhihoude compaction qudiao
/*void DBImpl::MaybeScheduleSstCompaction() {
  mutex_.AssertHeld();

  if (background_sst_compaction_scheduled_) {
    return;
  }

  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (!bg_error_.ok()) {
    return;
  }

  // PM flush 优先级最高。
  if (imm_ != nullptr || hot_imm_ != nullptr) {
    return;
  }

  // PM 内部 compaction / PM -> SSD 优先级高于 SSD compaction。
  for (int level = 1; level < config::kNumLevels; ++level) {
    if (versions_->NeedsCompaction(level)) {
      return;
    }
  }

  // 实验开关：
  // 只允许 SSD L0 -> L1 region 化 compaction。
  // 不再因为 L1+ Region 有 compaction 压力而调度后台 SSD compaction。
  const bool has_l0_pressure =
      versions_sst->current()->NumFiles(0) >= config::kL0_CompactionTrigger;

  if (!has_l0_pressure) {
    return;
  }

  background_sst_compaction_scheduled_ = true;
  env_->Schedule(&DBImpl::BGWorkSst, this, 0);
}*/
//jingzhiSSD compaction
/*void DBImpl::MaybeScheduleSstCompaction() {
  mutex_.AssertHeld();

  // 实验开关：
  // 禁止 SSD 后台 compaction，包括：
  // 1. SSD L0 -> L1 region 化 compaction
  // 2. Region L1+ compaction
  //
  // 目的：验证 run 阶段 4KB 严重停顿是否来自 SSD L0 -> L1 region 化。
  return;
}*/

void DBImpl::BGWorkSst(void* db, int unused_level) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCallSst();
}
//region&&no SSD L1 zhihoude
/*void DBImpl::BackgroundCallSst() {
  MutexLock l(&mutex_);

  assert(background_sst_compaction_scheduled_);

  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundSstCompaction();
  }

  background_sst_compaction_scheduled_ = false;

  background_work_finished_signal_.SignalAll();
}*/
void DBImpl::BackgroundCallSst() {
  MutexLock l(&mutex_);

  assert(background_sst_compaction_scheduled_);

  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundSstCompaction();
  }

  background_sst_compaction_scheduled_ = false;

  // 注意：
  // 这里不要再调用 MaybeScheduleSstCompaction()。
  // 本实验中 SSD compaction 已经完全禁止，
  // 不能让 L0 文件压力反复唤醒后台线程。
  background_work_finished_signal_.SignalAll();
}
//region
void DBImpl::BackgroundSstCompaction() {
  mutex_.AssertHeld();

  // SSD compaction 的优先级低于 PM flush / PM compaction。
  if (imm_ != nullptr || hot_imm_ != nullptr) {
    return;
  }

  for (int level = 1; level < config::kNumLevels; ++level) {
    if (versions_->NeedsCompaction(level)) {
      return;
    }
  }

  struct WorkloadInfo {
    double read_ratio;
    double write_ratio;
    double theta;
    uint64_t window_read_ops;
    uint64_t window_write_ops;
  };

  auto current_read_ops = [&]() -> uint64_t {
    return static_cast<uint64_t>(my_stats.memtable_count_) +
           static_cast<uint64_t>(my_stats.pmtable_count_) +
           static_cast<uint64_t>(my_stats.disk_count_);
  };

  auto current_write_ops = [&]() -> uint64_t {
    return static_cast<uint64_t>(versions_->LastSequence());
  };

  auto compute_workload_info = [&]() -> WorkloadInfo {
    struct WorkloadSample {
      uint64_t read_ops;
      uint64_t write_ops;
    };

    static const size_t kWorkloadWindowSamples = 8;
    static uint64_t last_read_ops = 0;
    static uint64_t last_write_ops = 0;
    static uint64_t window_read_ops = 0;
    static uint64_t window_write_ops = 0;
    static std::deque<WorkloadSample> workload_window;

    const uint64_t now_read_ops = current_read_ops();
    const uint64_t now_write_ops = current_write_ops();

    uint64_t delta_read_ops =
        now_read_ops >= last_read_ops ? now_read_ops - last_read_ops
                                      : now_read_ops;

    uint64_t delta_write_ops =
        now_write_ops >= last_write_ops ? now_write_ops - last_write_ops
                                        : now_write_ops;

    last_read_ops = now_read_ops;
    last_write_ops = now_write_ops;

    workload_window.push_back({delta_read_ops, delta_write_ops});
    window_read_ops += delta_read_ops;
    window_write_ops += delta_write_ops;

    while (workload_window.size() > kWorkloadWindowSamples) {
      const WorkloadSample& oldest = workload_window.front();
      window_read_ops -= oldest.read_ops;
      window_write_ops -= oldest.write_ops;
      workload_window.pop_front();
    }

    double read_ratio = 0.5;
    double write_ratio = 0.5;

    const uint64_t total_ops = window_read_ops + window_write_ops;
    if (total_ops > 0) {
      read_ratio =
          static_cast<double>(window_read_ops) / static_cast<double>(total_ops);
      write_ratio =
          static_cast<double>(window_write_ops) / static_cast<double>(total_ops);
    }

    // 写比例越大，theta 越高；
    // 读比例越大，theta 越低。
    static const double kMinInvalidRatioThreshold = 0.35;
    static const double kMaxInvalidRatioThreshold = 0.70;

    double theta = kMinInvalidRatioThreshold +
                   (kMaxInvalidRatioThreshold - kMinInvalidRatioThreshold) *
                       write_ratio;

    if (theta < kMinInvalidRatioThreshold) {
      theta = kMinInvalidRatioThreshold;
    }

    if (theta > kMaxInvalidRatioThreshold) {
      theta = kMaxInvalidRatioThreshold;
    }

    WorkloadInfo info;
    info.read_ratio = read_ratio;
    info.write_ratio = write_ratio;
    info.theta = theta;
    info.window_read_ops = window_read_ops;
    info.window_write_ops = window_write_ops;
    return info;
  };

  auto estimate_invalid_ratio = [&](Compaction_sst* c) -> double {
    if (c == nullptr) {
      return 0.0;
    }

    static const uint64_t kMaxSampleKeys = 200000;

    uint64_t total_keys = 0;
    uint64_t unique_keys = 0;

    std::string last_user_key;
    bool has_last_user_key = false;

    Status iter_status;

    mutex_.Unlock();

    Iterator* input = versions_sst->MakeInputIterator(c);
    input->SeekToFirst();

    ParsedInternalKey ikey;

    for (; input->Valid() && total_keys < kMaxSampleKeys; input->Next()) {
      Slice key = input->key();

      if (!ParseInternalKey(key, &ikey)) {
        continue;
      }

      total_keys++;

      if (!has_last_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(last_user_key)) != 0) {
        unique_keys++;
        last_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_last_user_key = true;
      }
    }

    iter_status = input->status();
    delete input;
    input = nullptr;

    mutex_.Lock();

    if (!iter_status.ok()) {
      Log(options_.info_log,
          "[CompactionIR] IR estimation failed: %s",
          iter_status.ToString().c_str());
      return 0.0;
    }

    if (total_keys == 0 || unique_keys == 0) {
      return 0.0;
    }

    double invalid_ratio =
        1.0 - static_cast<double>(unique_keys) /
                  static_cast<double>(total_keys);

    if (invalid_ratio < 0.0) {
      invalid_ratio = 0.0;
    }

    if (invalid_ratio > 1.0) {
      invalid_ratio = 1.0;
    }

    return invalid_ratio;
  };

  const WorkloadInfo workload = compute_workload_info();
  const double theta = workload.theta;

  // ------------------------------------------------------------
  // 1. SSD L0 -> L1 region 化 compaction。
  //    L0 同样需要 IR >= theta 才触发。
  // ------------------------------------------------------------
  {
    Compaction_sst* c = versions_sst->PickCompaction();

    if (c != nullptr && c->level() == 0) {
      const double invalid_ratio = estimate_invalid_ratio(c);

      if (invalid_ratio < theta) {
        Log(options_.info_log,
            "[SSD-L0-Compaction] delay L0 -> L1 regionization "
            "IR=%.4f theta=%.4f read_ratio=%.4f write_ratio=%.4f "
            "window_read=%llu window_write=%llu",
            invalid_ratio,
            theta,
            workload.read_ratio,
            workload.write_ratio,
            static_cast<unsigned long long>(workload.window_read_ops),
            static_cast<unsigned long long>(workload.window_write_ops));

        c->ReleaseInputs();
        delete c;
        c = nullptr;
      } else {
        CompactionState_sst* compact = new CompactionState_sst(c);

        // L0 输出需要按 key 所属 Region 重新划分。
        compact->target_region_id = -1;

        Log(options_.info_log,
            "[SSD-L0-Compaction] trigger L0 -> L1 regionization "
            "IR=%.4f theta=%.4f read_ratio=%.4f write_ratio=%.4f "
            "window_read=%llu window_write=%llu",
            invalid_ratio,
            theta,
            workload.read_ratio,
            workload.write_ratio,
            static_cast<unsigned long long>(workload.window_read_ops),
            static_cast<unsigned long long>(workload.window_write_ops));

        Status s = DoCompactionWork_sst(compact);

        CleanupCompaction_sst(compact);
        c->ReleaseInputs();

        if (!s.ok()) {
          RecordBackgroundError(s);
        }

        delete c;
        return;
      }
    }

    if (c != nullptr) {
      c->ReleaseInputs();
      delete c;
    }
  }

  // ------------------------------------------------------------
  // 2. Region L1+ 自适应 compaction + 动态范围调整。
  //
  // 流程：
  //   1. PickCompactionForRegion() 选择候选 SSTable；
  //   2. IR >= theta 后才真正触发 compaction；
  //   3. 若 Region 文件数超过 kMaxFileCountInRegion，
  //      则根据候选文件之外的剩余文件范围调整 Region 边界；
  //   4. 输出 SSTable 按调整后的 key 范围重新归属 Region。
  // ------------------------------------------------------------
  std::vector<int> regions = versions_sst->current()->GetActiveRegions();
  bool did_compaction = false;

  for (int rid : regions) {
    for (int level = 1; level < config::kNumLevels_sst - 1; ++level) {
      double invalid_ratio = 0.0;

      Compaction_sst* c =
          versions_sst->PickCompactionForRegion(rid, level, &invalid_ratio);

      if (c == nullptr) {
        continue;
      }

      if (invalid_ratio < theta) {
        Log(options_.info_log,
            "[RegionCompaction] delay region=%d level=%d "
            "IR=%.4f theta=%.4f read_ratio=%.4f write_ratio=%.4f "
            "window_read=%llu window_write=%llu",
            rid,
            level,
            invalid_ratio,
            theta,
            workload.read_ratio,
            workload.write_ratio,
            static_cast<unsigned long long>(workload.window_read_ops),
            static_cast<unsigned long long>(workload.window_write_ops));

        c->ReleaseInputs();
        delete c;
        continue;
      }

      // 只有确认本轮 compaction 会执行之后，才允许动态调整 Region 范围。
      versions_sst->MaybeAdjustRegionBoundaryForPickedCompaction(rid, level, c);

      CompactionState_sst* compact = new CompactionState_sst(c);

      // 注意：
      // 动态调整 Region 边界后，新生成 SSTable 必须按 key 所在 Region 重新归属。
      // 因此这里不要固定写回 rid，而是交给输出路径根据 key 判断。
      compact->target_region_id = -1;

      Log(options_.info_log,
          "[RegionCompaction] trigger region=%d level=%d "
          "IR=%.4f theta=%.4f read_ratio=%.4f write_ratio=%.4f "
          "window_read=%llu window_write=%llu",
          rid,
          level,
          invalid_ratio,
          theta,
          workload.read_ratio,
          workload.write_ratio,
          static_cast<unsigned long long>(workload.window_read_ops),
          static_cast<unsigned long long>(workload.window_write_ops));

      Status s = DoCompactionWork_sst(compact);

      CleanupCompaction_sst(compact);
      c->ReleaseInputs();

      if (!s.ok()) {
        RecordBackgroundError(s);
      }

      delete c;
      did_compaction = true;
      break;
    }

    if (did_compaction) {
      break;
    }
  }

  if (!did_compaction) {
    Log(options_.info_log,
        "[RegionCompaction] no eligible compaction "
        "theta=%.4f read_ratio=%.4f write_ratio=%.4f "
        "window_read=%llu window_write=%llu",
        theta,
        workload.read_ratio,
        workload.write_ratio,
        static_cast<unsigned long long>(workload.window_read_ops),
        static_cast<unsigned long long>(workload.window_write_ops));
  }
}

Status DBImpl::Compactpmtable(int level) {
  mutex_.AssertHeld();

  Compaction* c = versions_->PickCompaction(config::kNumLevels - 1);
  if (c == nullptr) {
    return Status::OK();
  }

  CompactionState* compact = new CompactionState(c);

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  Status s = CompactionToSsd(compact, compact->smallest_snapshot);

  CleanupCompaction(compact);

  if (!s.ok()) {
    RecordBackgroundError(s);
  }

  c->ReleaseInputs();
  delete c;

  return s;
}

Status DBImpl::DoCompactionForLevel(int level) {
  mutex_.AssertHeld();
  Compaction* c = versions_->PickCompaction(level);
  CompactionState* compact = new CompactionState(c);

  uint64_t smallest_snapshot;

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }
  Status s = DoCompactionWork(compact, smallest_snapshot);
  // Status s = (level == config::kNumLevels - 1) ?
  // CompactionToSsd(compact,smallest_snapshot) :
  // DoCompactionWork(compact,smallest_snapshot);
  CleanupCompaction(compact);

  if (!s.ok()) {
    RecordBackgroundError(s);
  }
  c->ReleaseInputs();
  RemoveObsoleteFiles();
  delete c;
  return s;
}

void DBImpl::HandleOverflowToSSD() {
  Status s;
  MutexLock l(&mutex_);
  // while (versions_->current()->NumFiles(config::kNumLevels-2) > 4) {

  //   Status s = Compactpmtable(config::kNumLevels-2);
  //   printf("level files %d\n",
  //   versions_->current()->NumFiles(config::kNumLevels-2));
  //   //background_work_finished_signal_.Wait();
  // }

  if (versions_->current()->NumFiles(config::kNumLevels - 2) > 4) {
    //   printf("wait for background Compaction\n");

    background_work_finished_signal_.Wait();
    // printf("level files %d\n",
    // versions_->current()->NumFiles(config::kNumLevels-2));
    Status s = Compactpmtable(config::kNumLevels - 2);
    // while (learning_prepare.empty()) {
    //     preparing_queue_cv.Wait();
    //     env_->
    //   }
    // env_->forlearning();
  }
}

Status DBImpl::DoCompactionWork_sst(CompactionState_sst* compact) {
  mutex_.AssertHeld();

  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_sst->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;

  const bool l0_regionize = (compact->compaction->level() == 0);

  Log(options_.info_log,
      "DoCompactionWork_sst %d@%d + %d@%d files, target_region=%d, l0_regionize=%d",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      compact->target_region_id,
      l0_regionize ? 1 : 0);

  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  mutex_.Unlock();

  Iterator* input = versions_sst->MakeInputIterator(compact->compaction);
  input->SeekToFirst();

  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  for (; input->Valid() && !shutting_down_.load(std::memory_order_acquire);) {
    Slice key = input->key();

    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        Log(options_.info_log,
            "FinishCompactionOutputFile failed on ShouldStopBefore");
        break;
      }
    }

    bool drop = false;

    if (!ParseInternalKey(key, &ikey)) {
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        drop = true;
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      // Every SSD output follows the current Region boundaries, including
      // L1+ compactions whose Region boundary was adjusted after selection.
      const int output_region_id = GetRegionIdForKey(key);
      if (output_region_id <= 0 ||
          output_region_id >= g_region_meta_manager.NumRegions()) {
        status = Status::Corruption("SSD compaction key has no valid Region");
        break;
      }

      // Close the current SSTable whenever the next key belongs to a
      // different Region, regardless of the input level.
      if (compact->builder != nullptr &&
          compact->current_output()->region_id != output_region_id) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          Log(options_.info_log,
              "FinishCompactionOutputFile failed on region boundary");
          break;
        }
      }

      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }

        compact->current_output()->region_id = output_region_id;
        compact->current_output()->smallest.DecodeFrom(key);
      }

      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          Log(options_.info_log,
              "FinishCompactionOutputFile failed due to size limit");
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }

  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }

  if (status.ok()) {
    status = input->status();
  } else {
    Log(options_.info_log,
        "Compaction error: %s",
        status.ToString().c_str());
  }

  delete input;
  input = nullptr;

  CompactionStattossd stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;

  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
    my_stats.total_compaction_bytes += compact->outputs[i].file_size;
  }

  mutex_.Lock();

  _stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults_sst(compact);
  }

  if (!status.ok()) {
    RecordBackgroundError(status);
  }

  VersionSet_sst::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to -----------: %s",
      versions_sst->LevelSummary(&tmp));

  return status;
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();

  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;

  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

void DBImpl::CleanupCompaction_sst(CompactionState_sst* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState_sst::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState_sst* compact) {
  assert(compact != nullptr);
  assert(compact->builder == nullptr);

  uint64_t file_number;
  {
    mutex_.Lock();

    file_number = versions_sst->NewFileNumber();
    pending_outputs_.insert(file_number);

    CompactionState_sst::Output out;
    out.number = file_number;
    out.file_size = 0;
    out.smallest.Clear();
    out.largest.Clear();

    // The first retained key assigns the Region using the current boundaries.
    // A scheduling hint may be stale after a Region boundary adjustment.
    out.region_id = -1;
    out.dt = nullptr;

    compact->outputs.push_back(out);

    mutex_.Unlock();
  }

  std::string fname = TableFileName(dbname_ssd_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile, file_number);
  } else {
    printf("error in OpenCompactionOutputFile\n");
  }

  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState_sst* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  if (compact->current_output()->region_id <= 0 ||
      compact->current_output()->region_id >=
          g_region_meta_manager.NumRegions()) {
    return Status::Corruption("SSD compaction output has no valid Region");
  }

  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();

  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }

  const uint64_t current_bytes = compact->builder->FileSize();
  assert(current_bytes > 0);

  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;

  delete compact->builder;
  compact->builder = nullptr;

  delete compact->outfile;
  compact->outfile = nullptr;

  if (adgMod::KV_S == 1) {
    CompactionState_sst::Output* output = compact->current_output();
    int output_level = compact->compaction->level() + 1;

    FileMetaData* meta = new FileMetaData();
    meta->number = output->number;
    meta->file_size = output->file_size;
    meta->smallest = output->smallest;
    meta->largest = output->largest;
    meta->region_id = output->region_id;

    env_->PrepareLearning(output_level, meta);
  }

  if (s.ok() && current_entries > 0) {
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;

    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes, region=%d",
          (unsigned long long)output_number,
          compact->compaction->level() + 1,
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes,
          compact->current_output()->region_id);
    }
  }

  return s;
}

Status DBImpl::InstallCompactionResults_sst(CompactionState_sst* compact) {
  mutex_.AssertHeld();

  Log(options_.info_log,
      "Compacted sst %d@%d + %d@%d files => %lld bytes, target_region=%d",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes),
      compact->target_region_id);

  compact->compaction->AddInputDeletions(compact->compaction->edit());

  const int base_level = compact->compaction->level();
  const int target_level = base_level + 1;

  for (size_t i = 0; i < compact->outputs.size(); ++i) {
    const CompactionState_sst::Output& out = compact->outputs[i];

    if (target_level >= 1 && out.region_id != -1) {
      compact->compaction->edit()->AddFileWithRegion(
          target_level,
          out.number,
          out.file_size,
          out.smallest,
          out.largest,
          out.region_id);
    } else {
      compact->compaction->edit()->AddFile(
          target_level,
          out.number,
          out.file_size,
          out.smallest,
          out.largest);
    }
  }

  Status s = versions_sst->LogAndApply(compact->compaction->edit(), &mutex_);

  if (s.ok()) {
    // 1. 删除输入文件对应的 region 元数据。
    for (int which = 0; which < 2; which++) {
      for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
        FileMetaData* f = compact->compaction->input(which, i);
        if (f != nullptr) {
          g_region_meta_manager.RemoveFileFromAllRegions(f->number);
        }
      }
    }

    // 2. 添加输出文件对应的 region 元数据。
    for (size_t i = 0; i < compact->outputs.size(); ++i) {
      const CompactionState_sst::Output& out = compact->outputs[i];

      if (target_level >= 1 && out.region_id != -1) {
        FileMetaData meta;
        meta.number = out.number;
        meta.file_size = out.file_size;
        meta.smallest = out.smallest;
        meta.largest = out.largest;
        meta.dt = out.dt;
        meta.region_id = out.region_id;

        g_region_meta_manager.UpdateRegionMetaforCompaction(out.region_id, meta);
      } else {
        Log(options_.info_log,
            "[SSDCompaction] skip registering output #%llu because region_id=%d target_level=%d",
            (unsigned long long)out.number,
            out.region_id,
            target_level);
      }
    }
  }

  return s;
}

/*Status DBImpl::InstallCompactionResults_sst(CompactionState_sst* compact) {
  mutex_.AssertHeld();

  compact->compaction->AddInputDeletions(compact->compaction->edit());

  const int base_level = compact->compaction->level();
  const int target_level = base_level + 1;

  for (size_t i = 0; i < compact->outputs.size(); ++i) {
    const CompactionState_sst::Output& out = compact->outputs[i];

    if (target_level >= 1) {
      compact->compaction->edit()->AddFileWithRegion(
          target_level, out.number, out.file_size,
          out.smallest, out.largest, out.region_id);
    } else {
      compact->compaction->edit()->AddFile(
          target_level, out.number, out.file_size,
          out.smallest, out.largest);
    }
  }

  Status s = versions_sst->LogAndApply(compact->compaction->edit(), &mutex_);

  if (s.ok()) {
    // 1. 删除输入文件的 region 元数据
    for (int which = 0; which < 2; which++) {
      for (size_t i = 0; i < compact->compaction->num_input_files(which); i++) {
        FileMetaData* f = compact->compaction->input(which, i);
        if (f != nullptr) {
          g_region_meta_manager.RemoveFileFromAllRegions(f->number);
        }
      }
    }

    // 2. 添加输出文件的 region 元数据
    for (size_t i = 0; i < compact->outputs.size(); ++i) {
      const CompactionState_sst::Output& out = compact->outputs[i];

      if (target_level >= 1 && out.region_id != -1) {
        FileMetaData meta;
        meta.number = out.number;
        meta.file_size = out.file_size;
        meta.smallest = out.smallest;
        meta.largest = out.largest;
        meta.region_id = out.region_id;
        meta.dt = out.dt;

        g_region_meta_manager.UpdateRegionMetaforCompaction(out.region_id, meta);
      }
    }
  }

  return s;
}*/

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  const uint64_t install_start = env_->NowMicros();
  const int level = compact->compaction->level();
  const char* temp = PMCompactionHotnessName(compact->compaction);
  PMTraceLog("[PM-INSTALL-BEGIN] cid=%llu level=%d temp=%s outputs=%zu %s",
             (unsigned long long)compact->trace_cid, level, temp,
             compact->outputs.size(), PMLevelFilesString().c_str());

  Log(options_.info_log, "Compacted pm---%d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  Status s;
  if (level >= config::kNumLevels - 2) {
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 0)->dt);

#if FULL_TIERED_COMPACTION
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 2)->dt);
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 3)->dt);
#endif
    s = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  } else {
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 0)->dt);
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 1)->dt);
#if FULL_TIERED_COMPACTION
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 2)->dt);
    compact->compaction->edit()->RemoveFile(
        level, compact->compaction->input(0, 3)->dt);
#endif

    for (size_t i = 0; i < compact->outputs.size(); i++) {
      const CompactionState::Output& out = compact->outputs[i];
      compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                           out.smallest, out.largest, out.dt,
                                           out.hotness);
    }

    s = versions_->LogAndApply(compact->compaction->edit(), &mutex_);
  }

  PMTraceLog("[PM-INSTALL-END] cid=%llu level=%d temp=%s cost_ms=%.3f status=%s %s",
             (unsigned long long)compact->trace_cid, level, temp,
             (env_->NowMicros() - install_start) / 1000.0,
             s.ToString().c_str(), PMLevelFilesString().c_str());
  return s;
}

// --------------------
// pmtable -> sstable
// need to separation
Status DBImpl::CompactionToSsd(CompactionState* compact,
                               uint64_t smallest_snapshot) {
  mutex_.AssertHeld();
  Status s;
  VersionEdit edit;
  Version* base = versions_->current();

  CompactionState::Output out;
  out.smallest.Clear();
  out.largest.Clear();

  compact->trace_cid = ++pm_trace_cid_;
  const uint64_t pm_to_ssd_start = env_->NowMicros();
  FileMetaData* oldfmd = compact->compaction->input(0, 0);
  // FileMetaData* newfmd = compact->compaction->input(0, 1);

  PMTraceLog("[PM-TO-SSD-BEGIN] cid=%llu level=%d temp=%s file=%llu size=%llu mem=%llu %s",
             (unsigned long long)compact->trace_cid, compact->compaction->level(),
             PMHotnessName(oldfmd->hotness),
             (unsigned long long)oldfmd->number,
             (unsigned long long)oldfmd->file_size,
             (unsigned long long)(oldfmd->dt == nullptr ? 0 : oldfmd->dt->ApproximateMemoryUsage()),
             PMLevelFilesString().c_str());

  PMtable* newdt = oldfmd->dt;
  // PMtable* olddt; newdt

  // if (newfmd == nullptr) {
  //     printf("newfmd is null\n");
  //     newdt = new PMtable(internal_comparator_);
  // } else {
  //     newdt = newfmd->dt;
  // }
  // mutex_.Unlock();

  // s = newdt->Compact(olddt, smallest_snapshot);

  // mutex_.Lock();
  out.dt = newdt;
  base->Ref();

  // kv seperation

  // printf("CompactionToSsd11\n");
  s = WriteLeveltoSsTable(newdt, &edit);
  // printf("CompactionToSsd22\n");

  base->Unref();
  uint32_t len;
  const char* p = newdt->table_.smallest->key;
  p = GetVarint32Ptr(p, p + 5, &len);
  out.smallest.DecodeFrom(Slice(p, len));

  p = newdt->table_.largest[0]->key;
  p = GetVarint32Ptr(p, p + 5, &len);
  out.largest.DecodeFrom(Slice(p, len));

  out.file_size = newdt->ApproximateMemoryUsage();

  compact->outputs.push_back(out);
  nvmn_size_delete(out.file_size);

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    Log(options_.info_log, "s.ok()\n");
    // printf("s.ok()\n");
    s = Status::IOError("Deleting DB during Pmemtable compaction");
  }

  if (s.ok()) {
    //  printf("s.ok()2\n");
    s = InstallCompactionResults(compact);
    // InstallCompactionResults_sst(compact);
  }
  PMTraceLog("[PM-TO-SSD-END] cid=%llu level=%d temp=%s cost_ms=%.3f status=%s %s",
             (unsigned long long)compact->trace_cid, compact->compaction->level(),
             PMHotnessName(oldfmd->hotness),
             (env_->NowMicros() - pm_to_ssd_start) / 1000.0,
             s.ToString().c_str(), PMLevelFilesString().c_str());
  return s;
}

Status DBImpl::DoCompactionWork(CompactionState* compact,
                                uint64_t smallest_snapshot) {
  // compaction time
  mutex_.AssertHeld();

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;
  compact->trace_cid = ++pm_trace_cid_;
  const int trace_level = compact->compaction->level();
  const char* trace_temp = PMCompactionHotnessName(compact->compaction);
  PMTraceLog("[PM-COMPACT-BEGIN] cid=%llu temp=%s arrival=%d physical=%d %s",
             (unsigned long long)compact->trace_cid, trace_temp,
             trace_level + 1, trace_level, PMLevelFilesString().c_str());

  Log(options_.info_log, "Compacting pm  %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  // int pmtables = versions_->NumLevelFiles(compact->compaction->level());

  // while (pmtables > 2) {
  mutex_.Unlock();
  Status status;
  size_t readsum = 0;
  if (!shutting_down_.load(std::memory_order_acquire)) {
    int level = compact->compaction->level();

    CompactionState::Output out;
    out.smallest.Clear();
    out.largest.Clear();

    PMtable *olddt, *newdt;
    // printf("level______________ %d\n",level);
    FileMetaData* oldfmd = compact->compaction->input(0, 0);
    // printf("oldfmd %d\n",oldfmd->number);
    FileMetaData* newfmd = compact->compaction->input(0, 1);
    // printf("newfmd %d\n",newfmd->number);

#if FULL_TIERED_COMPACTION
    // for full tiered compaciton
    FileMetaData* newfmd2 = compact->compaction->input(0, 2);
    printf("newfmd2 %d\n", newfmd2->number);
    FileMetaData* newfmd3 = compact->compaction->input(0, 3);
    printf("newfmd3 %d\n", newfmd3->number);
#endif
    // PMtable* olddt = oldfmd->dt;
    // PMtable* newdt = newfmd->dt;

    //    if (oldfmd == nullptr) {
    //      olddt = new PMtable(internal_comparator_);
    //      out.number = versions_->NewFileNumber();
    //    } else {
    //      olddt = oldfmd->dt;
    //      out.number = oldfmd->number;
    //    }
    olddt = oldfmd->dt;
    out.number = oldfmd->number;
    newdt = newfmd->dt;

    const uint64_t inner_start = env_->NowMicros();
    PMTraceLog("[PM-COMPACT-INNER-BEGIN] cid=%llu level=%d temp=%s old_file=%llu old_temp=%s old_size=%llu old_mem=%llu new_file=%llu new_temp=%s new_size=%llu new_mem=%llu",
               (unsigned long long)compact->trace_cid, level,
               trace_temp,
               (unsigned long long)oldfmd->number, PMHotnessName(oldfmd->hotness),
               (unsigned long long)oldfmd->file_size,
               (unsigned long long)(olddt == nullptr ? 0 : olddt->ApproximateMemoryUsage()),
               (unsigned long long)newfmd->number, PMHotnessName(newfmd->hotness),
               (unsigned long long)newfmd->file_size,
               (unsigned long long)(newdt == nullptr ? 0 : newdt->ApproximateMemoryUsage()));

    status = olddt->Compact(newdt, smallest_snapshot);

    PMTraceLog("[PM-COMPACT-INNER-END] cid=%llu level=%d temp=%s cost_ms=%.3f status=%s out_file=%llu out_temp=%s out_mem=%llu",
               (unsigned long long)compact->trace_cid, level, trace_temp,
               (env_->NowMicros() - inner_start) / 1000.0,
               status.ToString().c_str(),
               (unsigned long long)oldfmd->number, PMHotnessName(oldfmd->hotness),
               (unsigned long long)(olddt == nullptr ? 0 : olddt->ApproximateMemoryUsage()));

#if FULL_TIERED_COMPACTION
    auto* newdt2 = newfmd2 == nullptr ? nullptr : newfmd2->dt;
    auto* newdt3 = newfmd3 == nullptr ? nullptr : newfmd3->dt;

    if (newdt2 != nullptr) {
      status = olddt->Compact(newdt2, smallest_snapshot);
    }

    if (newdt3 != nullptr) {
      status = olddt->Compact(newdt3, smallest_snapshot);
    }
#endif
    out.dt = olddt;
    // Preserve hot data when a mixed pair has to be merged to make progress.
    out.hotness = (oldfmd->hotness == HOT_TABLE ||
                   newfmd->hotness == HOT_TABLE) ? HOT_TABLE : COLD_TABLE;


    uint32_t len;
    const char* p = olddt->table_.smallest->key;

    p = GetVarint32Ptr(p, p + 5, &len);
    out.smallest.DecodeFrom(Slice(p, len));

    p = olddt->table_.largest[0]->key;
    p = GetVarint32Ptr(p, p + 5, &len);
    out.largest.DecodeFrom(Slice(p, len));
    out.file_size = olddt->ApproximateMemoryUsage();
    compact->outputs.push_back(out);
  }
  mutex_.Lock();

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  /*
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }*/
  stats.bytes_read += readsum;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    // stats.bytes_written += compact->outputs[i].file_size;
    stats.bytes_written += compact->outputs[i].dt->table_.wa;
    my_stats.total_compaction_bytes += compact->outputs[i].dt->table_.wa;
  }

  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }

  PMTraceLog("[PM-COMPACT-END] cid=%llu temp=%s arrival=%d physical=%d cost_ms=%.3f status=%s %s",
             (unsigned long long)compact->trace_cid, trace_temp,
             trace_level + 1, trace_level,
             (env_->NowMicros() - start_micros) / 1000.0,
             status.ToString().c_str(), PMLevelFilesString().c_str());

  if (!status.ok()) {
    RecordBackgroundError(status);
  }

  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));

  // }

  // pmtables = versions_->NumLevelFiles(compact->compaction->level());
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);
  Version_sst* const version_sst GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version,
            Version_sst* version_sst)
      : mu(mutex),
        version(version),
        mem(mem),
        imm(imm),
        version_sst(version_sst) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  if (state->version_sst != nullptr) state->version_sst->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();

  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }

  list.push_back(hot_mem_->NewIterator());
  hot_mem_->Ref();
  if (hot_imm_ != nullptr) {
    list.push_back(hot_imm_->NewIterator());
    hot_imm_->Ref();
  }

  versions_->current()->AddIterators(options, &list);

  Index* index = versions_sst->options()->index;
  // [B-tree]
  if (versions_sst->current()->NumFiles(0) > 0 ||
      versions_sst->current()->NumFiles(1) > 0) {
    list.push_back(index->NewIterator(options, table_cache_));
  }

  versions_sst->current()->AddIterators(options, &list);
  versions_sst->current()->Ref();
  // }

  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current(),
                                     versions_sst->current());

  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  key_detector.RecordAccess(key.ToString());
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  MemTable* hot_mem = hot_mem_;
  MemTable* hot_imm = hot_imm_;

  Version* PM_current = versions_->current();
  Version_sst* BT_current = versions_sst->current();
  mem->Ref();
  hot_mem->Ref();
  if (imm != nullptr) imm->Ref();
  if(hot_imm != nullptr) hot_imm->Ref();
  PM_current->Ref();
  BT_current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;
  Version_sst::GetStats bt_stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s) || hot_mem->Get(lkey, value, &s)){
      mem_hits++;
      my_stats.memtable_count_++;
      // Done
    }else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      imm_hits++;
      my_stats.memtable_count_++;
      // Done
    }else if (hot_imm != nullptr && hot_imm->Get(lkey, value, &s)) {
      imm_hits++;
      my_stats.memtable_count_++;
      // Done
    }else if (PM_current->Get(options, lkey, value, &stats, &s)) {
      PMtable_hits++;
      my_stats.pmtable_count_++;
    } else {
      s = BT_current->Get(options, lkey, value);
      have_stat_update = true;
      Btree_hits++;
      my_stats.disk_count_++;
    }
  }
  mutex_.Lock();

  mem->Unref();
  hot_mem->Unref();
  if (imm != nullptr) imm->Unref();
  if (hot_imm != nullptr) hot_imm->Unref();
  PM_current->Unref();
  BT_current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    for (int i = 0; i < config::kNumLevels; i++) {
      MaybeScheduleCompaction(i);
    }
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {

  return DB::Put(o, key, val);

}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = WriteBatchInternal::InsertInto(updates, mem_,hot_mem_,log_);
      mutex_.Lock();
      //      mutex_.Unlock();
      //      bool sync_error = false;
      //      if (adgMod::KV_S == 0) {
      //#if time
      //        uint64_t start = env_->NowMicros();
      //#endif
      //        status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      //#if time
      //        uint64_t end = env_->NowMicros();
      //        log_time += (end - start);
      //#endif
      //        if (status.ok() && options.sync) {
      //          status = logfile_->Sync();
      //          if (!status.ok()) {
      //            sync_error = true;
      //          }
      //        }
      //      }
      //
      //      if (status.ok()) {
      //#if time
      //        uint64_t start2 = env_->NowMicros();
      //#endif
      //        status = WriteBatchInternal::InsertInto(updates, mem_);
      //#if time
      //        uint64_t end2 = env_->NowMicros();
      //        w_mem_time += (end2 - start2);
      //#endif
      //      }
      //      mutex_.Lock();
      //      if (sync_error) {
      //        // The state of the log file is indeterminate: the log record we
      //        // just added may or may not show up when the DB is re-opened.
      //        // So we force the DB into a mode where all future writes fail.
      //        RecordBackgroundError(status);
      //      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      Log(options_.info_log,
          "[WRITE-STOP-BG-ERROR] %s",
          bg_error_.ToString().c_str());

      fprintf(stderr,
              "[WRITE-STOP-BG-ERROR] %s\n",
              bg_error_.ToString().c_str());
      fflush(stderr);

      s = bg_error_;
      break;

    } else if (allow_delay &&versions_->NumLevelFiles(config::kNumLevels-2) >=
                                  config::kL0_SlowdownWritesTrigger) {
      uint64_t start = env_->NowMicros();

      mutex_.Unlock();
      Log(options_.info_log, "Too many L3 files; slowdown...\n");
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();

      uint64_t end = env_->NowMicros();
      l0_slow_tall_time_ += (end - start);
      // l0_slow_tall_time_+=1;


    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) &&
               hot_mem_->ApproximateMemoryUsage() <= options_.write_buffer_size) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr || hot_imm_ != nullptr) {

      uint64_t start = env_->NowMicros();

      Log(options_.info_log, "Current memtable full; waiting...\n");
      while (imm_ != nullptr || hot_imm_ != nullptr) {
        background_work_finished_signal_.Wait();
      }

      uint64_t end = env_->NowMicros();
      mem_stall_time_ += (end - start);
      //mem_stall_time_ +=1;

    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      if(mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
        assert(versions_->PrevLogNumber() == 0);
        uint64_t new_log_number = versions_->NewFileNumber();
        WritableFile* lfile = nullptr;
        s = env_->NewWritableFile(LogFileName(dbname_ssd_, new_log_number), &lfile);
        if (!s.ok()) {
          // Avoid chewing through file number space in a tight loop.
          versions_->ReuseFileNumber(new_log_number);
          break;
        }
        delete log_;
        delete logfile_;
        logfile_ = lfile;
        logfile_number_ = new_log_number;
        log_ = new log::Writer(lfile);
        imm_ = mem_;
        has_imm_.store(true, std::memory_order_release);
        mem_ = new MemTable(internal_comparator_, options_.write_buffer_size + 2 * 1024 * 1024);
        mem_->Ref();
      } else if(hot_mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
        hot_imm_ = hot_mem_;
        has_hot_imm_.store(true, std::memory_order_release);
        hot_mem_ = new MemTable(internal_comparator_, options_.write_buffer_size + 2 * 1024 * 1024);
        hot_mem_->Ref();
        //std::cout<<"hot memtable flush!!!"<<std::endl;
      }
      force = false;

      MaybeScheduleCompaction(0);
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();
  HandleOverflowToSSD();

  my_stats.PrintStats();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    std::cout << "--------------------------------------------------"
              << std::endl;
    std::cout << "memtable stall time: " << 1.0 * mem_stall_time_ / 1000000
              << " s" << std::endl;
    std::cout << "L0 stall time: " << 1.0 * L0_stop_stall_time_ / 1000000
              << "  s" << std::endl;
    std::cout << "L0 slow stall time: " << 1.0 * l0_slow_tall_time_ / 1000000
              << "  s" << std::endl;
#if time

    std::cout << "write log time: " << 1.0 * w_mem_time / 1000000 << " s"
              << std::endl;
    std::cout << "write log time: " << 1.0 * log_time / 1000000 << " s"
              << std::endl;
    std::cout << "vlog get time: " << 1.0 * vlog_imm / 1000000 << " s"
              << std::endl;
#endif
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "           Compactions\n"
                  "P.Level  Files Size(MB) \n"
                  "---------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "P.%3d %8d %8.0f\n", level, files,
                      versions_->NumLevelBytes(level) / 1048576.0);
        // stats_[level].micros / 1e6,
        // stats_[level].bytes_read / 1048576.0,
        // stats_[level].bytes_written / 1048576.0);
        value->append(buf);
        // std::cout << "\nwrite in SSD size (MB):"
        // <<versions_sst->NumLevelBytes(level) / 1048576.0 << std::endl;
      }
      fprintf(stderr,
              "mem_hits %lu, imm_hits %lu, PMtable_hits %lu, B+tree_hits %lu \n",
              mem_hits, imm_hits, PMtable_hits, Btree_hits);

      NvmUsagePrint();
    };

    // if (versions_sst->NumLevelFiles(0) > 0) {
    char Sbuf[200];
    std::snprintf(Sbuf, sizeof(Sbuf),
                  "\nS.Level  Files Size(MB) \n"
                  "---------------------\n");
    value->append(Sbuf);
    for (int level = 0; level < config::kNumLevels_sst; level++) {
      int files = versions_sst->NumLevelFiles(level);

      if (_stats_[level].micros > 0 || files > 0) {
        std::snprintf(Sbuf, sizeof(Sbuf), "S.%3d %8d %8.0f\n", level, files,
                      versions_sst->NumLevelBytes(level) / 1048576.0);
        // stats_[level].micros / 1e6,
        // stats_[level].bytes_read / 1048576.0,
        // stats_[level].bytes_written / 1048576.0);
        value->append(Sbuf);
        // std::cout << "\nwrite in SSD size (MB):"
        // <<versions_sst->NumLevelBytes(level) / 1048576.0 << std::endl;
      }
    };

    key_detector.PrintStats();
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }
  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname_,
                DB** dbptr) {
  *dbptr = nullptr;
  // printf("option.env: %p\n", options.env);
  // printf("option.env: %p\n", adgMod::env);

  // DBImpl* impl = new DBImpl(options, dbname_disk, dbname_mem);
  DBImpl* impl = new DBImpl(options, dbname_);
  // printf("dbname_disk: %s\n",  impl->dbname_ssd_.c_str());
  // printf("dbname_mem: %s\n",  impl->dbname_.c_str());

  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;

  Status s = impl->Recover(&edit, &save_manifest);

  if (s.ok() && impl->mem_ == nullptr && impl->hot_mem_ == nullptr) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(
        LogFileName(impl->dbname_ssd_, new_log_number), &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_,
                                options.write_buffer_size + 2* 1024 * 1024);
      impl->mem_->Ref();
      impl->hot_mem_ = new MemTable(impl->internal_comparator_,
                                    options.write_buffer_size + 2* 1024 * 1024);
      impl->hot_mem_->Ref();
    }

  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_sst->LogAndApply(&edit, &impl->mutex_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }

  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->deleteObsoleteFiles();
    for (int i = 0; i < config::kNumLevels; i++) {
      impl->MaybeScheduleCompaction(i);
    }

    impl->MaybeScheduleSstCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;

  } else {
    delete impl;
  }

  return s;
}

Snapshot::~Snapshot() = default;

// 删除已有的数据库
Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

bool DBImpl::HaveBalancedDistribution() {
  for (int i = 0; i < config::kNumLevels; i++) {
    if (versions_->NeedsCompaction(i)) {
      return false;
    }
  }
  return true;
}

Version_sst* DBImpl::GetCurrentVersion() {
  MutexLock l(&mutex_);
  Version_sst* ver = versions_sst->current();
  ver->Ref();
  return ver;
}

int DBImpl::GetRegionIdForKey(const Slice& internal_key) {
  return g_region_meta_manager.FindRegionContainingKey(internal_key);
}

void DBImpl::ReturnCurrentVersion(Version_sst* version) {
  MutexLock l(&mutex_);
  version->Unref();
}



}  // namespace leveldb
