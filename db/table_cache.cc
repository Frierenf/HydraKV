// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"
#include <table/filter_block.h>
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"
#include "table/format.h"
#include "table/block.h"
#include "db/version_bt.h"
#ifdef PERF_LOG
#include "util/perf_log.h"
#endif

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok() && file_size == 0) {
      s = env_->GetFileSize(fname, &file_size);
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}
Status TableCache::Get(const ReadOptions& options,
                       const IndexMeta* index,
                       const Slice& k,
                       void* arg,
                       void(*saver)(void*, const Slice&, const Slice&)) {
  Iterator* block_iter = nullptr;
  Status s = GetBlockIterator(options, index, &block_iter);
  assert(s.ok());
  if (block_iter != nullptr) {
    block_iter->Seek(k);
    if (block_iter->Valid()) {
      (*saver)(arg, block_iter->key(), block_iter->value());
    }

    s = block_iter->status();
    delete block_iter;
  }
  return s;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

// learned index
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&), int level,
                       FileMetaData* meta, uint64_t lower, uint64_t upper, bool learned, Version_sst* version,
                       adgMod::LearnedIndexData** model, bool* file_learned) {

  Cache::Handle* handle = nullptr;

  if ((adgMod::KV_S == 1)) {
      // check if file model is ready
      *model = adgMod::file_data->GetModel(meta->number);
      assert(file_learned != nullptr);
      *file_learned = (*model)->Learned();
      // printf("file_learned: %d\n", *file_learned);  
      // printf("learned: %d\n", learned);

      if (learned || *file_learned) {
        // printf("Bourbon path\n");
          LevelRead(options, file_number, file_size, k, arg, handle_result, level, meta, lower, upper, learned, version);
          return Status::OK();
      }
  }

  // else, go baseline path

  Status s = FindTable(file_number, file_size, &handle);

  if (s.ok()) {
      Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
      s = t->InternalGet(options, k, arg, handle_result, level, meta, lower, upper, learned, version);
      cache_->Release(handle);
  }

  return s;
}

void TableCache::LevelRead(const ReadOptions &options, uint64_t file_number,
                            uint64_t file_size, const Slice &k, void *arg,
                            void (*handle_result)(void *, const Slice &, const Slice &), int level,
                            FileMetaData *meta, uint64_t lower, uint64_t upper, bool learned, Version_sst *version) {

    //Cache::Handle* cache_handle = FindFile(options, file_number, file_size);
    Cache::Handle* cache_handle = nullptr;
    Status s = FindTable(file_number, file_size, &cache_handle);
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(cache_handle));
    RandomAccessFile* file = tf->file;
    FilterBlockReader* filter = tf->table->rep_->filter;

    if (!learned) {
      // if level model is not used, consult file model for predicted position
      ParsedInternalKey parsed_key;
      ParseInternalKey(k, &parsed_key);
      adgMod::LearnedIndexData* model = adgMod::file_data->GetModel(meta->number);
      auto bounds = model->GetPosition(parsed_key.user_key);
      lower = bounds.first;
      // printf("lower: %lu\n", lower);
      upper = bounds.second;
      // printf("upper: %lu\n", upper);

      if (lower > model->MaxPosition()) return;
    }

    // Get the position we want to read
    // Get the data block index
    size_t index_lower = lower / adgMod::block_num_entries;
    // printf("index_lower: %lu\n", index_lower);
    size_t index_upper = upper / adgMod::block_num_entries;
    // printf("index_upper: %lu\n", index_upper);

    // if the given interval overlaps two data block, consult the index block to get
    // the largest key in the first data block and compare it with the target key
    // to decide which data block the key is in
    uint64_t i = index_lower;
    if (index_lower != index_upper) {
      Block* index_block = tf->table->rep_->index_block;
      uint32_t mid_index_entry = DecodeFixed32(index_block->data_ + index_block->restart_offset_ + index_lower * sizeof(uint32_t));
      // printf("mid_index_entry: %d\n", mid_index_entry);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr = DecodeEntry(index_block->data_ + mid_index_entry,
                                        index_block->data_ + index_block->restart_offset_, &shared, &non_shared, &value_length);
      // printf("key_ptr_1: %s\n", key_ptr);
      assert(key_ptr != nullptr && shared == 0 && "Index Entry Corruption");
      Slice mid_key(key_ptr, non_shared);
      // printf("mid_key_1: %s\n", mid_key.ToString().c_str());
      int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
      i = comp < 0 ? index_upper : index_lower;
    }


    // Check Filter Block
    uint64_t block_offset = i * adgMod::block_size;
    // printf("block_offset: %lu\n", block_offset);

    if (filter != nullptr && !filter->KeyMayMatch(block_offset, k)) {

      cache_->Release(cache_handle);
      return;
    }


    // Get the interval within the data block that the target key may lie in
    size_t pos_block_lower = i == index_lower ? lower % adgMod::block_num_entries : 0;
    size_t pos_block_upper = i == index_upper ? upper % adgMod::block_num_entries : adgMod::block_num_entries - 1;

    // Read corresponding entries
    size_t read_size = (pos_block_upper - pos_block_lower + 1) * adgMod::entry_size;
    static char scratch[4096];
    Slice entries;
    // printf("block_offset + pos_block_lower * adgMod::entry_size: %lu\n", block_offset + pos_block_lower * adgMod::entry_size);
    s = file->Read(block_offset + pos_block_lower * adgMod::entry_size, read_size, &entries, scratch);
    // printf("file->Read: %s\n", s.ToString().c_str());
    assert(s.ok());

    // Binary Search within the interval
    uint64_t left = pos_block_lower, right = pos_block_upper;
    while (left < right) {
      uint32_t mid = (left + right) / 2;
      // printf("mid: %d\n", mid);
      // printf("left: %lu\n", left);
      // printf("right: %lu\n", right);
      // printf("pos_block_lower: %lu\n", pos_block_lower);
      // printf("entry_size: %d\n", adgMod::entry_size);
      uint32_t shared, non_shared, value_length;
      const char* key_ptr = DecodeEntry(entries.data() + (mid - pos_block_lower) * adgMod::entry_size,
              entries.data() + read_size, 
              &shared, &non_shared, &value_length);
              // printf("entry_size: %d\n", adgMod::entry_size);
              // printf("entries.data() %d\n", entries.data());
              //    printf("limit - p: %d\n", entries.data() + read_size - key_ptr);
              //  printf("shared: %d\n", shared);
              //   printf("non_shared: %d\n", non_shared);
              //   printf("value_length: %d\n", value_length);
              // printf("key_ptr: %d\n", key_ptr);
              
      assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");


      Slice mid_key(key_ptr, non_shared);
      // printf("mid_key_2: %s\n", mid_key.ToString().c_str());
      int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
      if (comp < 0) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }


    // decode the target entry to get the key and value (actually value_addr)
    uint32_t shared, non_shared, value_length;
    const char* key_ptr = DecodeEntry(entries.data() + (left - pos_block_lower) * adgMod::entry_size,
            entries.data() + read_size, &shared, &non_shared, &value_length);
    assert(key_ptr != nullptr && shared == 0 && "Entry Corruption");

    Slice key(key_ptr, non_shared), value(key_ptr + non_shared, value_length);
    handle_result(arg, key, value);

    //cache handle;
    cache_->Release(cache_handle);
}


// [B-tree] Added
Status TableCache::GetBlockIterator(const ReadOptions& options,
                                    const IndexMeta* index,
                                    Iterator** iterator) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(index->file_number, 0, &handle);
  if (s.ok()) {
    Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    *iterator = table->BlockIterator(options, BlockHandle(index->size, index->offset));
    cache_->Release(handle);
  }
  return s;
}

Status TableCache::GetTable(uint64_t file_number, uint64_t file_size, TableHandle* table_handle) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    table_handle->table_ = t;
    table_handle->RegisterCleanup(&UnrefEntry, cache_, handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

bool TableCache::FillData(const ReadOptions& options, FileMetaData *meta, adgMod::LearnedIndexData* data) {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(meta->number, meta->file_size, &handle);

    if (s.ok()) {
        Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        table->FillData(options, data);
        cache_->Release(handle);
        return true;
    } else return false;
}

}  // namespace leveldb
