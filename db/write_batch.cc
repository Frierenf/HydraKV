// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"

#include "leveldb/db.h"

#include "util/coding.h"

#include "LRUTierDetector.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() { Clear(); }

WriteBatch::~WriteBatch() = default;

WriteBatch::Handler::~Handler() = default;

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

size_t WriteBatch::ApproximateSize() const { return rep_.size(); }

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& source) {
  WriteBatchInternal::Append(this, &source);
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;
  MemTable* hot_mem_;
  log::Writer* log_;
  std::string raw_data;

  void Put(const Slice& key, const Slice& value) override {
        Status status;
        std::string key_;
        log::Writer* log = log_;
        key_.assign(key.data(),key.size());
        static uint64_t hot_put_cnt = 0;
        static uint64_t cold_put_cnt = 0;

        MemoryTier tier = key_detector.GetTierAndRecordAccess(key_);

        if (tier == HOT_TIER) {
          hot_put_cnt++;
          hot_mem_->Add(sequence_, kTypeValue, key, value);
        } else {
          cold_put_cnt++;
          log->AddRecord(raw_data);
          mem_->Add(sequence_, kTypeValue, key, value);
        }

        /*if ((hot_put_cnt + cold_put_cnt) % 100000 == 0) {
          fprintf(stderr,
                  "[A-TIER] hot=%llu cold=%llu hot_ratio=%.6f hot_mem=%zu cold_mem=%zu\n",
                  (unsigned long long)hot_put_cnt,
                  (unsigned long long)cold_put_cnt,
                  (double)hot_put_cnt / (double)(hot_put_cnt + cold_put_cnt),
                  hot_mem_->ApproximateMemoryUsage(),
                  mem_->ApproximateMemoryUsage());
        }*/
       sequence_++;
  }
  void Delete(const Slice& key) override {
    Status status;
    std::string key_;
    log::Writer* log = log_;
    key_.assign(key.data(),key.size());
    MemoryTier tier = key_detector.GetTierAndRecordAccess(key_);//Record new key and update hot/cold table
    if(tier == HOT_TIER) {
      hot_mem_->Add(sequence_, kTypeValue, key, Slice());
    } else {
      log->AddRecord(raw_data);
      mem_->Add(sequence_, kTypeValue, key, Slice());
    }
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b, MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}
Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable,
                                      MemTable* hot_memtable,
                                      log::Writer* log_) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  inserter.hot_mem_ = hot_memtable;
  inserter.log_ = log_;
  inserter.raw_data = b->rep_;
  return b->Iterate(&inserter);
}
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
