

#include "learned_index.h"

#include "db/version_bt.h"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <utility>

#include "util/mutexlock.h"

#include "util.h"

namespace adgMod {

// for position
std::pair<uint64_t, uint64_t> LearnedIndexData::GetPosition(
    const Slice& target_x) const {
  assert(string_segments.size() > 1);
  ++served;

  // check if the key is within the model bounds
  uint64_t target_int = SliceToInteger(target_x);
  if (target_int > max_key) return std::make_pair(size, size);
  if (target_int < min_key) return std::make_pair(size, size);

  // binary search between segments
  uint32_t left = 0, right = (uint32_t)string_segments.size() - 1;
  while (left != right - 1) {
    uint32_t mid = (right + left) / 2;
    if (target_int < string_segments[mid].x)
      right = mid;
    else
      left = mid;
  }

  // calculate the interval according to the selected segment
  double result =
      target_int * string_segments[left].k + string_segments[left].b;
  result = is_level ? result / 2 : result;
  uint64_t lower =
      result - error > 0 ? (uint64_t)std::floor(result - error) : 0;
  uint64_t upper = (uint64_t)std::ceil(result + error);
  if (lower >= size) return std::make_pair(size, size);
  upper = upper < size ? upper : size - 1;
  //                printf("%s %s %s\n", string_keys[lower].c_str(),
  //                string(target_x.data(), target_x.size()).c_str(),
  //                string_keys[upper].c_str()); assert(target_x >=
  //                string_keys[lower] && target_x <= string_keys[upper]);
  return std::make_pair(lower, upper);
}

uint64_t LearnedIndexData::MaxPosition() const { return size - 1; }

double LearnedIndexData::GetError() const { return error; }

// Actual function doing learning
bool LearnedIndexData::Learn() {

  // FILL IN GAMMA (error)
  PLR plr = PLR(error);

  // check if data if filled
  if (string_keys.empty()) assert(false);

  // fill in some bounds for the model
  uint64_t temp = atoll(string_keys.back().c_str());
  min_key = atoll(string_keys.front().c_str());
  max_key = atoll(string_keys.back().c_str());
  size = string_keys.size();

  // actual training
  std::vector<Segment> segs = plr.train(string_keys, !is_level);
  if (segs.empty()) return false;
  // fill in a dummy last segment (used in segment binary search)
  segs.push_back((Segment){temp, 0, 0});
  string_segments = std::move(segs);

  // for (auto& str : string_segments) {
  //   printf("%lu %f %f\n", str.x, str.k, str.b);
  // }

  // printf("Learned %d %lu %lu\n", level,string_segments.size(), size);
         
  learned.store(true);

  // string_keys.clear();
  return true;
}


// static learning function to be used with LevelDB background scheduling
// file learning
uint64_t LearnedIndexData::FileLearn(void* arg) {

  // printf("FileLearn\n");
  bool entered = false;


  MetaAndSelf* mas = reinterpret_cast<MetaAndSelf*>(arg);
  LearnedIndexData* self = mas->self;
  self->level = mas->level;

  Version_sst* c = db->GetCurrentVersion();
  if (self->FillData(c, mas->meta)) {
    self->Learn();
    entered = true;
  } else {
    self->learning.store(false);
  }
  adgMod::db->ReturnCurrentVersion(c);

  // printf("FileLearned %d %lu %lu\n", self->level,self->string_segments.size(),  self->size);

  // if (entered) {
  //   // count how many file learning are done.
  //   self->cost = time.second - time.first;
  //   learn_counter_mutex.Lock();
  //   events[1].push_back(new LearnEvent(time, 1, self->level, true));
  //   levelled_counters[11].Increment(mas->level, time.second - time.first);
  //   learn_counter_mutex.Unlock();
  // }

  //        if (fresh_write) {
  //            self->WriteModel(adgMod::db->versions_->dbname_ + "/" +
  //            to_string(mas->meta->number) + ".fmodel");
  //            self->string_keys.clear();
  //            self->num_entries_accumulated.array.clear();
  //        }

  if (!fresh_write) delete mas->meta;
  delete mas;
  return entered ? 1 : 0;
}

// general model checker
bool LearnedIndexData::Learned() {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  } else
    return false;
}

// file model checker, used to be also learning trigger
bool LearnedIndexData::Learned(Version_sst* version, int v_count,
                               FileMetaData* meta, int level) {
  if (learned_not_atomic)
    return true;
  else if (learned.load()) {
    learned_not_atomic = true;
    return true;
  } else
    return false;
  //        } else {
  //            if (file_learning_enabled && (true || level != 0 && level != 1)
  //            && ++current_seek >= allowed_seek && !learning.exchange(true)) {
  //                env->ScheduleLearning(&LearnedIndexData::FileLearn, new
  //                MetaAndSelf{version, v_count, meta, this, level}, 0);
  //            }
  //            return false;
  //        }
}

bool LearnedIndexData::FillData(Version_sst* version, FileMetaData* meta) {
  // if (filled) return true;

// 수정
  if (version->FillData(adgMod::read_options, meta, this)) {
    // filled = true;
    return true;
  }
  return false;
}


void LearnedIndexData::ReportStats() {


  printf("%d %lu\n", level, size);

}


LearnedIndexData* FileLearnedIndexData::GetModel(int number) {
  leveldb::MutexLock l(&mutex);
  if (file_learned_index_data.size() <= number)
    file_learned_index_data.resize(number + 1, nullptr);
  if (file_learned_index_data[number] == nullptr)
    file_learned_index_data[number] = new LearnedIndexData(file_allowed_seek, false);
  return file_learned_index_data[number];
}

bool FileLearnedIndexData::FillData(Version_sst* version, FileMetaData* meta) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->FillData(version, meta);
}

std::vector<std::string>& FileLearnedIndexData::GetData(FileMetaData* meta) {
  auto* model = GetModel(meta->number);
  return model->string_keys;
}

bool FileLearnedIndexData::Learned(Version_sst* version, FileMetaData* meta,
                                   int level) {
  LearnedIndexData* model = GetModel(meta->number);
  return model->Learned(version, 0, meta, level);
}

AccumulatedNumEntriesArray* FileLearnedIndexData::GetAccumulatedArray(
    int file_num) {
  auto* model = GetModel(file_num);
  return &model->num_entries_accumulated;
}

std::pair<uint64_t, uint64_t> FileLearnedIndexData::GetPosition(
    const Slice& key, int file_num) {
  return file_learned_index_data[file_num]->GetPosition(key);
}

FileLearnedIndexData::~FileLearnedIndexData() {
  leveldb::MutexLock l(&mutex);
  for (auto pointer : file_learned_index_data) {
    delete pointer;
  }
}

void FileLearnedIndexData::Report() {
  leveldb::MutexLock l(&mutex);

  std::set<uint64_t> live_files;
  adgMod::db->versions_sst->AddLiveFiles(&live_files);

  for (size_t i = 0; i < file_learned_index_data.size(); ++i) {
    auto pointer = file_learned_index_data[i];
  
     

       printf("%d %d %lu %lu %lu\n", pointer->level, pointer->served,
              pointer->string_segments.size(), pointer->cost, pointer->size);
      
  }
}

void AccumulatedNumEntriesArray::Add(uint64_t num_entries, string&& key) {
  array.emplace_back(num_entries, key);
}

bool AccumulatedNumEntriesArray::Search(const Slice& key, uint64_t lower,
                                        uint64_t upper, size_t* index,
                                        uint64_t* relative_lower,
                                        uint64_t* relative_upper) {

    size_t left = 0, right = array.size() - 1;
    while (left < right) {
      size_t mid = (left + right) / 2;
      if (lower < array[mid].first)
        right = mid;
      else
        left = mid + 1;
    }

    if (upper >= array[left].first) {
      while (true) {
        if (left >= array.size()) return false;
        if (key <= array[left].second) break;
        lower = array[left].first;
        ++left;
      }
      upper = std::min(upper, array[left].first - 1);
    }

    *index = left;
    *relative_lower = left > 0 ? lower - array[left - 1].first : lower;
    *relative_upper = left > 0 ? upper - array[left - 1].first : upper;
    return true;
  
}

bool AccumulatedNumEntriesArray::SearchNoError(uint64_t position, size_t* index,
                                               uint64_t* relative_position) {
  *index = position / array[0].first;
  *relative_position = position % array[0].first;
  return *index < array.size();

  //        size_t left = 0, right = array.size() - 1;
  //        while (left < right) {
  //            size_t mid = (left + right) / 2;
  //            if (position < array[mid].first) right = mid;
  //            else left = mid + 1;
  //        }
  //        *index = left;
  //        *relative_position = left > 0 ? position - array[left - 1].first :
  //        position; return left < array.size();
}

uint64_t AccumulatedNumEntriesArray::NumEntries() const {
  return array.empty() ? 0 : array.back().first;
}

}  // namespace adgMod
