

#include <util/mutexlock.h>
#include "util.h"
#include "map"
#include <x86intrin.h>
#include "learned_index.h"

using std::to_string;

namespace adgMod {

    bool KV_S = 0 ;
    bool string_mode = true;
   
    int key_size;
    int value_size;
    leveldb::Env* env;
    leveldb::DBImpl* db;
    leveldb::ReadOptions read_options;
    leveldb::WriteOptions write_options;
    FileLearnedIndexData* file_data = nullptr;
    int file_allowed_seek = 10;
    uint32_t file_model_error = 10;

    bool fresh_write = false;


    std::atomic<int> num_read(0);
    std::atomic<int> num_write(0);


    leveldb::port::Mutex compaction_counter_mutex;
    leveldb::port::Mutex file_stats_mutex;

    bool block_num_entries_recorded = false;
    bool level_learning_enabled = false;
    bool file_learning_enabled = true;
    bool load_level_model = true;
    bool load_file_model = true;
    uint64_t block_num_entries = 0;
    uint64_t block_size = 0;
    uint64_t entry_size = 0;

    int compare(const Slice& slice, const string& string) {
        return memcmp((void*) slice.data(), string.c_str(), slice.size());
    }

    bool operator<(const Slice& slice, const string& string) {
        return memcmp((void*) slice.data(), string.c_str(), slice.size()) < 0;
    }
    bool operator>(const Slice& slice, const string& string) {
        return memcmp((void*) slice.data(), string.c_str(), slice.size()) > 0;
    }
    bool operator<=(const Slice& slice, const string& string) {
        return memcmp((void*) slice.data(), string.c_str(), slice.size()) <= 0;
    }
    bool operator>=(const Slice& slice, const string& string) {
        return memcmp((void*) slice.data(), string.c_str(), slice.size()) >= 0;
    }

    uint64_t get_time_difference(timespec start, timespec stop) {
        return (stop.tv_sec - start.tv_sec) * 1000000000 + stop.tv_nsec - start.tv_nsec;
    }

    uint64_t SliceToInteger(const Slice& slice) {
        const char* data = slice.data();
        size_t size = slice.size();
        uint64_t num = 0;
        bool leading_zeros = true;

        for (int i = 0; i < size; ++i) {
            int temp = data[i];
            if (leading_zeros && temp == '0') continue;
            leading_zeros = false;
            num = (num << 3) + (num << 1) + temp - 48;
        }
        return num;
    }

}