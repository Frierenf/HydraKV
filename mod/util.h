

#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <vector>
#include "db/db_impl.h"
#include "leveldb/slice.h"
#include "leveldb/env.h"
#include "Counter.h"

#include <x86intrin.h>


using std::string;
using std::vector;
//using std::map;
using leveldb::Slice;


namespace adgMod {

    class FileStats;
    class FileLearnedIndexData;
    class LearnedIndexData;

    //vlog =1 
    extern bool KV_S;

    extern bool string_mode;

    // some variables and pointers made global
    extern int key_size;
    extern int value_size;
    extern leveldb::Env* env;
    extern leveldb::DBImpl* db;
    extern leveldb::ReadOptions read_options;
    extern leveldb::WriteOptions write_options;
    extern uint32_t file_model_error;
    extern bool fresh_write;
    extern int file_allowed_seek;
    uint64_t SliceToInteger(const Slice& slice);
    extern FileLearnedIndexData* file_data;
    int compare(const Slice& slice, const string& string);
    bool operator<(const Slice& slice, const string& string);
    bool operator>(const Slice& slice, const string& string);
    bool operator<=(const Slice& slice, const string& string);
    bool operator>=(const Slice& slice, const string& string);
    uint64_t get_time_difference(timespec start, timespec stop);

    extern int level_allowed_seek;
    extern float reference_frequency;
    extern bool block_num_entries_recorded;
    extern uint64_t block_num_entries;
    extern uint64_t block_size;
    extern uint64_t entry_size;


}



