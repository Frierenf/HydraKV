//
// Created by daiyi on 2020/03/23.
//

#include <fcntl.h>
#include <sys/stat.h>
#include "Vlog.h"
#include "util.h"
#include "util/coding.h"

using std::string;



// const int buffer_size_max = 300 * 1024;
const int buffer_size_max = 300 * 1024;

namespace adgMod {

VLog::VLog(const std::string& vlog_name) : writer(nullptr), reader(nullptr) {
    adgMod::env->NewWritableFile(vlog_name, &writer);
    adgMod::env->NewRandomAccessFile(vlog_name, &reader);
    buffer.reserve(buffer_size_max * 2);

    struct ::stat file_stat;
    if (::stat(vlog_name.c_str(), &file_stat) == -1) {
        std::cerr << "Error calling stat: " << std::strerror(errno) << std::endl;
        // 处理错误，例如设置 vlog_size 为 0 或返回错误
        vlog_size = 0;
        return;
    }
    vlog_size = file_stat.st_size;
}
uint64_t VLog::AddRecord(const Slice& key, const Slice& value) {
    PutLengthPrefixedSlice(&buffer, key);
    PutVarint32(&buffer, value.size());
    uint64_t result = vlog_size + buffer.size();
    buffer.append(value.data(), value.size());

    if (buffer.size() >= buffer_size_max) Flush();
    return result;
}

string VLog::ReadRecord(uint64_t address, uint32_t size) {
    // printf("2\n");
    // printf("ssd address %d\n", address);
    // printf("ssd vlog_size %d\n", vlog_size);
    if (address >= vlog_size) return string(buffer.c_str() + address - vlog_size, size);

    char* scratch = new char[size];
    // printf("size %d\n", size);
    Slice value;
    // printf("ssd address1111111111 %d\n", address);
    reader->Read(address, size, &value, scratch);
    // printf("ssd address2222222222 %d\n", address);
    string result(value.data(), value.size());
    delete[] scratch;
    // printf("ssd address3333333333 %d\n", address);
    return result;
}

void VLog::Flush() {
    if (buffer.empty()) return;

    vlog_size += buffer.size();
    writer->Append(buffer);
    writer->Flush();
    buffer.clear();
    buffer.reserve(buffer_size_max * 2);
}

void VLog::Sync() {
    Flush();
    writer->Sync();
}

VLog::~VLog() {
    Flush();
}



uint64_t VLog::getvlog_size(){
    return vlog_size;
}

int VLog::getvlog_buffer() {
    return buffer.size();
}

}
