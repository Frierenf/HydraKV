
#include "leveldb/persistant_pool.h"
#include <mutex>
#include <libpmemobj.h>
#include <libpmemobj/ctl.h>
#include <libpmem.h>
#include <stdexcept>

#include <cstdio>
#include <cerrno>
#include <cstring>
#include <leveldb/nvm_option.h>

namespace leveldb {
namespace nvram {

#define LAYOUT_NAME "PMINDEXDB"

static PMEMobjpool* pm_pool;
static std::string pool_path;
// A compact PMDK header takes 16 bytes. Keep small index metadata in a
// single 64-byte unit instead of the larger default allocation class.
static constexpr size_t kSmallAllocUnitSize = 64;
static constexpr size_t kSmallAllocMaxSize = kSmallAllocUnitSize - 16;
static unsigned small_alloc_class_id = 0;
// static bool init = true;
static uint64_t allocs = 0;

char* pmemaddr=nullptr;

int is_pmem;
char* raw_;
static std::mutex pmem_mutex;

int nvm_file_exists(const char* file) { return access(file, F_OK); }

static bool ConfigureSmallAllocations() {
    small_alloc_class_id = 0;

    // Allocation classes are runtime state and must be selected again after
    // every open. Reuse an equivalent class, including one supplied by PMDK
    // configuration, before creating a new class with an available identifier.
    for (unsigned id = 1; id <= 254; ++id) {
        char name[64];
        std::snprintf(name, sizeof(name), "heap.alloc_class.%u.desc", id);
        struct pobj_alloc_class_desc desc = {};
        if (pmemobj_ctl_get(pm_pool, name, &desc) == 0 &&
            desc.unit_size == kSmallAllocUnitSize && desc.alignment == 0 &&
            desc.header_type == POBJ_HEADER_COMPACT) {
            small_alloc_class_id = id;
            break;
        }
    }

    if (small_alloc_class_id == 0) {
        struct pobj_alloc_class_desc desc = {};
        desc.unit_size = kSmallAllocUnitSize;
        desc.alignment = 0;
        desc.units_per_block = 1000;
        desc.header_type = POBJ_HEADER_COMPACT;
        if (pmemobj_ctl_set(pm_pool, "heap.alloc_class.new.desc", &desc) != 0) {
            fprintf(stderr,
                    "[PM-ALLOC-CLASS-ERROR] pool=%s error=%s\n",
                    pool_path.c_str(), pmemobj_errormsg());
            return false;
        }
        small_alloc_class_id = desc.class_id;
    }

    fprintf(stderr,
            "[PM-ALLOC-CLASS] small_request_max=%zu unit_bytes=%zu "
            "header=compact class_id=%u\n",
            kSmallAllocMaxSize, kSmallAllocUnitSize, small_alloc_class_id);
    return true;
}

void create_pool(const std::string& dir, const size_t& size) {
    pool_path = dir;
    printf("Attempting to open NVM pool\n");
    pm_pool = pmemobj_open(dir.data(), LAYOUT_NAME);
    const bool opened_existing = pm_pool != nullptr;

    if (pm_pool == nullptr) {
    
        pm_pool = pmemobj_create(dir.data(), LAYOUT_NAME, size, 0666);
        
        if (pm_pool == nullptr) {
            fprintf(stderr, "[PM-POOL-ERROR] path=%s error=%s\n",
                    dir.c_str(), pmemobj_errormsg());
            perror(dir.data());
            exit(1);
        }
    } else {
        printf("NVM pool opened successfully\n");
    }

    if (!ConfigureSmallAllocations()) {
        pmemobj_close(pm_pool);
        pm_pool = nullptr;
        exit(1);
    }

    struct stat pool_stat;
    if (stat(dir.c_str(), &pool_stat) == 0) {
        fprintf(stderr,
                "[PM-POOL] path=%s mode=%s file_bytes=%llu "
                "requested_create_bytes=%zu\n",
                dir.c_str(), opened_existing ? "open-existing" : "create-new",
                static_cast<unsigned long long>(pool_stat.st_size), size);
    }
}

void close_pool() {
   // fprintf(stdout, "pmem allocs %lu\\n", allocs);
    pmemobj_close(pm_pool);
}

void pfree(void* ptr) {
    PMEMoid oid = pmemobj_oid(ptr); 
    if (OID_IS_NULL(oid)) {
        return; // Don't attempt to free a null pointer.
    } 
    pmemobj_free(&oid);
}

void* pmalloc(size_t size) {
    std::lock_guard<std::mutex> lock(pmem_mutex);
    allocs++;

    if (pm_pool == nullptr) {
        fprintf(stderr,
                "[PM-ALLOC-FAILED] pool=%s request_bytes=%zu: pool is not open\n",
                pool_path.c_str(), size);
        fflush(stderr);
        errno = EINVAL;
        return nullptr;
    }

    PMEMoid oid;
    unsigned allocation_class_id = 0;
    int allocation_result;
    if (size > 0 && size <= kSmallAllocMaxSize) {
        allocation_class_id = small_alloc_class_id;
        const uint64_t flags = POBJ_XALLOC_ZERO |
                               POBJ_CLASS_ID(allocation_class_id);
        allocation_result = pmemobj_xalloc(pm_pool, &oid, size, 0, flags,
                                          nullptr, nullptr);
    } else {
        allocation_result = pmemobj_zalloc(pm_pool, &oid, size, 0);
    }
    if (allocation_result != 0) {
        const int allocation_errno = errno;
        fprintf(stderr,
                "[PM-ALLOC-FAILED] pool=%s request_bytes=%zu "
                "class_id=%u allocation_attempt=%llu errno=%d (%s) pmdk=%s\n",
                pool_path.c_str(), size, allocation_class_id,
                static_cast<unsigned long long>(allocs), allocation_errno,
                std::strerror(allocation_errno), pmemobj_errormsg());
        fflush(stderr);
        errno = allocation_errno;
        return nullptr;
    }

    return pmemobj_direct(oid);
}


}
}
