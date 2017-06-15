#include "lfalloc_helpers.h"

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>

#include <library/malloc/api/malloc.h>

#include <thread>
#include <mutex>

#include <dlfcn.h>

namespace NYT {
namespace NLFAlloc {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static std::once_flag BindWithMallocInfoFlag;
static NMalloc::TMallocInfo (*DynamicMallocInfo)() = nullptr;

static std::once_flag BindWithLFAllocFlag;
static i64 (*DynamicGetLFAllocCounterFast)(int) = nullptr;
static i64 (*DynamicGetLFAllocCounterFull)(int) = nullptr;

static void BindWithMallocInfoImpl()
{
#ifdef _unix_
    DynamicMallocInfo = (decltype(DynamicMallocInfo))dlsym(nullptr, "_ZN7NMalloc10MallocInfoEv");
#endif
}

static void BindWithMallocInfoOnce()
{
    std::call_once(BindWithMallocInfoFlag, &BindWithMallocInfoImpl);
}

// These two helpers allow us to operate with allocators that do not implement
// NMalloc::MallocInfo().

const void* SafeMallocGetParam(const char* param)
{
    BindWithMallocInfoOnce();
    if (DynamicMallocInfo) {
        auto&& mallocInfo = DynamicMallocInfo();
        return reinterpret_cast<const void*>(mallocInfo.GetParam(param));
    }
    return nullptr;
}

void SafeMallocSetParam(const char* param, const void* value)
{
    BindWithMallocInfoOnce();
    if (DynamicMallocInfo) {
        auto&& mallocInfo = DynamicMallocInfo();
        mallocInfo.SetParam(param, reinterpret_cast<const char*>(value));
    }
}

static i64 DummyGetLFAllocCounter(int)
{
    return 0;
}

static void BindWithLFAllocImpl()
{
    DynamicGetLFAllocCounterFast = (decltype(DynamicGetLFAllocCounterFast))SafeMallocGetParam("GetLFAllocCounterFast");
    DynamicGetLFAllocCounterFull = (decltype(DynamicGetLFAllocCounterFull))SafeMallocGetParam("GetLFAllocCounterFull");
    if (!DynamicGetLFAllocCounterFast) {
        DynamicGetLFAllocCounterFast = &DummyGetLFAllocCounter;
    }
    if (!DynamicGetLFAllocCounterFull) {
        DynamicGetLFAllocCounterFull = &DummyGetLFAllocCounter;
    }
}

static void BindWithLFAllocOnce()
{
    std::call_once(BindWithLFAllocFlag, &BindWithLFAllocImpl);
}

// Copied from library/lfalloc.
enum {
    CT_USER_ALLOC,      // accumulated size requested by user code
    CT_MMAP,            // accumulated mmapped size
    CT_MMAP_CNT,        // number of mmapped regions
    CT_MUNMAP,          // accumulated unmmapped size
    CT_MUNMAP_CNT,      // number of munmaped regions
    CT_SYSTEM_ALLOC,    // accumulated allocated size for internal lfalloc needs
    CT_SYSTEM_FREE,     // accumulated deallocated size for internal lfalloc needs
    CT_SMALL_ALLOC,     // accumulated allocated size for fixed-size blocks
    CT_SMALL_FREE,      // accumulated deallocated size for fixed-size blocks
    CT_LARGE_ALLOC,     // accumulated allocated size for large blocks
    CT_LARGE_FREE,      // accumulated deallocated size for large blocks
    CT_MAX
};

void SetBufferSize(i64 size)
{
    BindWithLFAllocOnce();
    auto sizeAsString = ToString(size);
    SafeMallocSetParam("LB_LIMIT_TOTAL_SIZE_BYTES", sizeAsString.c_str());
}

void SetEnableDefrag(bool flag)
{
    BindWithLFAllocOnce();
    if (flag) {
        SafeMallocSetParam("EnableDefrag", "true");
    } else {
        SafeMallocSetParam("EnableDefrag", "false");
    }
}

i64 GetCurrentUsed()
{
    BindWithLFAllocOnce();
    return GetCurrentLargeBlocks() + GetCurrentSmallBlocks() + GetCurrentSystem();
}

i64 GetCurrentMmapped()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_MMAP) - DynamicGetLFAllocCounterFull(CT_MUNMAP);
}

i64 GetCurrentMmappedCount()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_MMAP_CNT) - DynamicGetLFAllocCounterFull(CT_MUNMAP_CNT);
}

i64 GetCurrentLargeBlocks()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_LARGE_ALLOC) - DynamicGetLFAllocCounterFull(CT_LARGE_FREE);
}

i64 GetCurrentSmallBlocks()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_SMALL_ALLOC) - DynamicGetLFAllocCounterFull(CT_SMALL_FREE);
}

i64 GetCurrentSystem()
{
    BindWithLFAllocOnce();
    return DynamicGetLFAllocCounterFull(CT_SYSTEM_ALLOC) - DynamicGetLFAllocCounterFull(CT_SYSTEM_FREE);
}

#define IMPL(fn, ct) \
i64 fn() { BindWithLFAllocOnce(); return DynamicGetLFAllocCounterFull(ct); }
IMPL(GetUserAllocated, CT_USER_ALLOC);
IMPL(GetMmapped, CT_MMAP);
IMPL(GetMmappedCount, CT_MMAP_CNT);
IMPL(GetMunmapped, CT_MUNMAP);
IMPL(GetMunmappedCount, CT_MUNMAP_CNT);
IMPL(GetSystemAllocated, CT_SYSTEM_ALLOC);
IMPL(GetSystemFreed, CT_SYSTEM_FREE);
IMPL(GetSmallBlocksAllocated, CT_SMALL_ALLOC);
IMPL(GetSmallBlocksFreed, CT_SMALL_FREE);
IMPL(GetLargeBlocksAllocated, CT_LARGE_ALLOC);
IMPL(GetLargeBlocksFreed, CT_LARGE_FREE);
#undef IMPL

////////////////////////////////////////////////////////////////////////////////

class TLFAllocProfiler::TImpl
    : public TRefCounted
{
public:
    TImpl()
        : Executor_(New<NConcurrency::TPeriodicExecutor>(
            NProfiling::TProfileManager::Get()->GetInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            TDuration::Seconds(1)))
        , Profiler("/lf_alloc")
    {
        Executor_->Start();
    }

private:
    const NConcurrency::TPeriodicExecutorPtr Executor_;
    const NProfiling::TProfiler Profiler;

    void OnProfiling()
    {
        Profiler.Enqueue("/total/user_allocated", GetUserAllocated(), EMetricType::Counter);
        Profiler.Enqueue("/total/mmapped", GetMmapped(), EMetricType::Counter);
        Profiler.Enqueue("/total/mmapped_count", GetMmappedCount(), EMetricType::Counter);
        Profiler.Enqueue("/total/munmapped", GetMunmapped(), EMetricType::Counter);
        Profiler.Enqueue("/total/munmapped_count", GetMunmappedCount(), EMetricType::Counter);
        Profiler.Enqueue("/total/system_allocated", GetSystemAllocated(), EMetricType::Counter);
        Profiler.Enqueue("/total/system_deallocated", GetSystemFreed(), EMetricType::Counter);
        Profiler.Enqueue("/total/small_blocks_allocated", GetSmallBlocksAllocated(), EMetricType::Counter);
        Profiler.Enqueue("/total/small_blocks_deallocated", GetSmallBlocksFreed(), EMetricType::Counter);
        Profiler.Enqueue("/total/large_blocks_allocated", GetLargeBlocksAllocated(), EMetricType::Counter);
        Profiler.Enqueue("/total/large_blocks_deallocated", GetLargeBlocksFreed(), EMetricType::Counter);

        Profiler.Enqueue("/current/system", GetCurrentSystem(), EMetricType::Gauge);
        Profiler.Enqueue("/current/small_blocks", GetCurrentSmallBlocks(), EMetricType::Gauge);
        Profiler.Enqueue("/current/large_blocks", GetCurrentLargeBlocks(), EMetricType::Gauge);

        auto mmapped = GetCurrentMmapped();
        Profiler.Enqueue("/current/mmapped", mmapped, EMetricType::Gauge);

        auto mmappedCount = GetCurrentMmappedCount();
        Profiler.Enqueue("/current/mmapped_count", mmappedCount, EMetricType::Gauge);

        auto used = GetCurrentUsed();
        Profiler.Enqueue("/current/used", used, EMetricType::Gauge);
        Profiler.Enqueue("/current/locked", mmapped - used, EMetricType::Gauge);
    }
};

TLFAllocProfiler::TLFAllocProfiler()
    : Impl_(New<TImpl>())
{ }

TLFAllocProfiler::~TLFAllocProfiler() = default;

////////////////////////////////////////////////////////////////////////////////

} // namespace NLFAlloc
} // namespace NYT
