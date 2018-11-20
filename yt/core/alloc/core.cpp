// This file contains the core parts of YTAlloc but no malloc/free-bridge.
// The latter bridge is placed into alloc.cpp, which includes (sic!) core.cpp.
// This ensures that YTAlloc/YTFree calls are properly inlined into malloc/free.
// Also core.cpp can be directly included in, e.g., benchmarks.

#include "alloc.h"

#include <util/system/tls.h>
#include <util/system/align.h>
#include <util/system/event.h>

#include <util/generic/singleton.h>

#include <util/string/vector.h>

#include <yt/core/misc/size_literals.h>
#include <yt/core/misc/intrusive_linked_list.h>
#include <yt/core/misc/memory_tag.h>
#include <yt/core/misc/align.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>

#include <yt/core/concurrency/fork_aware_spinlock.h>

#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <atomic>
#include <array>
#include <vector>
#include <mutex>

#include <sys/mman.h>

#ifdef _linux_
#include <sys/utsname.h>
#endif

#include <errno.h>
#include <pthread.h>

#ifndef MAP_POPULATE
#define MAP_POPULATE 0x08000
#endif

#ifndef MADV_POPULATE
#define MADV_POPULATE 0x59410003
#endif

#ifndef MADV_STOCKPILE
#define MADV_STOCKPILE 0x59410004
#endif

#ifndef MADV_FREE
#define MADV_FREE 8
#endif

#ifndef NDEBUG
#define PARANOID
#endif

#ifdef PARANOID
#define PARANOID_CHECK(condition) YCHECK(condition)
#else
#define PARANOID_CHECK(condition) (void)(0)
#endif

namespace NYT {
namespace NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

// Allocations are classified into three types:
//
// a) Small chunks (less than LargeSizeThreshold)
// These are the fastest and are extensively cached (both per-thread and globally).
// Memory claimed for these allocations is never reclaimed back.
// Code dealing with such allocations is heavy optimized with all hot paths
// as streamlined as possible. The implementation is mostly inspired by LFAlloc.
//
// b) Large blobs (from LargeSizeThreshold to HugeSizeThreshold)
// These are cached as well. We expect such allocations to be less frequent
// than small ones but still do our best to provide good scalability.
// In particular, thread-sharded concurrent data structures as used to provide access to
// cached blobs. Memory is claimed via madvise(MADV_POPULATE) and relcaimed back
// via madvise(MADV_FREE).
//
// c) Huge blobs (from HugeSizeThreshold).
// These should be rare; we delegate directly to mmap and munmap for each allocation.
//
// We also provide a separate allocator for all system allocations (that are needed by YTAlloc itself).
// These are rare and also delegate to mmap/unmap.

using ::AlignUp;

// Periods between background activities.
constexpr auto BackgroundInterval = TDuration::Seconds(1);
constexpr auto StockpileInterval = TDuration::MilliSeconds(10);

constexpr size_t StockpileSize = 1_GB;

constexpr size_t PageSize = 4_KB;
constexpr size_t ZoneSize = 1_TB;

constexpr size_t MinLargeRank = 15;
constexpr size_t LargeSizeThreshold = 32_KB;
constexpr size_t MaxCachedChunksPerRank = 256;

constexpr uintptr_t UntaggedSmallZonesStart = 0;
constexpr uintptr_t UntaggedSmallZonesEnd = UntaggedSmallZonesStart + 32 * ZoneSize;
constexpr uintptr_t MinUntaggedSmallPtr = UntaggedSmallZonesStart + ZoneSize * 1;
constexpr uintptr_t MaxUntaggedSmallPtr = UntaggedSmallZonesStart + ZoneSize * SmallRankCount;

constexpr uintptr_t TaggedSmallZonesStart = UntaggedSmallZonesEnd;
constexpr uintptr_t TaggedSmallZonesEnd = TaggedSmallZonesStart + 32 * ZoneSize;
constexpr uintptr_t MinTaggedSmallPtr = TaggedSmallZonesStart + ZoneSize * 1;
constexpr uintptr_t MaxTaggedSmallPtr = TaggedSmallZonesStart + ZoneSize * SmallRankCount;

constexpr uintptr_t LargeZoneStart = TaggedSmallZonesEnd;
constexpr uintptr_t LargeZoneEnd = LargeZoneStart + ZoneSize;

constexpr uintptr_t HugeZoneStart = LargeZoneEnd;
constexpr uintptr_t HugeZoneEnd = HugeZoneStart + ZoneSize;

constexpr uintptr_t SystemZoneStart = HugeZoneStart + ZoneSize;
constexpr uintptr_t SystemZoneEnd = SystemZoneStart + ZoneSize;

constexpr size_t SmallExtentSize = 256_MB;
constexpr size_t SmallSegmentSize = 1_MB;

constexpr size_t LargeExtentSize = 1_GB;
constexpr size_t HugeSizeThreshold = 1ULL << (LargeRankCount - 1);

constexpr const char* BackgroundThreadName = "YTAllocBack";
constexpr const char* StockpileThreadName = "YTAllocStock";
constexpr const char* LoggerCategory = "YTAlloc";
constexpr const char* ProfilerPath = "/yt_alloc";

DEFINE_ENUM(EAllocationKind,
    (Untagged)
    (Tagged)
);

// Forward declarations.
struct TThreadState;
struct TLargeArena;
struct TLargeBlobExtent;

////////////////////////////////////////////////////////////////////////////////

// Wraps an instance of T enabling its explicit construction.
template <class T>
class TBox
{
public:
    template <class... Ts>
    void Construct(Ts&&... args)
    {
        new (reinterpret_cast<T*>(&Storage_)) T(std::forward<Ts>(args)...);
#ifndef NDEBUG
        Constructed_ = true;
#endif
    }

    Y_FORCE_INLINE T* Get()
    {
#ifndef NDEBUG
        PARANOID_CHECK(Constructed_);
#endif
        return reinterpret_cast<T*>(&Storage_);
    }

    Y_FORCE_INLINE const T* Get() const
    {
#ifndef NDEBUG
        PARANOID_CHECK(Constructed_);
#endif
        return reinterpret_cast<T*>(&Storage_);
    }

    Y_FORCE_INLINE T* operator->()
    {
        return Get();
    }

    Y_FORCE_INLINE const T* operator->() const
    {
        return Get();
    }

    Y_FORCE_INLINE T& operator*()
    {
        return *Get();
    }

    Y_FORCE_INLINE const T& operator*() const
    {
        return *Get();
    }

private:
    typename std::aligned_storage<sizeof(T), alignof(T)>::type Storage_;
#ifndef NDEBUG
    bool Constructed_;
#endif
};

// Initializes all singletons.
// Safe to call multiple times.
void InitializeGlobals();

////////////////////////////////////////////////////////////////////////////////

// Maps small chunk ranks to size in bytes.
static const ui16 SmallRankToSize[SmallRankCount] = {
    0,
    16, 16,
    32, 32, 48, 64, 96, 128,
    192, 256, 384, 512, 768, 1024, 1536, 2048,
    3072, 4096, 6144, 8192, 12288, 16384, 24576, 32768,
};

// Helper array for mapping size to small chunk rank.
static const ui8 SmallSizeToRank1[65] = {
    1,
    2, 2, 4, 4,  // 16, 16, 32, 32
    5, 5, 6, 6,  // 48, 64
    7, 7, 7, 7, 8, 8, 8, 8, // 96, 128
    9, 9, 9, 9, 9, 9, 9, 9,  10, 10, 10, 10, 10, 10, 10, 10,  // 192, 256
    11, 11, 11, 11, 11, 11, 11, 11,  11, 11, 11, 11, 11, 11, 11, 11,  // 384
    12, 12, 12, 12, 12, 12, 12, 12,  12, 12, 12, 12, 12, 12, 12, 12   // 512
};

// Helper array for mapping size to small chunk rank.
static const unsigned char SmallSizeToRank2[128] = {
    12, 12, 13, 14, // 512, 512, 768, 1024
    15, 15, 16, 16, // 1536, 2048
    17, 17, 17, 17, 18, 18, 18, 18, // 3072, 4096
    19, 19, 19, 19, 19, 19, 19, 19,  20, 20, 20, 20, 20, 20, 20, 20, // 6144, 8192
    21, 21, 21, 21, 21, 21, 21, 21,  21, 21, 21, 21, 21, 21, 21, 21, // 12288
    22, 22, 22, 22, 22, 22, 22, 22,  22, 22, 22, 22, 22, 22, 22, 22, // 16384
    23, 23, 23, 23, 23, 23, 23, 23,  23, 23, 23, 23, 23, 23, 23, 23,
    23, 23, 23, 23, 23, 23, 23, 23,  23, 23, 23, 23, 23, 23, 23, 23, // 24576
    24, 24, 24, 24, 24, 24, 24, 24,  24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24,  24, 24, 24, 24, 24, 24, 24, 24, // 32768
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE size_t GetUsed(ssize_t allocated, ssize_t freed)
{
    return allocated >= freed ? static_cast<size_t>(allocated - freed) : 0;
}

template <class T>
Y_FORCE_INLINE void* HeaderToPtr(T* header)
{
    return header + 1;
}

template <class T>
Y_FORCE_INLINE T* PtrToHeader(void* ptr)
{
    return static_cast<T*>(ptr) - 1;
}

Y_FORCE_INLINE size_t PtrToSmallRank(void* ptr)
{
    return (reinterpret_cast<uintptr_t>(ptr) >> 40) & 0x1f;
}

Y_FORCE_INLINE uintptr_t PtrToSegmentIndex(void* ptr)
{
    return reinterpret_cast<uintptr_t>(ptr) / SmallSegmentSize;
}

template <class T>
static Y_FORCE_INLINE void UnalignPtr(void*& ptr)
{
    if (reinterpret_cast<uintptr_t>(ptr) % PageSize == 0) {
        reinterpret_cast<char*&>(ptr) -= PageSize - sizeof (T);
    }
    PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) % PageSize == sizeof (T));
}

template <class T>
Y_FORCE_INLINE size_t GetRawBlobSize(size_t size)
{
    return AlignUp(size + sizeof (T), PageSize);
}

Y_FORCE_INLINE size_t GetLargeRank(size_t size)
{
    size_t rank = 64 - __builtin_clzl(size);
    if (size == (1ULL << (rank - 1))) {
        --rank;
    }
    return rank;
}

Y_FORCE_INLINE void PoisonRange(void* ptr, size_t size, ui32 magic)
{
#ifdef PARANOID
    size = ::AlignUp<size_t>(size, 4);
    std::fill(static_cast<ui32*>(ptr), static_cast<ui32*>(ptr) + size / 4, magic);
#endif
}

Y_FORCE_INLINE void PoisonFreedRange(void* ptr, size_t size)
{
    PoisonRange(ptr, size, 0xdeadbeef);
}

Y_FORCE_INLINE void PoisonUninitializedRange(void* ptr, size_t size)
{
    PoisonRange(ptr, size, 0xcafebabe);
}

// Checks that the header size is divisible by 16 (as needed due to alignment restrictions).
#define CHECK_HEADER_SIZE(T) static_assert(sizeof(T) % 16 == 0, "sizeof(" #T ") % 16 != 0");

////////////////////////////////////////////////////////////////////////////////

// Background activities involve logging and pushing some profiling events;
// obviously we need a logger and a profiler for that.
// These, however, cannot be declared singletons (e.g. TBox-ed) since constructing them
// involves allocations. Rather we provide TBackgroundContext to serve as a container for
// storing such objects and pass TBackgroundContext to each method that needs them.
struct TBackgroundContext
{
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFreeListItem
{
    T* Next = nullptr;
};

// A lock-free stack of items (derived from TFreeListItem).
// Supports multiple producers and multiple consumers.
// Internally uses DCAS with tagged pointers to defeat ABA.
template <class T>
class TFreeList
{
public:
    void Put(T* item)
    {
        auto newEpoch = Epoch_++;
        for (;;) {
            auto currentHead = __atomic_load_n(&Head_, __ATOMIC_RELAXED);
            auto currentPair = Unpack(currentHead);
            item->Next = currentPair.first;
            auto newHead = Pack(item, newEpoch);
            if (__sync_bool_compare_and_swap(&Head_, currentHead, newHead)) {
                break;
            }
        }
    }

    T* Extract()
    {
        auto newEpoch = Epoch_++;
        for (;;) {
            auto currentHead = __atomic_load_n(&Head_, __ATOMIC_RELAXED);
            auto currentPair = Unpack(currentHead);
            auto* item = currentPair.first;
            if (!item) {
                return nullptr;
            }
            auto newHead = Pack(item->Next, newEpoch);
            if (__sync_bool_compare_and_swap(&Head_, currentHead, newHead)) {
                return item;
            }
        }
    }

    T* ExtractAll()
    {
        auto newEpoch = Epoch_++;
        for (;;) {
            auto currentHead = __atomic_load_n(&Head_, __ATOMIC_RELAXED);
            auto currentPair = Unpack(currentHead);
            auto* item = currentPair.first;
            auto newHead = Pack(nullptr, newEpoch);
            if (__sync_bool_compare_and_swap(&Head_, currentHead, newHead)) {
                return item;
            }
        }
    }

private:
    using TEpoch = ui64;
    using TDoubleWordAtomic = volatile unsigned __int128;

    TDoubleWordAtomic Head_ = {0};
    std::atomic<TEpoch> Epoch_ = {0};

    // Avoid false sharing.
    char Padding[40];

private:
    static std::pair<T*, TEpoch> Unpack(TDoubleWordAtomic value)
    {
        return std::make_pair(
            reinterpret_cast<T*>(value & 0xffffffffffffffff),
            static_cast<TEpoch>(value >> 64));
    }

    static TDoubleWordAtomic Pack(T* item, TEpoch epoch)
    {
        return
            reinterpret_cast<unsigned __int128>(item) |
            static_cast<unsigned __int128>(epoch) << 64;
    }
};

// 64 is the expected cache line size.
static_assert(sizeof(TFreeList<void>) == 64, "sizeof(TFreeList) != 64");

////////////////////////////////////////////////////////////////////////////////

constexpr size_t ShardCount = 16;

// Provides a context for working with sharded data structures.
// Captures an initial random shard index upon construction (indicating the shard
// where all insertions go). Maintains the current shard index (round-robin,
// indicating the shard currently used for extraction).
// Can be or be not thread-safe depending on TCounter.
template <class TCounter>
class TShardedState
{
public:
    TShardedState()
        : InitialShardIndex_(rand() % ShardCount)
        , CurrentShardIndex_(InitialShardIndex_)
    { }

    Y_FORCE_INLINE size_t GetInitialShardIndex() const
    {
        return InitialShardIndex_;
    }

    Y_FORCE_INLINE size_t GetNextShardIndex()
    {
        return ++CurrentShardIndex_ % ShardCount;
    }

private:
    const size_t InitialShardIndex_;
    TCounter CurrentShardIndex_;
};

using TLocalShardedState = TShardedState<size_t>;
using TGlobalShardedState = TShardedState<std::atomic<size_t>>;

// Implemented as a collection of free lists (each called a shard).
// One needs TShardedState to access the sharded data structure.
template <class T>
class TShardedFreeList
{
public:
    // First tries to extract an item from the initial shard;
    // if failed then proceeds to all shards in round-robin fashion.
    template <class TState>
    T* Extract(TState* state)
    {
        if (auto* item = Shards_[state->GetInitialShardIndex()].Extract()) {
            return item;
        }
        return ExtractRoundRobin(state);
    }

    // Attempts to extract an item from all shards in round-robin fashion.
    template <class TState>
    T* ExtractRoundRobin(TState* state)
    {
       for (size_t index = 0; index < ShardCount; ++index) {
            if (auto* item = Shards_[state->GetNextShardIndex()].Extract()) {
                return item;
            }
        }
        return nullptr;
    }

    // Extracts items from all shards linking them together.
    T* ExtractAll()
    {
        T* head = nullptr;
        T* tail = nullptr;
        for (auto& shard : Shards_) {
            auto* item = shard.ExtractAll();
            if (!head) {
                head = item;
            }
            if (tail) {
                PARANOID_CHECK(!tail->Next);
                tail->Next = item;
            } else {
                tail = item;
            }
            while (tail && tail->Next) {
                tail = tail->Next;
            }
        }
        return head;
    }

    template <class TState>
    void Put(TState* state, T* item)
    {
        Shards_[state->GetInitialShardIndex()].Put(item);
    }

private:
    std::array<TFreeList<T>, ShardCount> Shards_;
};

////////////////////////////////////////////////////////////////////////////////

// Holds TYAlloc control knobs.
// Thread safe.
class TConfigurationManager
{
public:
    void EnableLogging()
    {
        LoggingEnabled_.store(true);
    }

    bool IsLoggingEnabled() const
    {
        return LoggingEnabled_.load(std::memory_order_relaxed);
    }


    void EnableProfiling()
    {
        ProfilingEnabled_.store(true);
    }

    bool IsProfilingEnabled()
    {
        return ProfilingEnabled_.load(std::memory_order_relaxed);
    }


    void SetLargeUnreclaimableCoeff(double value)
    {
        LargeUnreclaimableCoeff_.store(value);
    }

    double GetLargeUnreclaimableCoeff() const
    {
        return LargeUnreclaimableCoeff_.load(std::memory_order_relaxed);
    }


    void SetLargeUnreclaimableBytes(size_t value)
    {
        LargeUnreclaimableBytes_.store(value);
    }

    size_t GetLargeUnreclaimableBytes() const
    {
        return LargeUnreclaimableBytes_.load(std::memory_order_relaxed);
    }


    void SetSlowCallWarningThreshold(TDuration value)
    {
        SlowCallWarningThreshold_.store(value.MicroSeconds());
    }

    TDuration GetSlowCallWarningThreshold() const
    {
        return TDuration::MicroSeconds(SlowCallWarningThreshold_.load());
    }

private:
    std::atomic<bool> LoggingEnabled_ = {false};
    std::atomic<bool> ProfilingEnabled_ = {false};
    std::atomic<double> LargeUnreclaimableCoeff_ = {0.05};
    std::atomic<size_t> LargeUnreclaimableBytes_ = {128_MB};
    std::atomic<ui64> SlowCallWarningThreshold_ = {10000}; // in microseconds, 10 ms by default
};

TBox<TConfigurationManager> ConfigurationManager;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETimingEventType,
    (Mmap)
    (Munmap)
    (MadvisePopulate)
    (MadviseFree)
    (MadviseDontNeed)
    (Locking)
    (Prefault)
);

struct TTimingEvent
{
    ETimingEventType Type;
    TDuration Duration;
    size_t Size;
    TInstant Timestamp;
    NConcurrency::TFiberId FiberId;
};

class TTimingManager
{
public:
    void DisableForCurrentThread()
    {
        DisabledForCurrentThread_ = true;
    }

    void EnqueueEvent(ETimingEventType type, TDuration duration, size_t size = 0)
    {
        if (DisabledForCurrentThread_) {
            return;
        }
        auto timestamp = NProfiling::GetInstant();
        auto fiberId = NConcurrency::GetCurrentFiberId();
        auto guard = Guard(EventLock_);

        auto& counters = EventCounters_[type];
        counters.Count += 1;
        counters.Size += size;

        if (EventCount_ >= EventBufferSize) {
            return;
        }
        
        Events_[EventCount_++] = {
            type,
            duration,
            size,
            timestamp,
            fiberId
        };
    }

    void RunBackgroundTasks(const TBackgroundContext& context)
    {
        const auto& Logger = context.Logger;
        if (Logger) {
            for (const auto& event : PullEvents()) {
                LOG_DEBUG("Timing event logged (Type: %v, Duration: %v, Size: %v, Timestamp: %v, FiberId: %llx)",
                    event.Type,
                    event.Duration,
                    event.Size,
                    event.Timestamp,
                    event.FiberId);
            }
        }

        if (context.Profiler.GetEnabled()) {
            for (auto type : TEnumTraits<ETimingEventType>::GetDomainValues()) {
                NProfiling::TProfiler profiler(
                    context.Profiler.GetPathPrefix() + "/timing_events",
                    {
                        NProfiling::TProfileManager::Get()->RegisterTag("type", type)
                    });
                auto& counters = EventCounters_[type];
                profiler.Enqueue("/count", counters.Count, NProfiling::EMetricType::Gauge);
                profiler.Enqueue("/size", counters.Size, NProfiling::EMetricType::Gauge);
            }
        }
    }

private:
    static constexpr size_t EventBufferSize = 1000;
    NConcurrency::TForkAwareSpinLock EventLock_;
    size_t EventCount_ = 0;
    std::array<TTimingEvent, EventBufferSize> Events_;

    Y_POD_STATIC_THREAD(bool) DisabledForCurrentThread_;

    struct TPerEventTimeCounters
    {
        size_t Count = 0;
        size_t Size = 0;
    };
    TEnumIndexedVector<TPerEventTimeCounters, ETimingEventType> EventCounters_;

private:
    std::vector<TTimingEvent> PullEvents()
    {
        std::vector<TTimingEvent> events;
        events.reserve(EventBufferSize);

        auto guard = Guard(EventLock_);
        for (size_t index = 0; index < EventCount_; ++index) {
            events.push_back(Events_[index]);
        }
        EventCount_ = 0;
        return events;
    }
};

Y_POD_THREAD(bool) TTimingManager::DisabledForCurrentThread_;

TBox<TTimingManager> TimingManager;

// Used to log statistics about long-running syscalls and lock acquisitions.
// Maintains recursion depth and execution stats in TLS.
// Recursion depth counter ensures that logging only happens
// when the topmost guard is being destroyed and thus YTAlloc does not invoke itself in
// an unexpected way.
class TTimingGuard
    : public TNonCopyable
{
public:
    explicit TTimingGuard(ETimingEventType eventType, size_t size = 0)
        : EventType_(eventType)
        , Size_(size)
        // Sadly, TWallTimer cannot be used prior to all statics being initialized.
        , StartTime_(ConfigurationManager->IsLoggingEnabled() ? NProfiling::GetCpuInstant() : 0)
    { }

    ~TTimingGuard()
    {
        if (StartTime_ == 0) {
            return;
        }

        auto endTime = NProfiling::GetCpuInstant();
        auto duration = NProfiling::CpuDurationToDuration(endTime - StartTime_);
        if (duration > ConfigurationManager->GetSlowCallWarningThreshold()) {
            TimingManager->EnqueueEvent(EventType_, duration, Size_);
        };
    }

private:
    const ETimingEventType EventType_;
    const size_t Size_;
    const NProfiling::TCpuInstant StartTime_;
};

template <class T>
Y_FORCE_INLINE TGuard<T> GuardWithTiming(const T& lock)
{
    TTimingGuard timingGuard(ETimingEventType::Locking);
    TGuard<T> lockingGuard(lock);
    return lockingGuard;
}

////////////////////////////////////////////////////////////////////////////////

// A wrapper for mmap, mumap, and madvise calls.
// The latter are invoked with MADV_POPULATE and MADV_FREE flags
// and may fail if the OS support is missing. These failures are logged (once) and
// handled as follows:
// * if MADV_POPULATE fails then we fallback to manual per-page prefault
// for all subsequent attempts;
// * if MADV_FREE fails then it (and all subsequent attempts) is replaced with MADV_DONTNEED
// (which is non-lazy and is less efficient but will somehow do).
// Also this class mlocks all VMAs on startup to prevent pagefaults in our heavy binaries
// from disturbing latency tails.
class TMappedMemoryManager
{
public:
    TMappedMemoryManager()
    {
        if (::mlockall(MCL_CURRENT) != 0) {
            MlockallFailedLogged_ = true;
        }
    }

    void* Map(uintptr_t hint, size_t size, int flags)
    {
        TTimingGuard timingGuard(ETimingEventType::Mmap, size);
        auto* result = ::mmap(
            reinterpret_cast<void*>(hint),
            size,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | flags,
            -1,
            0);
        if (result == MAP_FAILED) {
            auto error = errno;
            if (error == ENOMEM) {
                OnOOM();
            }
            Y_UNREACHABLE();
        }
        return result;
    }

    void Unmap(void* ptr, size_t size)
    {
        TTimingGuard timingGuard(ETimingEventType::Munmap, size);
        auto result = ::munmap(ptr, size);
        YCHECK(result == 0);
    }

    void Populate(void* ptr, size_t size)
    {
        if (PopulateUnavailable_.load(std::memory_order_relaxed)) {
            DoPrefault(ptr, size);
        } else if (!TryMadvisePopulate(ptr, size)) {
            PopulateUnavailable_.store(true);
            DoPrefault(ptr, size);
        }
    }

    void Release(void* ptr, size_t size)
    {
        if (FreeUnavailable_.load(std::memory_order_relaxed)) {
            DoMadviseDontNeed(ptr, size);
        } else if (!TryMadviseFree(ptr, size)) {
            FreeUnavailable_.store(true);
            DoMadviseDontNeed(ptr, size);
        }
    }

    bool Stockpile(size_t size)
    {
        if (StockpileUnavailable_.load(std::memory_order_relaxed)) {
            return false;
        }
        if (!TryMadviseStockpile(size)) {
            StockpileUnavailable_.store(true);
            return false;
        }
        return true;
    }

    void RunBackgroundTasks(const TBackgroundContext& context)
    {
        const auto& Logger = context.Logger;
        if (!Logger) {
            return;
        }
        if (IsBuggyKernel() && !BuggyKernelLogged_) {
            LOG_WARNING("Kernel is buggy; see KERNEL-118");
            BuggyKernelLogged_ = true;
        }
        if (MlockallFailed_ && !MlockallFailedLogged_) {
            LOG_WARNING("Failed lock process memory");
            MlockallFailedLogged_ = true;
        }
        if (PopulateUnavailable_.load() && !PopulateUnavailableLogged_) {
            LOG_WARNING("MADV_POPULATE is not supported");
            PopulateUnavailableLogged_ = true;
        }
        if (FreeUnavailable_.load() && !FreeUnavailableLogged_) {
            LOG_WARNING("MADV_FREE is not supported");
            FreeUnavailableLogged_ = true;
        }
        if (StockpileUnavailable_.load() && !StockpileUnavailableLogged_) {
            LOG_WARNING("MADV_STOCKPILE is not supported");
            StockpileUnavailableLogged_ = true;
        }
    }

private:
    bool BuggyKernelLogged_ = false;

    bool MlockallFailed_ = false;
    bool MlockallFailedLogged_ = false;

    std::atomic<bool> PopulateUnavailable_ = {false};
    bool PopulateUnavailableLogged_ = false;

    std::atomic<bool> FreeUnavailable_ = {false};
    bool FreeUnavailableLogged_ = false;

    std::atomic<bool> StockpileUnavailable_ = {false};
    bool StockpileUnavailableLogged_ = false;

private:
    bool TryMadvisePopulate(void* ptr, size_t size)
    {
        TTimingGuard timingGuard(ETimingEventType::MadvisePopulate, size);
        auto result = ::madvise(ptr, size, MADV_POPULATE);
        if (result != 0) {
            auto error = errno;
            if (error == ENOMEM) {
                OnOOM();
            }
            YCHECK(error == EINVAL);
            return false;
        }
        return true;
    }

    void DoPrefault(void* ptr, size_t size)
    {
        TTimingGuard timingGuard(ETimingEventType::Prefault, size);
        auto* begin = static_cast<char*>(ptr);
        for (auto* current = begin; current < begin + size; current += PageSize) {
            *current = 0;
        }
    }

    bool TryMadviseFree(void* ptr, size_t size)
    {
        if (IsBuggyKernel()) {
            return false;
        }
        TTimingGuard timingGuard(ETimingEventType::MadviseFree, size);
        auto result = ::madvise(ptr, size, MADV_FREE);
        if (result != 0) {
            auto error = errno;
            YCHECK(error == EINVAL);
            return false;
        }
        return true;
    }

    void DoMadviseDontNeed(void* ptr, size_t size)
    {
        TTimingGuard timingGuard(ETimingEventType::MadviseDontNeed, size);
        auto result = ::madvise(ptr, size, MADV_DONTNEED);
        // Must not fail.
        YCHECK(result == 0);
    }
    
    bool TryMadviseStockpile(size_t size)
    {
        auto result = ::madvise(nullptr, size, MADV_STOCKPILE);
        if (result != 0) {
            auto error = errno;
            if (error == ENOMEM || error == EAGAIN || error == EINTR) {
                // The call is advisory, ignore ENOMEM, EAGAIN, and EINTR.
                return true;
            }
            YCHECK(error == EINVAL);
            return false;
        }
        return true;
    }

    void OnOOM()
    {
        fprintf(stderr, "YTAlloc has detected an out-of-memory condition; terminating\n");
        _exit(9);
    }

    // Some kernels are known to contain bugs in MADV_FREE; see https://st.yandex-team.ru/KERNEL-118.
    bool IsBuggyKernel()
    {
#ifdef _linux_
        static const bool result = [] () {
            struct utsname buf;
            YCHECK(uname(&buf) == 0);
            if (strverscmp(buf.release, "4.4.1-1") >= 0 &&
                strverscmp(buf.release, "4.4.96-44") < 0)
            {
                return true;
            }
            if (strverscmp(buf.release, "4.14.1-1") >= 0 &&
                strverscmp(buf.release, "4.14.79-33") < 0)
            {
                return true;
            }
            return false;
        }();
        return result;
#else
        return false;
#endif
    }
};

TBox<TMappedMemoryManager> MappedMemoryManager;

////////////////////////////////////////////////////////////////////////////////
// System allocator

// Each system allocation is prepended with such a header.
struct TSystemBlobHeader
{
    explicit TSystemBlobHeader(size_t size)
        : Size(size)
    { }

    size_t Size;
    char Padding[8];
};

CHECK_HEADER_SIZE(TSystemBlobHeader)

// Used for some internal allocations.
// Delgates directly to TMappedMemoryManager.
class TSystemAllocator
{
public:
    void* Allocate(size_t size);
    void Free(void* ptr);

private:
    std::atomic<uintptr_t> CurrentPtr_ = {SystemZoneStart};
};

TBox<TSystemAllocator> SystemAllocator;

////////////////////////////////////////////////////////////////////////////////

// Deriving from this class makes instances bound to TSystemAllocator.
struct TSystemAllocatable
{
    void* operator new(size_t size) noexcept
    {
        return SystemAllocator->Allocate(size);
    }

    void* operator new[](size_t size) noexcept
    {
        return SystemAllocator->Allocate(size);
    }

    void operator delete(void* ptr) noexcept
    {
        SystemAllocator->Free(ptr);
    }

    void operator delete[](void* ptr) noexcept
    {
        SystemAllocator->Free(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Maintains a pool of objects.
// Objects are allocated in groups each containing BatchSize instances.
// The actual allocation is carried out by TSystemAllocator.
// Memory is never actually reclaimed; freed instances are put into TFreeList.
template <class T, size_t BatchSize>
class TSystemPool
{
public:
    T* Allocate()
    {
        while (true) {
            auto* obj = FreeList_.Extract();
            if (Y_LIKELY(obj)) {
                new (obj) T();
                return obj;
            }
            AllocateMore();
        }
    }

    void Free(T* obj)
    {
        obj->T::~T();
        PoisonFreedRange(obj, sizeof(T));
        FreeList_.Put(obj);
    }

private:
    TFreeList<T> FreeList_;

private:
    void AllocateMore()
    {
        auto* objs = static_cast<T*>(SystemAllocator->Allocate(sizeof(T) * BatchSize));
        for (size_t index = 0; index < BatchSize; ++index) {
            auto* obj = objs + index;
            FreeList_.Put(obj);
        }
    }
};

// A sharded analogue TSystemPool.
template <class T, size_t BatchSize>
class TShardedSystemPool
{
public:
    template <class TState>
    T* Allocate(TState* state)
    {
        if (auto* obj = FreeLists_[state->GetInitialShardIndex()].Extract()) {
            new (obj) T();
            return obj;
        }

        while (true) {
            for (size_t index = 0; index < ShardCount; ++index) {
                if (auto* obj = FreeLists_[state->GetNextShardIndex()].Extract()) {
                    new (obj) T();
                    return obj;
                }
            }
            AllocateMore();
        }
    }

    template <class TState>
    void Free(TState* state, T* obj)
    {
        obj->T::~T();
        PoisonFreedRange(obj, sizeof(T));
        FreeLists_[state->GetInitialShardIndex()].Put(obj);
    }

private:
    std::array<TFreeList<T>, ShardCount> FreeLists_;

private:
    void AllocateMore()
    {
        auto* objs = static_cast<T*>(SystemAllocator->Allocate(sizeof(T) * BatchSize));
        for (size_t index = 0; index < BatchSize; ++index) {
            auto* obj = objs + index;
            FreeLists_[index % ShardCount].Put(obj);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Handles allocations inside a zone of memory given by its start and end pointers.
// Each allocation is a separate mapped region of memory.
// A special care is taken to guarantee that all allocated regions fall inside the zone.
class TZoneAllocator
{
public:
    TZoneAllocator(uintptr_t zoneStart, uintptr_t zoneEnd)
        : ZoneStart_(zoneStart)
        , ZoneEnd_(zoneEnd)
        , Current_(zoneStart)
    {
        YCHECK(ZoneStart_ % PageSize == 0);
    }

    void* Allocate(size_t size, int flags)
    {
        YCHECK(size % PageSize == 0);
        bool restarted = false;
        while (true) {
            auto hint = (Current_ += size) - size;
            if (reinterpret_cast<uintptr_t>(hint) + size > ZoneEnd_) {
                YCHECK(!restarted);
                restarted = true;
                Current_ = ZoneStart_;
            } else {
                char* ptr = static_cast<char*>(MappedMemoryManager->Map(hint, size, flags));
                if (reinterpret_cast<uintptr_t>(ptr) == hint) {
                    return ptr;
                }
                MappedMemoryManager->Unmap(ptr, size);
            }
        }
    }

    void Free(void* ptr, size_t size)
    {
        MappedMemoryManager->Unmap(ptr, size);
    }

private:
    const uintptr_t ZoneStart_;
    const uintptr_t ZoneEnd_;

    std::atomic<uintptr_t> Current_;
};

////////////////////////////////////////////////////////////////////////////////

// YTAlloc supports tagged allocations (see core/misc/memory_tag.h).
// Since the total number of tags can be huge, a two-level scheme is employed.
// Possible tags are arranged into sets each containing TaggedCounterSetSize tags.
// There are up to MaxTaggedCounterSets in total.

constexpr size_t TaggedCounterSetSize = 16384;
constexpr size_t MaxTaggedCounterSets = 256;

static_assert(
    MaxMemoryTag == TaggedCounterSetSize * MaxTaggedCounterSets - 1,
    "MaxMemoryTag != TaggedCounterSetSize * MaxTaggedCounterSets - 1");

template <class TCounter>
using TUntaggedTotalCounters = TEnumIndexedVector<TCounter, EBasicCounter>;

template <class TCounter>
struct TTaggedTotalCounterSet
    : public TSystemAllocatable
{
    std::array<TEnumIndexedVector<TCounter, EBasicCounter>, TaggedCounterSetSize> Counters;
};

using TLocalTaggedBasicCounterSet = TTaggedTotalCounterSet<ssize_t>;
using TGlobalTaggedBasicCounterSet = TTaggedTotalCounterSet<std::atomic<ssize_t>>;

template <class TCounter>
struct TTotalCounters
{
    // The sum of counters across all tags.
    TUntaggedTotalCounters<TCounter> CumulativeTaggedCounters;

    // Counters for untagged allocations.
    TUntaggedTotalCounters<TCounter> UntaggedCounters;

    // Access to tagged counters may involve creation of a new tag set.
    // For simplicity, we separate the read-side (TaggedCounterSets) and the write-side (TaggedCounterSetHolders).
    // These arrays contain virtually identical data (up to std::unique_ptr and std::atomic semantic differences).
    std::array<std::atomic<TTaggedTotalCounterSet<TCounter>*>, MaxTaggedCounterSets> TaggedCounterSets{};
    std::array<std::unique_ptr<TTaggedTotalCounterSet<TCounter>>, MaxTaggedCounterSets> TaggedCounterSetHolders;

    // Protects TaggedCounterSetHolders from concurrent updates.
    NConcurrency::TForkAwareSpinLock TaggedCounterSetsLock;

    // Returns null if the set is not yet constructed.
    Y_FORCE_INLINE TTaggedTotalCounterSet<TCounter>* FindTaggedCounterSet(size_t index) const
    {
        return TaggedCounterSets[index].load();
    }

    // Constructs the set on first access.
    TTaggedTotalCounterSet<TCounter>* GetOrCreateTaggedCounterSet(size_t index)
    {
        auto* set = TaggedCounterSets[index].load();
        if (Y_LIKELY(set)) {
            return set;
        }

        auto guard = GuardWithTiming(TaggedCounterSetsLock);
        auto& setHolder = TaggedCounterSetHolders[index];
        if (!setHolder) {
            setHolder = std::make_unique<TTaggedTotalCounterSet<TCounter>>();
            TaggedCounterSets[index] = setHolder.get();
        }
        return setHolder.get();
    }
};

using TLocalSystemCounters = TEnumIndexedVector<ssize_t, ESystemCounter>;
using TGlobalSystemCounters = TEnumIndexedVector<std::atomic<ssize_t>, ESystemCounter>;

using TLocalSmallCounters = TEnumIndexedVector<ssize_t, ESmallArenaCounter>;
using TGlobalSmallCounters = TEnumIndexedVector<std::atomic<ssize_t>, ESmallArenaCounter>;

using TLocalLargeCounters = TEnumIndexedVector<ssize_t, ELargeArenaCounter>;
using TGlobalLargeCounters = TEnumIndexedVector<std::atomic<ssize_t>, ELargeArenaCounter>;

using TLocalHugeCounters = TEnumIndexedVector<ssize_t, EHugeCounter>;
using TGlobalHugeCounters = TEnumIndexedVector<std::atomic<ssize_t>, EHugeCounter>;

Y_FORCE_INLINE ssize_t LoadCounter(ssize_t counter)
{
    return counter;
}

Y_FORCE_INLINE ssize_t LoadCounter(const std::atomic<ssize_t>& counter)
{
    return counter.load();
}

////////////////////////////////////////////////////////////////////////////////

// A per-thread structure containing counters, chunk caches etc.
struct TThreadState
    : public TFreeListItem<TThreadState>
    , public TLocalShardedState
{
    // TThreadState instances of all alive threads are put into a double-linked intrusive list.
    // This is a pair of next/prev pointers connecting an instance of TThreadState to its neighbors.
    TIntrusiveLinkedListNode<TThreadState> RegistryNode;

    // TThreadStates are ref-counted.
    // TThreadManager::EnumerateThreadStates enumerates the registered states and acquires
    // a temporary reference preventing these states from being destructed. This provides
    // for shorter periods of time the global lock needs to be held.
    int RefCounter = 1;

    // Per-thread counters.
    TTotalCounters<ssize_t> TotalCounters;
    std::array<TLocalLargeCounters, LargeRankCount> LargeArenaCounters;

    // Each thread maintains caches of small chunks.
    // One cache is for tagged chunks; the other is for untagged ones.
    // Each cache contains up to MaxCachedChunksPerRank chunks per any rank.
    // Special sentinels are placed to distinguish the boundaries of region containing
    // pointers of a specific rank. This enables a tiny-bit faster inplace boundary checks.

    static constexpr uintptr_t LeftSentinel = 1;
    static constexpr uintptr_t RightSentinel = 2;

    struct TSmallBlobCache
    {
        TSmallBlobCache()
        {
            void** chunkPtrs = CachedChunks.data();
            for (size_t rank = 0; rank < SmallRankCount; ++rank) {
                RankToCachedChunkPtr[rank] = chunkPtrs;
                chunkPtrs[0] = reinterpret_cast<void*>(LeftSentinel);
                chunkPtrs[MaxCachedChunksPerRank + 1] = reinterpret_cast<void*>(RightSentinel);
                chunkPtrs += MaxCachedChunksPerRank + 2;
            }
        }

        // For each rank we have a segment of pointers in CachedChunks with the following layout:
        //   LPP[P]........R
        // Legend:
        //   .  = null pointer
        //   L  = left sentinel
        //   R  = right sentinel
        //   P  = cached pointer
        //  [P] = current cached pointer
        //
        //  +2 is for two sentinels
        std::array<void*, SmallRankCount * (MaxCachedChunksPerRank + 2)> CachedChunks{};

        // Pointer to [P] for each rank.
        std::array<void**, SmallRankCount> RankToCachedChunkPtr{};
    };
    TEnumIndexedVector<TSmallBlobCache, EAllocationKind> SmallBlobCache;
};

struct TThreadStateToRegistryNode
{
    auto operator() (TThreadState* state) const
    {
        return &state->RegistryNode;
    }
};

// Manages all registered threads and controls access to TThreadState.
class TThreadManager
{
public:
    TThreadManager()
    {
        pthread_key_create(&ThreadDtorKey_, DestroyThread);
    }

    // Returns TThreadState for the current thread; may return null.
    static TThreadState* FindThreadState();

    // Returns TThreadState for the current thread; may not return null
    // (but may crash if TThreadState is already destroyed).
    static TThreadState* GetThreadState()
    {
        auto* state = FindThreadState();
        YCHECK(state);
        return state;
    }

    // Enumerates all threads and invokes #func passing TThreadState instances.
    // #func must not throw but can take arbitrary time; no locks are being held while it executes.
    template <class F>
    void EnumerateThreadStates(F func) noexcept
    {
        TMemoryTagGuard guard(NullMemoryTag);

        SmallVector<TThreadState*, 1024> states;

        {
            // Only hold this guard for a small period of time to reference all the states.
            auto guard = GuardWithTiming(ThreadRegistryLock_);
            auto* current = ThreadRegistry_.GetFront();
            while (current) {
                RefThreadState(current);
                states.push_back(current);
                current = current->RegistryNode.Next;
            }
        }

        for (auto* state : states) {
            func(state);
        }

        {
            // Releasing references also requires global lock to be held to avoid getting zombies above.
            auto guard = GuardWithTiming(ThreadRegistryLock_);
            for (auto* state : states) {
                UnrefThreadState(state);
            }
        }
    }

    Y_FORCE_INLINE static TMemoryTag GetCurrentMemoryTag()
    {
        return CurrentMemoryTag_;
    }

    Y_FORCE_INLINE static void SetCurrentMemoryTag(TMemoryTag tag)
    {
        YCHECK(tag <= MaxMemoryTag);
        CurrentMemoryTag_ = tag;
    }

private:
    static void DestroyThread(void*);

    TThreadState* AllocateThreadState()
    {
        auto* state = ThreadStatePool_.Allocate();

        {
            auto guard = GuardWithTiming(ThreadRegistryLock_);
            ThreadRegistry_.PushBack(state);
        }

        // Need to pass some non-null value for DestroyThread to be called.
        pthread_setspecific(ThreadDtorKey_, (void*)-1);

        return state;
    }

    void RefThreadState(TThreadState* state)
    {
        auto result = ++state->RefCounter;
        YCHECK(result > 1);
    }

    void UnrefThreadState(TThreadState* state)
    {
        auto result = --state->RefCounter;
        YCHECK(result >= 0);
        if (result == 0) {
            DestroyThreadState(state);
        }
    }

    void DestroyThreadState(TThreadState* state);

private:
    // TThreadState instance for the current thread.
    // Initially null, then initialized when first needed.
    // TThreadState is destroyed upon thread termination (which is detected with
    // the help of pthread_key_create machinery), so this pointer can become null again.
    Y_POD_STATIC_THREAD(TThreadState*) ThreadState_;

    // Initially false, then set to true then TThreadState is destroyed.
    // If the thread requests for its state afterwards, null is returned and no new state is (re-)created.
    // The caller must be able to deal with it.
    Y_POD_STATIC_THREAD(bool) ThreadStateDestroyed_;

    // See tagged allocations API.
    Y_POD_STATIC_THREAD(TMemoryTag) CurrentMemoryTag_;

    pthread_key_t ThreadDtorKey_;

    static constexpr size_t ThreadStatesBatchSize = 16;
    TSystemPool<TThreadState, ThreadStatesBatchSize> ThreadStatePool_;

    NConcurrency::TForkAwareSpinLock ThreadRegistryLock_;
    TIntrusiveLinkedList<TThreadState, TThreadStateToRegistryNode> ThreadRegistry_;
};

Y_POD_THREAD(TThreadState*) TThreadManager::ThreadState_;
Y_POD_THREAD(bool) TThreadManager::ThreadStateDestroyed_;
Y_POD_THREAD(TMemoryTag) TThreadManager::CurrentMemoryTag_;

TBox<TThreadManager> ThreadManager;

////////////////////////////////////////////////////////////////////////////////

// Mimics the counters of TThreadState but uses std::atomic to survive concurrent access.
struct TGlobalState
    : public TGlobalShardedState
{
    TTotalCounters<std::atomic<ssize_t>> TotalCounters;
    std::array<TGlobalLargeCounters, LargeRankCount> LargeArenaCounters;
};

TBox<TGlobalState> GlobalState;

////////////////////////////////////////////////////////////////////////////////

// Accumulates various allocation statistics.
class TStatisticsManager
{
public:
    template <EAllocationKind Kind = EAllocationKind::Tagged, class TState>
    static Y_FORCE_INLINE void IncrementTotalCounter(TState* state, TMemoryTag tag, EBasicCounter counter, ssize_t delta)
    {
        // This branch is typically resolved at compile time.
        if (Kind == EAllocationKind::Tagged && tag != NullMemoryTag) {
            IncrementTaggedTotalCounter(&state->TotalCounters, tag, counter, delta);
        } else {
            IncrementUntaggedTotalCounter(&state->TotalCounters, counter, delta);
        }
    }

    static Y_FORCE_INLINE void IncrementTotalCounter(TMemoryTag tag, EBasicCounter counter, ssize_t delta)
    {
        IncrementTotalCounter(GlobalState.Get(), tag, counter, delta);
    }

    void IncrementSmallArenaCounter(ESmallArenaCounter counter, size_t rank, ssize_t delta)
    {
        SmallArenaCounters_[rank][counter] += delta;
    }

    template <class TState>
    static Y_FORCE_INLINE void IncrementLargeArenaCounter(TState* state, size_t rank, ELargeArenaCounter counter, ssize_t delta)
    {
        state->LargeArenaCounters[rank][counter] += delta;
    }

    void IncrementHugeCounter(EHugeCounter counter, ssize_t delta)
    {
        HugeCounters_[counter] += delta;
    }

    void IncrementSystemCounter(ESystemCounter counter, ssize_t delta)
    {
        SystemCounters_[counter] += delta;
    }

    // Computes memory usage for a list of tags by aggregating counters across threads.
    void GetTaggedMemoryUsage(TRange<TMemoryTag> tags, size_t* usage)
    {
        TMemoryTagGuard guard(NullMemoryTag);

        SmallVector<size_t, 64> bytesAllocated(tags.Size());
        SmallVector<size_t, 64> bytesFreed(tags.Size());

        for (size_t index = 0; index < tags.Size(); ++index) {
            auto tag = tags[index];
            bytesAllocated[index] += LoadTaggedTotalCounter(GlobalState->TotalCounters, tag, EBasicCounter::BytesAllocated);
            bytesFreed[index] += LoadTaggedTotalCounter(GlobalState->TotalCounters, tag, EBasicCounter::BytesFreed);
        }

        ThreadManager->EnumerateThreadStates(
            [&] (const auto* state) {
                for (size_t index = 0; index < tags.Size(); ++index) {
                    auto tag = tags[index];
                    bytesAllocated[index] += LoadTaggedTotalCounter(state->TotalCounters, tag, EBasicCounter::BytesAllocated);
                    bytesFreed[index] += LoadTaggedTotalCounter(state->TotalCounters, tag, EBasicCounter::BytesFreed);
                }
            });

        for (size_t index = 0; index < tags.Size(); ++index) {
            usage[index] = GetUsed(bytesAllocated[index], bytesFreed[index]);
        }
    }

    TEnumIndexedVector<ssize_t, ETotalCounter> GetTotalCounters()
    {
        TEnumIndexedVector<ssize_t, ETotalCounter> result;

        auto accumulate = [&] (const auto& counters) {
            result[ETotalCounter::BytesAllocated] += LoadCounter(counters[EBasicCounter::BytesAllocated]);
            result[ETotalCounter::BytesFreed] += LoadCounter(counters[EBasicCounter::BytesFreed]);
        };

        accumulate(GlobalState->TotalCounters.UntaggedCounters);
        accumulate(GlobalState->TotalCounters.CumulativeTaggedCounters);

        ThreadManager->EnumerateThreadStates(
            [&] (const auto* state) {
                accumulate(state->TotalCounters.UntaggedCounters);
                accumulate(state->TotalCounters.CumulativeTaggedCounters);
            });

        result[ETotalCounter::BytesUsed] = GetUsed(
            result[ETotalCounter::BytesAllocated],
            result[ETotalCounter::BytesFreed]);

        auto systemCounters = GetSystemCounters();
        result[ETotalCounter::BytesCommitted] += systemCounters[EBasicCounter::BytesUsed];

        auto hugeCounters = GetHugeCounters();
        result[ETotalCounter::BytesCommitted] += hugeCounters[EHugeCounter::BytesUsed];

        auto smallArenaCounters = GetSmallArenaCounters();
        for (size_t rank = 0; rank < SmallRankCount; ++rank) {
            result[ETotalCounter::BytesCommitted] += smallArenaCounters[rank][ESmallArenaCounter::BytesCommitted];
        }

        auto largeArenaCounters = GetLargeArenaCounters();
        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            result[ETotalCounter::BytesCommitted] += largeArenaCounters[rank][ELargeArenaCounter::BytesCommitted];
        }

        auto rss = GetProcessMemoryUsage().Rss;
        result[ETotalCounter::BytesUnaccounted] = std::max<ssize_t>(static_cast<ssize_t>(rss) - result[ETotalCounter::BytesCommitted], 0);

        return result;
    }

    TEnumIndexedVector<ssize_t, ESmallCounter> GetSmallCounters()
    {
        TEnumIndexedVector<ssize_t, ESmallCounter> result;

        auto totalCounters = GetTotalCounters();
        result[ESmallCounter::BytesAllocated] = totalCounters[ETotalCounter::BytesAllocated];
        result[ESmallCounter::BytesFreed] = totalCounters[ETotalCounter::BytesFreed];
        result[ESmallCounter::BytesUsed] = totalCounters[ETotalCounter::BytesUsed];
        
        auto largeArenaCounters = GetLargeArenaCounters();
        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            result[ESmallCounter::BytesAllocated] -= largeArenaCounters[rank][ELargeArenaCounter::BytesAllocated];
            result[ESmallCounter::BytesFreed] -= largeArenaCounters[rank][ELargeArenaCounter::BytesFreed];
            result[ESmallCounter::BytesUsed] -= largeArenaCounters[rank][ELargeArenaCounter::BytesUsed];
        }
        
        auto hugeCounters = GetHugeCounters();
        result[ESmallCounter::BytesAllocated] -= hugeCounters[EHugeCounter::BytesAllocated];
        result[ESmallCounter::BytesFreed] -= hugeCounters[EHugeCounter::BytesFreed];
        result[ESmallCounter::BytesUsed] -= hugeCounters[EHugeCounter::BytesUsed];
        
        return result;
    }

    std::array<TLocalSmallCounters, SmallRankCount> GetSmallArenaCounters()
    {
        std::array<TLocalSmallCounters, SmallRankCount> result;
        for (size_t rank = 0; rank < SmallRankCount; ++rank) {
            for (auto counter : TEnumTraits<ESmallArenaCounter>::GetDomainValues()) {
                result[rank][counter] = SmallArenaCounters_[rank][counter].load();
            }
        }
        return result;
    }

    TEnumIndexedVector<ssize_t, ELargeCounter> GetLargeCounters()
    {
        TEnumIndexedVector<ssize_t, ELargeCounter> result;
        auto largeArenaCounters = GetLargeArenaCounters();
        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            result[ESmallCounter::BytesAllocated] += largeArenaCounters[rank][ELargeArenaCounter::BytesAllocated];
            result[ESmallCounter::BytesFreed] += largeArenaCounters[rank][ELargeArenaCounter::BytesFreed];
            result[ESmallCounter::BytesUsed] += largeArenaCounters[rank][ELargeArenaCounter::BytesUsed];
        }
        return result;
    }

    std::array<TLocalLargeCounters, LargeRankCount> GetLargeArenaCounters()
    {
        std::array<TLocalLargeCounters, LargeRankCount> result{};

        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            for (auto counter : TEnumTraits<ELargeArenaCounter>::GetDomainValues()) {
                result[rank][counter] = GlobalState->LargeArenaCounters[rank][counter].load();
            }
        }

        ThreadManager->EnumerateThreadStates(
            [&] (const auto* state) {
                for (size_t rank = 0; rank < LargeRankCount; ++rank) {
                    for (auto counter : TEnumTraits<ELargeArenaCounter>::GetDomainValues()) {
                        result[rank][counter] += state->LargeArenaCounters[rank][counter];
                    }
                }
            });

        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            result[rank][ELargeArenaCounter::BytesUsed] = GetUsed(result[rank][ELargeArenaCounter::BytesAllocated], result[rank][ELargeArenaCounter::BytesFreed]);
            result[rank][ELargeArenaCounter::BlobsUsed] = GetUsed(result[rank][ELargeArenaCounter::BlobsAllocated], result[rank][ELargeArenaCounter::BlobsFreed]);
        }

        return result;
    }

    // Returns system counters.
    TLocalSystemCounters GetSystemCounters()
    {
        TLocalSystemCounters result;
        for (auto counter : TEnumTraits<ESystemCounter>::GetDomainValues()) {
            result[counter] = SystemCounters_[counter].load();
        }
        result[ESystemCounter::BytesUsed] = GetUsed(result[ESystemCounter::BytesAllocated], result[ESystemCounter::BytesFreed]);
        return result;
    }

    // Returns huge counters.
    TLocalHugeCounters GetHugeCounters()
    {
        TLocalHugeCounters result;
        for (auto counter : TEnumTraits<EHugeCounter>::GetDomainValues()) {
            result[counter] = HugeCounters_[counter].load();
        }
        result[EHugeCounter::BytesUsed] = GetUsed(result[EHugeCounter::BytesAllocated], result[EHugeCounter::BytesFreed]);
        result[EHugeCounter::BlobsUsed] = GetUsed(result[EHugeCounter::BlobsAllocated], result[EHugeCounter::BlobsFreed]);
        return result;
    }

    // Called before TThreadState is destroyed.
    // Adds the counter values from TThreadState to the global counters.
    void AccumulateLocalCounters(TThreadState* state)
    {
        for (auto counter : TEnumTraits<EBasicCounter>::GetDomainValues()) {
            GlobalState->TotalCounters.CumulativeTaggedCounters[counter] += state->TotalCounters.CumulativeTaggedCounters[counter];
            GlobalState->TotalCounters.UntaggedCounters[counter] += state->TotalCounters.UntaggedCounters[counter];
        }
        for (size_t index = 0; index < MaxTaggedCounterSets; ++index) {
            const auto* localSet = state->TotalCounters.FindTaggedCounterSet(index);
            if (!localSet) {
                continue;
            }
            auto* globalSet = GlobalState->TotalCounters.GetOrCreateTaggedCounterSet(index);
            for (size_t jndex = 0; jndex < TaggedCounterSetSize; ++jndex) {
                for (auto counter : TEnumTraits<EBasicCounter>::GetDomainValues()) {
                    globalSet->Counters[jndex][counter] += localSet->Counters[jndex][counter];
                }
            }
        }
        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            for (auto counter : TEnumTraits<ELargeArenaCounter>::GetDomainValues()) {
                GlobalState->LargeArenaCounters[rank][counter] += state->LargeArenaCounters[rank][counter];
            }
        }
    }

    // Called on each background tick to push statistics to the profiler.
    void RunBackgroundTasks(const TBackgroundContext& context)
    {
        if (!context.Profiler.GetEnabled()) {
            return;
        }
        PushSystemStatistics(context);
        PushTotalStatistics(context);
        PushSmallStatistics(context);
        PushLargeStatistics(context);
        PushHugeStatistics(context);
    }

private:
    template <class TCounter>
    static ssize_t LoadTaggedTotalCounter(const TTotalCounters<TCounter>& counters, TMemoryTag tag, EBasicCounter counter)
    {
        const auto* set = counters.FindTaggedCounterSet(tag / TaggedCounterSetSize);
        if (Y_UNLIKELY(!set)) {
            return 0;
        }
        return LoadCounter(set->Counters[tag % TaggedCounterSetSize][counter]);
    }

    template <class TCounter>
    static Y_FORCE_INLINE void IncrementUntaggedTotalCounter(TTotalCounters<TCounter>* counters, EBasicCounter counter, ssize_t delta)
    {
        counters->UntaggedCounters[counter] += delta;
    }

    template <class TCounter>
    static Y_FORCE_INLINE void IncrementTaggedTotalCounter(TTotalCounters<TCounter>* counters, TMemoryTag tag, EBasicCounter counter, ssize_t delta)
    {
        counters->CumulativeTaggedCounters[counter] += delta;
        auto* set = counters->GetOrCreateTaggedCounterSet(tag / TaggedCounterSetSize);
        set->Counters[tag % TaggedCounterSetSize][counter] += delta;
    }

    template <class TCounters>
    static void PushCounterStatistics(const NProfiling::TProfiler& profiler, const TCounters& counters)
    {
        using T = typename TCounters::TIndex;
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            profiler.Enqueue("/" + FormatEnum(counter), counters[counter], NProfiling::EMetricType::Gauge);
        }
    }

    void PushSystemStatistics(const TBackgroundContext& context)
    {
        auto counters = GetSystemCounters();
        NProfiling::TProfiler profiler(context.Profiler.GetPathPrefix() + "/system");
        PushCounterStatistics(profiler, counters);
    }

    void PushTotalStatistics(const TBackgroundContext& context)
    {
        auto counters = GetTotalCounters();
        NProfiling::TProfiler profiler(context.Profiler.GetPathPrefix() + "/total");
        PushCounterStatistics(profiler, counters);
    }

    void PushHugeStatistics(const TBackgroundContext& context)
    {
        auto counters = GetHugeCounters();
        NProfiling::TProfiler profiler(context.Profiler.GetPathPrefix() + "/huge");
        PushCounterStatistics(profiler, counters);
    }

    void PushSmallArenaStatistics(
        const TBackgroundContext& context,
        size_t rank,
        const TLocalSmallCounters& counters)
    {
        NProfiling::TProfiler profiler(
            context.Profiler.GetPathPrefix() + "/small_arena",
            {
                NProfiling::TProfileManager::Get()->RegisterTag("rank", rank)
            });
        PushCounterStatistics(profiler, counters);
    }

    void PushSmallStatistics(const TBackgroundContext& context)
    {
        auto counters = GetSmallCounters();
        NProfiling::TProfiler profiler(context.Profiler.GetPathPrefix() + "/small");
        PushCounterStatistics(profiler, counters);

        auto arenaCounters = GetSmallArenaCounters();
        for (size_t rank = 1; rank < SmallRankCount; ++rank) {
            PushSmallArenaStatistics(context, rank, arenaCounters[rank]);
        }
    }

    void PushLargeArenaStatistics(
        const TBackgroundContext& context,
        size_t rank,
        const TLocalLargeCounters& counters)
    {
        NProfiling::TProfiler profiler(
            context.Profiler.GetPathPrefix() + "/large_arena",
            {
                NProfiling::TProfileManager::Get()->RegisterTag("rank", rank)
            });
        PushCounterStatistics(profiler, counters);

        auto bytesFreed = counters[ELargeArenaCounter::BytesFreed];
        auto bytesReleased = counters[ELargeArenaCounter::PagesReleased] * PageSize;
        int poolHitRatio;
        if (bytesFreed == 0) {
            poolHitRatio = 100;
        } else if (bytesReleased > bytesFreed) {
            poolHitRatio = 0;
        } else {
            poolHitRatio = 100 - bytesReleased * 100 / bytesFreed;
        }
        profiler.Enqueue("/pool_hit_ratio", poolHitRatio, NProfiling::EMetricType::Gauge);
    }

    void PushLargeStatistics(const TBackgroundContext& context)
    {
        auto counters = GetLargeCounters();
        NProfiling::TProfiler profiler(context.Profiler.GetPathPrefix() + "/large");
        PushCounterStatistics(profiler, counters);

        auto arenaCounters = GetLargeArenaCounters();
        for (size_t rank = MinLargeRank; rank < LargeRankCount; ++rank) {
            PushLargeArenaStatistics(context, rank, arenaCounters[rank]);
        }
    }

private:
    TGlobalSystemCounters SystemCounters_;
    std::array<TGlobalSmallCounters, SmallRankCount> SmallArenaCounters_;
    TGlobalHugeCounters HugeCounters_;
};

TBox<TStatisticsManager> StatisticsManager;

////////////////////////////////////////////////////////////////////////////////

void* TSystemAllocator::Allocate(size_t size)
{
    auto rawSize = GetRawBlobSize<TSystemBlobHeader>(size);
    void* mmappedPtr;
    while (true) {
        auto currentPtr = CurrentPtr_.fetch_add(rawSize);
        YCHECK(currentPtr + rawSize <= SystemZoneEnd);
        mmappedPtr = MappedMemoryManager->Map(currentPtr, rawSize, MAP_POPULATE);
        if (mmappedPtr == reinterpret_cast<void*>(currentPtr)) {
            break;
        }
        MappedMemoryManager->Unmap(mmappedPtr, rawSize);
    }
    auto* blob = static_cast<TSystemBlobHeader*>(mmappedPtr);
    new (blob) TSystemBlobHeader(size);
    auto* result = HeaderToPtr(blob);
    PoisonUninitializedRange(result, size);
    StatisticsManager->IncrementSystemCounter(ESystemCounter::BytesAllocated, rawSize);
    return result;
}

void TSystemAllocator::Free(void* ptr)
{
    auto* blob = PtrToHeader<TSystemBlobHeader>(ptr);
    auto rawSize = GetRawBlobSize<TSystemBlobHeader>(blob->Size);
    MappedMemoryManager->Unmap(blob, rawSize);
    StatisticsManager->IncrementSystemCounter(ESystemCounter::BytesFreed, rawSize);
}

////////////////////////////////////////////////////////////////////////////////
// Small allocator
//
// Allocations (called small chunks) are grouped by their sizes. Two most-significant binary digits are
// used to determine the rank of a chunk, which guarantees 25% overhead in the worst case.
// A pair of helper arrays (SmallSizeToRank1 and SmallSizeToRank2) are used to compute ranks; we expect
// them to be permanently cached.
//
// Chunks of the same rank are served by a (small) arena allocator.
// In fact, there are two arenas for each rank: one is for tagged allocations and another is for untagged ones.
//
// We encode chunk's rank and whether it is tagged or not in the resulting pointer as follows:
//   0- 3:  must be zero due to alignment
//   4-39:  varies
//  40-44:  rank
//     45:  0 for untagged allocations, 1 for tagged ones
//  45-63:  zeroes
// This enables computing chunk's rank and also determining if it is tagged in constant time
// without any additional lookups. Also, one pays no space overhead for untagged allocations
// and pays 16 bytes for each tagged one.
//
// Each arena allocates extents of memory by calling mmap for each extent of SmallExtentSize bytes.
// (Recall that this memory is never reclaimed.)
// Each extent is then sliced into segments of SmallSegmentSize bytes.
// Whenever a new segment is acquired, its memory is pre-faulted by madvise(MADV_POPULATE).
// New segments are acquired under per-arena fork-aware spin lock.
//
// Each thread maintains a separate cache of chunks of each rank (two caches to be precise: one
// for tagged allocations and the other for untagged). These caches are fully thread-local and
// involve no atomic operations.
//
// There are also global caches (per rank, for tagged and untagged allocations).
// Instead of keeping individual chunks these work with chunk groups (collections of up to ChunksPerGroup
// arbitrary chunks).
//
// When the local cache becomes exhausted, a group of chunks is fetched from the global cache
// (if the latter is empty then the arena allocator is consulted).
// Vice versa, if the local cache overflows, a group of chunks is moved from it to the global cache.
//
// Global caches and arena allocators also take care of (rare) cases when YTAlloc/YTFree is called
// without a valid thread state (which happens during thread shutdown when TThreadState is already destroyed).

// Each tagged small chunk is prepended with this header (and there is no header at all
// for untagged small chunks). Wish we could make it smaller but 16-byte alignment
// poses a problem.
struct TTaggedSmallChunkHeader
{
    explicit TTaggedSmallChunkHeader(TMemoryTag tag)
        : Tag(tag)
    { }

    TMemoryTag Tag;
    char Padding[12];
};

CHECK_HEADER_SIZE(TTaggedSmallChunkHeader)

class TSmallArenaAllocator
{
public:
    explicit TSmallArenaAllocator(size_t rank, uintptr_t zoneStart)
        : Rank_(rank)
        , ChunkSize_(SmallRankToSize[Rank_])
        , ZoneAllocator_(zoneStart, zoneStart + ZoneSize)
    { }

    void* Allocate(size_t size)
    {
        void* ptr;
        while (true) {
            ptr = TryAllocateFromCurrentSegment();
            if (Y_LIKELY(ptr)) {
                break;
            }
            PopulateAnotherSegment();
        }
        PARANOID_CHECK(PtrToSmallRank(ptr) == Rank_);
        PoisonUninitializedRange(ptr, size);
        return ptr;
    }

private:
    void* TryAllocateFromCurrentSegment()
    {
        while (true) {
            auto* oldPtr = CurrentPtr_.load();
            if (Y_UNLIKELY(!oldPtr)) {
                return nullptr;
            }

            auto* newPtr = oldPtr + ChunkSize_;
            if (Y_UNLIKELY(PtrToSegmentIndex(newPtr) != PtrToSegmentIndex(oldPtr))) {
                return nullptr;
            }

            if (Y_LIKELY(CurrentPtr_.compare_exchange_weak(oldPtr, newPtr))) {
                return oldPtr;
            }
        }
    }

    void PopulateAnotherSegment()
    {
        auto lockGuard = GuardWithTiming(SegmentLock_);

        auto* oldPtr = CurrentPtr_.load();
        if (oldPtr && PtrToSegmentIndex(oldPtr + ChunkSize_) == PtrToSegmentIndex(oldPtr)) {
            // No need for a new segment.
            return;
        }

        if (CurrentSegment_ && CurrentSegment_ + 2 * SmallSegmentSize <= CurrentExtent_ + SmallExtentSize) {
            CurrentSegment_ += SmallSegmentSize;
        } else {
            CurrentExtent_ = static_cast<char*>(ZoneAllocator_.Allocate(SmallExtentSize, 0));
            CurrentSegment_ = CurrentExtent_;
            StatisticsManager->IncrementSmallArenaCounter(ESmallArenaCounter::BytesMapped, Rank_, SmallExtentSize);
            StatisticsManager->IncrementSmallArenaCounter(ESmallArenaCounter::PagesMapped, Rank_, SmallExtentSize / PageSize);
        }

        MappedMemoryManager->Populate(CurrentSegment_, SmallSegmentSize);
        StatisticsManager->IncrementSmallArenaCounter(ESmallArenaCounter::BytesCommitted, Rank_, SmallSegmentSize);
        StatisticsManager->IncrementSmallArenaCounter(ESmallArenaCounter::PagesCommitted, Rank_, SmallSegmentSize / PageSize);
        CurrentPtr_.store(CurrentSegment_);
    }

private:
    const size_t Rank_;
    const size_t ChunkSize_;

    TZoneAllocator ZoneAllocator_;

    char* CurrentExtent_ = nullptr;
    char* CurrentSegment_ = nullptr;
    std::atomic<char*> CurrentPtr_ = {nullptr};

    NConcurrency::TForkAwareSpinLock SegmentLock_;
};

TBox<TEnumIndexedVector<std::array<TBox<TSmallArenaAllocator>, SmallRankCount>, EAllocationKind>> SmallArenaAllocators;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t ChunksPerGroup = 128;
constexpr size_t GroupsBatchSize = 1024;

static_assert(ChunksPerGroup <= MaxCachedChunksPerRank, "ChunksPerGroup > MaxCachedChunksPerRank");

class TChunkGroup
    : public TFreeListItem<TChunkGroup>
{
public:
    bool IsEmpty() const
    {
        return Size_ == 0;
    }

    size_t ExtractAll(void** ptrs)
    {
        auto count = Size_;
        ::memcpy(ptrs, Ptrs_.data(), count * sizeof(void*));
        Size_ = 0;
        return count;
    }

    void PutOne(void* ptr)
    {
        PutMany(&ptr, 1);
    }

    void PutMany(void** ptrs, size_t count)
    {
        PARANOID_CHECK(Size_ == 0);
        PARANOID_CHECK(count <= ChunksPerGroup);
        ::memcpy(Ptrs_.data(), ptrs, count * sizeof(void*));
        Size_ = count;
    }

private:
    size_t Size_ = 0; // <= ChunksPerGroup
    std::array<void*, ChunksPerGroup> Ptrs_;
};

class TGlobalSmallChunkCache
{
public:
    explicit TGlobalSmallChunkCache(EAllocationKind kind)
        : Kind_(kind)
    { }

    bool TryMoveGroupToLocal(TThreadState* state, size_t rank)
    {
        auto& groups = RankToChunkGroups_[rank];
        auto* group = groups.Extract(state);
        if (!Y_LIKELY(group)) {
            return false;
        }

        PARANOID_CHECK(!group->IsEmpty());

        auto& chunkPtrPtr = state->SmallBlobCache[Kind_].RankToCachedChunkPtr[rank];
        auto chunkCount = group->ExtractAll(chunkPtrPtr + 1);
        chunkPtrPtr += chunkCount;

        GroupPool_.Free(state, group);
        return true;
    }

    void MoveGroupToGlobal(TThreadState* state, size_t rank)
    {
        auto* group = GroupPool_.Allocate(state);

        auto& chunkPtrPtr = state->SmallBlobCache[Kind_].RankToCachedChunkPtr[rank];
        group->PutMany(chunkPtrPtr - ChunksPerGroup + 1, ChunksPerGroup);
        chunkPtrPtr -= ChunksPerGroup;
        ::memset(chunkPtrPtr + 1, 0, sizeof(void*) * ChunksPerGroup);

        auto& groups = RankToChunkGroups_[rank];
        PARANOID_CHECK(!group->IsEmpty());
        groups.Put(state, group);
    }

    void MoveOneToGlobal(void* ptr, size_t rank)
    {
        auto* group = GroupPool_.Allocate(&GlobalShardedState_);
        group->PutOne(ptr);

        auto& groups = RankToChunkGroups_[rank];
        PARANOID_CHECK(!group->IsEmpty());
        groups.Put(&GlobalShardedState_, group);
    }

    void MoveAllToGlobal(TThreadState* state, size_t rank)
    {
        auto& chunkPtrPtr = state->SmallBlobCache[Kind_].RankToCachedChunkPtr[rank];
        while (true) {
            size_t count = 0;
            while (count < ChunksPerGroup && *chunkPtrPtr != reinterpret_cast<void*>(TThreadState::LeftSentinel)) {
                --chunkPtrPtr;
                ++count;
            }

            if (count == 0) {
                break;
            }

            auto* group = GroupPool_.Allocate(state);
            group->PutMany(chunkPtrPtr + 1, count);
            ::memset(chunkPtrPtr + 1, 0, sizeof(void*) * count);

            auto& groups = RankToChunkGroups_[rank];
            groups.Put(state, group);
        }
    }

private:
    const EAllocationKind Kind_;

    TGlobalShardedState GlobalShardedState_;
    TShardedSystemPool<TChunkGroup, GroupsBatchSize> GroupPool_;
    std::array<TShardedFreeList<TChunkGroup>, SmallRankCount> RankToChunkGroups_;
};

TBox<TEnumIndexedVector<TBox<TGlobalSmallChunkCache>, EAllocationKind>> GlobalSmallChunkCaches;

////////////////////////////////////////////////////////////////////////////////

class TSmallAllocator
{
public:
    template <EAllocationKind Kind>
    static Y_FORCE_INLINE void* Allocate(TMemoryTag tag, size_t rank)
    {
        size_t size = SmallRankToSize[rank];

        auto* state = TThreadManager::FindThreadState();
        if (Y_UNLIKELY(!state)) {
            return AllocateGlobal<Kind>(tag, size, rank);
        }

        StatisticsManager->IncrementTotalCounter<Kind>(state, tag, EBasicCounter::BytesAllocated, size);

        while (true) {
            auto& chunkPtr = state->SmallBlobCache[Kind].RankToCachedChunkPtr[rank];
            auto& cachedPtr = *chunkPtr;
            auto* ptr = cachedPtr;
            PARANOID_CHECK(ptr);
            if (Y_LIKELY(ptr != reinterpret_cast<void*>(TThreadState::LeftSentinel))) {
                cachedPtr = nullptr;
                --chunkPtr;
                PoisonUninitializedRange(ptr, size);
                return ptr;
            }

            if (!(*GlobalSmallChunkCaches)[Kind]->TryMoveGroupToLocal(state, rank)) {
                return (*SmallArenaAllocators)[Kind][rank]->Allocate(size);
            }
        }
    }

    template <EAllocationKind Kind>
    static Y_FORCE_INLINE void Free(TMemoryTag tag, void* ptr)
    {
        auto rank = PtrToSmallRank(ptr);
        auto size = SmallRankToSize[rank];
        PoisonFreedRange(ptr, size);

        auto* state = TThreadManager::FindThreadState();
        if (Y_UNLIKELY(!state)) {
            FreeGlobal<Kind>(tag, ptr, rank, size);
            return;
        }

        StatisticsManager->IncrementTotalCounter<Kind>(state, tag, EBasicCounter::BytesFreed, size);

        while (true) {
            auto& chunkPtrPtr = state->SmallBlobCache[Kind].RankToCachedChunkPtr[rank];
            auto& chunkPtr = *(chunkPtrPtr + 1);
            if (Y_LIKELY(!chunkPtr)) {
                chunkPtr = ptr;
                ++chunkPtrPtr;
                return;
            }

            (*GlobalSmallChunkCaches)[Kind]->MoveGroupToGlobal(state, rank);
        }
    }

    static size_t GetSize(void* ptr)
    {
        auto rank = PtrToSmallRank(ptr);
        auto size = SmallRankToSize[rank];
        if (reinterpret_cast<uintptr_t>(ptr) >= TaggedSmallZonesStart) {
            size -= sizeof (TTaggedSmallChunkHeader);
        }
        return size;
    }

    static void PurgeCaches()
    {
        DoPurgeCaches<EAllocationKind::Untagged>();
        DoPurgeCaches<EAllocationKind::Tagged>();
    }

private:
    template <EAllocationKind Kind>
    static void DoPurgeCaches()
    {
        auto* state = TThreadManager::GetThreadState();
        for (size_t rank = 0; rank < SmallRankCount; ++rank) {
            (*GlobalSmallChunkCaches)[Kind]->MoveAllToGlobal(state, rank);
        }
    }

    template <EAllocationKind Kind>
    static void* AllocateGlobal(TMemoryTag tag, size_t size, size_t rank)
    {
        StatisticsManager->IncrementTotalCounter(tag, EBasicCounter::BytesAllocated, size);
        return (*SmallArenaAllocators)[Kind][rank]->Allocate(size);
    }

    template <EAllocationKind Kind>
    static void FreeGlobal(TMemoryTag tag, void* ptr, size_t rank, size_t size)
    {
        StatisticsManager->IncrementTotalCounter(tag, EBasicCounter::BytesFreed, size);
        (*GlobalSmallChunkCaches)[Kind]->MoveOneToGlobal(ptr, rank);
    }
};

////////////////////////////////////////////////////////////////////////////////
// Large blob allocator
//
// Like for small chunks, large blobs are grouped into arenas, where arena K handles
// blobs of size (2^{K-1},2^K]. Memory is mapped in extents of LargeExtentSize bytes.
// Each extent is split into segments of size 2^K (here segment is just a memory region, which may fully consist of
// unmapped pages). When a segment is actually allocated, it becomes a blob and a TLargeBlobHeader
// structure is placed at its start.
//
// When an extent is allocated, it is sliced into segments (not blobs, since no headers are placed and
// no memory is touched). These segments are put into disposed segments list.
//
// For each blob two separate sizes are maintained: BytesAcquired indicates the number of bytes
// acquired via madvise(MADV_POPULATE) from the system; BytesAllocated (<= BytesAcquired) corresponds
// to the number of bytes claimed by the user (including the header and page size alignment).
// If BytesAllocated == 0 then this blob is spare, i.e.
// was freed and remains cached for further possible reuse.
//
// When a new blob is being allocated, the allocator first tries to extract a spare blob. On success,
// its acquired size is extended (if needed); the acquired size never shrinks on allocation.
// If no spare blobs exist, a disposed segment is extracted and is turned into a blob (i.e.
// its header is initialized) and the needed number of bytes is acquired. If no disposed segments
// exist, then a new extent is allocated and slices into segments.
//
// The above algorithm only claims memory from the system (by means of madvise(MADV_POPULATE));
// the reclaim is handled by a separate background mechanism. Two types of reclaimable memory
// regions are possible:
// * spare: these correspond to spare blobs; upon reclaiming this region becomes a disposed segment
// * overhead: these correspond to trailing parts of allocated blobs in [BytesAllocated, BytesAcquired) byte range
//
// Reclaiming spare blobs is easy as these are explicitly tracked by spare blob lists. To reclaim,
// we atomically extract a blob from a spare list, call madvise(MADV_FREE), and put the pointer to
// the disposed segment list.
//
// Reclaiming overheads is more complicated since (a) allocated blobs are never tracked directly and
// (b) reclaiming them may interfere with YTAlloc and YTFree.
//
// To overcome (a), for each extent we maintain a bitmap marking segments that are actually blobs
// (i.e. contain a header). (For simplicity and efficiency this bitmap is just a vector of bytes.)
// These flags are updated in YTAlloc/YTFree with appropriate memory ordering. Note that since
// blobs are only disposed (and are turned into segments) by the background thread; if this
// thread discovers a segment that is marked as a blob, then it is safe to assume that this segment
// remains a blob unless the thread disposes it.
//
// To overcome (b), each large blob header maintains a spin lock. When blob B is extracted
// from a spare list in YTAlloc, an acquisition is tried. If successful, B is returned to the
// user. Otherwise it is assumed that B is currently being examined by the background
// reclaimer thread. YTAlloc then skips this blob and retries extraction; the problem is that
// since the spare list is basically a stack one cannot just push B back into the spare list.
// Instead, B is pushed into a special locked spare list. This list is purged by the background
// thread on each tick and its items are pushed back into the usual spare list.
//
// A similar trick is used by YTFree: when invoked for blob B its spin lock acquisition is first
// tried. Upon success, B is moved to the spare list. On failure, YTFree has to postpone this deallocation
// by moving B into the freed locked list. This list, similarly, is being purged by the background thread.
//
// It remains to explain how the background thread computes the number of bytes to be reclaimed from
// each arena. To this aim, we first compute the total number of reclaimable bytes.
// This is the sum of spare and overhead bytes in all arenas minus the number of unreclaimable bytes
// The latter grows linearly in the number of used bytes and is capped from below by a MinUnreclaimableLargeBytes;
// SetLargeUnreclaimableCoeff and SetLargeUnreclaimableBytes enable tuning these control knobs.
// The reclaimable bytes are distributed among arenas starting from those with the largest
// spare and overhead volumes.
//
// The above implies that each large blob contains a fixed-size header preceeding it.
// Hence ptr % PageSize == sizeof (TLargeBlobHeader) for each ptr returned by YTAlloc
// (since large blob sizes are larger than PageSize and are divisible by PageSize).
// For YTAllocPageAligned, however, ptr must be divisible by PageSize. To handle such an allocation, we
// artificially increase its size and align the result of YTAlloc up to the next page boundary.
// When handling a deallocation, ptr is moved back by UnalignPtr (which is capable of dealing
// with both the results of YTAlloc and YTAllocPageAligned).
// This technique is applied to both large and huge blobs.

// Every large blob (either tagged or not) is prepended with this header.
struct TLargeBlobHeader
    : public TFreeListItem<TLargeBlobHeader>
{
    TLargeBlobHeader(
        TLargeBlobExtent* extent,
        size_t bytesAcquired,
        size_t bytesAllocated,
        TMemoryTag tag)
        : Extent(extent)
        , BytesAcquired(bytesAcquired)
        , Tag(tag)
        , BytesAllocated(bytesAllocated)
    { }

    TLargeBlobExtent* Extent;
    // Number of bytes in all acquired pages.
    size_t BytesAcquired;
    std::atomic<bool> Locked = {false};
    TMemoryTag Tag = NullMemoryTag;
    char Padding[4];
    // For spare blobs this is zero.
    // For allocated blobs this is the number of bytes requested by user (not including header of any alignment).
    size_t BytesAllocated;
};

CHECK_HEADER_SIZE(TLargeBlobHeader)

struct TLargeBlobExtent
{
    TLargeBlobExtent(size_t segmentCount, char* ptr)
        : SegmentCount(segmentCount)
        , Ptr(ptr)
    { }

    size_t SegmentCount;
    char* Ptr;
    TLargeBlobExtent* NextExtent = nullptr;

    static constexpr ui8 DisposedTrue = 1;
    static constexpr ui8 DisposedFalse = 0;
    volatile ui8 DisposedFlags[0];
};

// A helper node that enables storing a number of extent's segments
// in a free list. Recall that segments themselves do not posses any headers.
struct TDisposedSegment
    : public TFreeListItem<TDisposedSegment>
{
    size_t Index;
    TLargeBlobExtent* Extent;
};

struct TLargeArena
{
    size_t Rank = 0;
    size_t SegmentSize = 0;

    TShardedFreeList<TLargeBlobHeader> SpareBlobs;
    TFreeList<TLargeBlobHeader> LockedSpareBlobs;
    TFreeList<TLargeBlobHeader> LockedFreedBlobs;
    TFreeList<TDisposedSegment> DisposedSegments;
    std::atomic<TLargeBlobExtent*> FirstExtent = {nullptr};

    TLargeBlobExtent* CurrentOverheadScanExtent = nullptr;
    size_t CurrentOverheadScanSegment = 0;
};

class TLargeBlobAllocator
{
public:
    TLargeBlobAllocator()
        : ZoneAllocator_(LargeZoneStart, LargeZoneEnd)
    {
        for (size_t rank = 0; rank < Arenas_.size(); ++rank) {
            auto& arena = Arenas_[rank];
            arena.Rank = rank;
            arena.SegmentSize = (1ULL << rank);
        }
    }

    void* Allocate(size_t size)
    {
        auto* state = TThreadManager::FindThreadState();
        return Y_LIKELY(state)
            ? DoAllocate(state, size)
            : DoAllocate(GlobalState.Get(), size);
    }

    void Free(void* ptr)
    {
        auto* state = TThreadManager::FindThreadState();
        if (Y_LIKELY(state)) {
            DoFree(state, ptr);
        } else {
            DoFree(GlobalState.Get(), ptr);
        }
    }

    static size_t GetSize(void* ptr)
    {
        UnalignPtr<TLargeBlobHeader>(ptr);
        const auto* blob = PtrToHeader<TLargeBlobHeader>(ptr);
        return blob->BytesAllocated;
    }

    void RunBackgroundTasks(const TBackgroundContext& context)
    {
        ReinstallLockedBlobs(context);
        ReclaimMemory(context);
    }

private:
    template <class TState>
    void PopulateArenaPages(TState* state, TLargeArena* arena, void* ptr, size_t size)
    {
        MappedMemoryManager->Populate(ptr, size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::BytesPopulated, size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::PagesPopulated, size / PageSize);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::BytesCommitted, size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::PagesCommitted, size / PageSize);
    }

    template <class TState>
    void ReleaseArenaPages(TState* state, TLargeArena* arena, void* ptr, size_t size)
    {
        MappedMemoryManager->Release(ptr, size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::BytesReleased, size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::PagesReleased, size / PageSize);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::BytesCommitted, -size);
        StatisticsManager->IncrementLargeArenaCounter(state, arena->Rank, ELargeArenaCounter::PagesCommitted, -size / PageSize);
    }

    bool TryLockBlob(TLargeBlobHeader* blob)
    {
        bool expected = false;
        return blob->Locked.compare_exchange_strong(expected, true);
    }

    void UnlockBlob(TLargeBlobHeader* blob)
    {
        blob->Locked.store(false);
    }

    template <class TState>
    void MoveBlobToSpare(TState* state, TLargeArena* arena, TLargeBlobHeader* blob, bool unlock)
    {
        auto rank = arena->Rank;
        auto size = blob->BytesAllocated;
        auto rawSize = GetRawBlobSize<TLargeBlobHeader>(size);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesSpare, blob->BytesAcquired);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesOverhead, -(blob->BytesAcquired - rawSize));
        blob->BytesAllocated = 0;
        if (unlock) {
            UnlockBlob(blob);
        } else {
            PARANOID_CHECK(!blob->Locked.load());
        }
        arena->SpareBlobs.Put(state, blob);
    }

    size_t GetBytesToReclaim(const std::array<TLocalLargeCounters, LargeRankCount>& arenaCounters)
    {
        size_t totalBytesAllocated = 0;
        size_t totalBytesFreed = 0;
        size_t totalBytesSpare = 0;
        size_t totalBytesOverhead = 0;
        for (size_t rank = 0; rank < Arenas_.size(); ++rank) {
            const auto& counters = arenaCounters[rank];
            totalBytesAllocated += counters[ELargeArenaCounter::BytesAllocated];
            totalBytesFreed += counters[ELargeArenaCounter::BytesFreed];
            totalBytesSpare += counters[ELargeArenaCounter::BytesSpare];
            totalBytesOverhead += counters[ELargeArenaCounter::BytesOverhead];
        }

        auto totalBytesUsed = totalBytesAllocated - totalBytesFreed;
        auto totalBytesReclaimable = totalBytesSpare + totalBytesOverhead;

        auto threshold = std::max(
            static_cast<size_t>(ConfigurationManager->GetLargeUnreclaimableCoeff() * totalBytesUsed),
            ConfigurationManager->GetLargeUnreclaimableBytes());
        if (totalBytesReclaimable < threshold) {
            return 0;
        }

        auto bytesToReclaim = totalBytesReclaimable - threshold;
        return AlignUp(bytesToReclaim, PageSize);
    }

    void ReinstallLockedSpareBlobs(const TBackgroundContext& context, TLargeArena* arena)
    {
        auto* blob = arena->LockedSpareBlobs.ExtractAll();
        auto* state = TThreadManager::GetThreadState();

        size_t count = 0;
        while (blob) {
            auto* nextBlob = blob->Next;
            PARANOID_CHECK(!blob->Locked.load());
            arena->SpareBlobs.Put(state, blob);
            blob = nextBlob;
            ++count;
        }

        const auto& Logger = context.Logger;
        LOG_DEBUG_IF(count > 0, "Locked spare blobs reinstalled (Rank: %v, Blobs: %v)",
            arena->Rank,
            count);
    }

    void ReinstallLockedFreedBlobs(const TBackgroundContext& context, TLargeArena* arena)
    {
        auto* state = TThreadManager::GetThreadState();
        auto* blob = arena->LockedFreedBlobs.ExtractAll();

        size_t count = 0;
        while (blob) {
            auto* nextBlob = blob->Next;
            MoveBlobToSpare(state, arena, blob, false);
            ++count;
            blob = nextBlob;
        }

        const auto& Logger = context.Logger;
        LOG_DEBUG_IF(count > 0, "Locked freed blobs reinstalled (Rank: %v, Blobs: %v)",
            arena->Rank,
            count);
    }

    void ReclaimSpareMemory(const TBackgroundContext& context, TLargeArena* arena, ssize_t bytesToReclaim)
    {
        if (bytesToReclaim <= 0) {
            return;
        }

        auto rank = arena->Rank;
        auto* state = TThreadManager::GetThreadState();

        const auto& Logger = context.Logger;
        LOG_DEBUG("Started processing spare memory in arena (BytesToReclaim: %vM, Rank: %v)",
            bytesToReclaim / 1_MB,
            rank);

        size_t bytesReclaimed = 0;
        size_t blobsReclaimed = 0;
        while (bytesToReclaim > 0) {
            auto* blob = arena->SpareBlobs.ExtractRoundRobin(state);
            if (!blob) {
                break;
            }
            
            PARANOID_CHECK(blob->BytesAllocated == 0);
            auto bytesAcquired = blob->BytesAcquired;

            StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesSpare, -bytesAcquired);
            bytesToReclaim -= bytesAcquired;
            bytesReclaimed += bytesAcquired;
            blobsReclaimed += 1;
            
            auto* extent = blob->Extent;
            auto* ptr = reinterpret_cast<char*>(blob);
            ReleaseArenaPages(
                state,
                arena,
                ptr,
                bytesAcquired);

            size_t segmentIndex = (ptr - extent->Ptr) / arena->SegmentSize;
            __atomic_store_n(&extent->DisposedFlags[segmentIndex], TLargeBlobExtent::DisposedTrue, __ATOMIC_RELEASE);

            auto* disposedSegment = DisposedSegmentPool_.Allocate();
            disposedSegment->Index = segmentIndex;
            disposedSegment->Extent = extent;
            arena->DisposedSegments.Put(disposedSegment);
        }

        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::SpareBytesReclaimed, bytesReclaimed);

        LOG_DEBUG("Finished processing spare memory in arena (Rank: %v, BytesReclaimed: %vM, BlobsReclaimed: %v)",
            arena->Rank,
            bytesReclaimed / 1_MB,
            blobsReclaimed);
    }

    void ReclaimOverheadMemory(const TBackgroundContext& context, TLargeArena* arena, ssize_t bytesToReclaim)
    {
        if (bytesToReclaim == 0) {
            return;
        }

        auto* state = TThreadManager::GetThreadState();
        auto rank = arena->Rank;

        const auto& Logger = context.Logger;
        LOG_DEBUG("Started processing overhead memory in arena (BytesToReclaim: %vM, Rank: %v)",
            bytesToReclaim / 1_MB,
            rank);

        size_t extentsTraversed = 0;
        size_t segmentsTraversed = 0;
        size_t bytesReclaimed = 0;

        bool restartedFromFirstExtent = false;
        auto& currentExtent = arena->CurrentOverheadScanExtent;
        auto& currentSegment = arena->CurrentOverheadScanSegment;
        while (bytesToReclaim > 0) {
            if (!currentExtent) {
                if (restartedFromFirstExtent) {
                    break;
                }
                currentExtent = arena->FirstExtent.load();
                restartedFromFirstExtent = true;
            }

            while (currentSegment  < currentExtent->SegmentCount && bytesToReclaim > 0) {
                ++segmentsTraversed;
                if (__atomic_load_n(&currentExtent->DisposedFlags[currentSegment], __ATOMIC_ACQUIRE) == TLargeBlobExtent::DisposedFalse) {
                    auto* ptr = currentExtent->Ptr + currentSegment * arena->SegmentSize;
                    auto* blob = reinterpret_cast<TLargeBlobHeader*>(ptr);
                    PARANOID_CHECK(blob->Extent == currentExtent);
                    if (TryLockBlob(blob)) {
                        if (blob->BytesAllocated > 0) {
                            size_t rawSize = GetRawBlobSize<TLargeBlobHeader>(blob->BytesAllocated);
                            size_t bytesToRelease = blob->BytesAcquired - rawSize;
                            if (bytesToRelease > 0) {
                                ReleaseArenaPages(
                                    state,
                                    arena,
                                    ptr + blob->BytesAcquired - bytesToRelease,
                                    bytesToRelease);
                                StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesOverhead, -bytesToRelease);
                                blob->BytesAcquired = rawSize;
                                bytesToReclaim -= bytesToRelease;
                                bytesReclaimed += bytesToRelease;
                            }
                        }
                        UnlockBlob(blob);
                    }
                }
                ++currentSegment;
            }

            ++extentsTraversed;
            currentSegment = 0;
            currentExtent = currentExtent->NextExtent;
        }

        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::OverheadBytesReclaimed, bytesReclaimed);

        LOG_DEBUG("Finished processing overhead memory in arena (Rank: %v, Extents: %v, Segments: %v, BytesReclaimed: %vM)",
            arena->Rank,
            extentsTraversed,
            segmentsTraversed,
            bytesReclaimed / 1_MB);
    }

    void ReinstallLockedBlobs(const TBackgroundContext& context)
    {
        for (auto& arena : Arenas_) {
            ReinstallLockedSpareBlobs(context, &arena);
            ReinstallLockedFreedBlobs(context, &arena);
        }
    }

    void ReclaimMemory(const TBackgroundContext& context)
    {
        auto arenaCounters = StatisticsManager->GetLargeArenaCounters();
        ssize_t bytesToReclaim = GetBytesToReclaim(arenaCounters);
        if (bytesToReclaim == 0) {
            return;
        }

        const auto& Logger = context.Logger;
        LOG_DEBUG("Memory reclaim started (BytesToReclaim: %vM)",
            bytesToReclaim / 1_MB);

        std::array<ssize_t, LargeRankCount * 2> bytesReclaimablePerArena;
        for (size_t rank = 0; rank < LargeRankCount; ++rank) {
            bytesReclaimablePerArena[rank * 2] = arenaCounters[rank][ELargeArenaCounter::BytesOverhead];
            bytesReclaimablePerArena[rank * 2 + 1] = arenaCounters[rank][ELargeArenaCounter::BytesSpare];
        }

        std::array<ssize_t, LargeRankCount * 2> bytesToReclaimPerArena{};
        while (bytesToReclaim > 0) {
            ssize_t maxBytes = std::numeric_limits<ssize_t>::min();
            int maxIndex = -1;
            for (int index = 0; index < LargeRankCount * 2; ++index) {
                if (bytesReclaimablePerArena[index] > maxBytes) {
                    maxBytes = bytesReclaimablePerArena[index];
                    maxIndex = index;
                }
            }

            if (maxIndex < 0) {
                break;
            }

            auto bytesToReclaimPerStep = std::min({bytesToReclaim, maxBytes, 4_MB});
            if (bytesToReclaimPerStep < 0) {
                break;
            }

            bytesToReclaimPerArena[maxIndex] += bytesToReclaimPerStep;
            bytesReclaimablePerArena[maxIndex] -= bytesToReclaimPerStep;
            bytesToReclaim -= bytesToReclaimPerStep;
        }

        for (auto& arena : Arenas_) {
            auto rank = arena.Rank;
            ReclaimOverheadMemory(context, &arena, bytesToReclaimPerArena[rank * 2]);
            ReclaimSpareMemory(context, &arena, bytesToReclaimPerArena[rank * 2 + 1]);
        }

        LOG_DEBUG("Memory reclaim finished");
    }

    template <class TState>
    void AllocateArenaExtent(TState* state, TLargeArena* arena)
    {
        auto rank = arena->Rank;
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::ExtentsAllocated, 1);

        size_t segmentCount = LargeExtentSize / arena->SegmentSize;
        size_t extentHeaderSize = AlignUp(sizeof (TLargeBlobExtent) + sizeof (TLargeBlobExtent::DisposedFlags[0]) * segmentCount, PageSize);
        size_t allocationSize = extentHeaderSize + LargeExtentSize;

        auto* ptr = ZoneAllocator_.Allocate(allocationSize, MAP_NORESERVE);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesMapped, allocationSize);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::PagesMapped, allocationSize / PageSize);

        auto* extent = static_cast<TLargeBlobExtent*>(ptr);
        MappedMemoryManager->Populate(ptr, extentHeaderSize);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesPopulated, extentHeaderSize);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::PagesPopulated, extentHeaderSize / PageSize);
        StatisticsManager->IncrementSystemCounter(ESystemCounter::BytesAllocated, extentHeaderSize);

        new (extent) TLargeBlobExtent(segmentCount, static_cast<char*>(ptr) + extentHeaderSize);

        for (size_t index = 0; index < segmentCount; ++index) {
            auto* disposedSegment = DisposedSegmentPool_.Allocate();
            disposedSegment->Index = index;
            disposedSegment->Extent = extent;
            arena->DisposedSegments.Put(disposedSegment);
            extent->DisposedFlags[index] = TLargeBlobExtent::DisposedTrue;
        }

        while (true) {
            auto* expectedFirstExtent = arena->FirstExtent.load();
            extent->NextExtent = expectedFirstExtent;
            if (arena->FirstExtent.compare_exchange_strong(expectedFirstExtent, extent)) {
                break;
            }
        }
    }

    template <class TState>
    void* DoAllocate(TState* state, size_t size)
    {
        auto tag = TThreadManager::GetCurrentMemoryTag();
        auto rawSize = GetRawBlobSize<TLargeBlobHeader>(size);
        auto rank = GetLargeRank(rawSize);
        auto& arena = Arenas_[rank];
        PARANOID_CHECK(rawSize <= arena.SegmentSize);

        TLargeBlobHeader* blob;
        while (true) {
            blob = arena.SpareBlobs.Extract(state);
            if (blob) {
                if (TryLockBlob(blob)) {
                    StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesSpare, -blob->BytesAcquired);
                    if (blob->BytesAcquired < rawSize) {
                        PopulateArenaPages(
                            state,
                            &arena,
                            reinterpret_cast<char*>(blob) + blob->BytesAcquired,
                            rawSize - blob->BytesAcquired);
                        blob->BytesAcquired = rawSize;
                    } else {
                        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesOverhead, blob->BytesAcquired - rawSize);
                    }
                    PARANOID_CHECK(blob->BytesAllocated == 0);
                    blob->BytesAllocated = size;
                    blob->Tag = tag;
                    UnlockBlob(blob);
                    break;
                } else {
                    arena.LockedSpareBlobs.Put(blob);
                }
            }

            auto* disposedSegment = arena.DisposedSegments.Extract();
            if (disposedSegment) {
                auto index = disposedSegment->Index;
                auto* extent = disposedSegment->Extent;
                DisposedSegmentPool_.Free(disposedSegment);

                auto* ptr = extent->Ptr + index * arena.SegmentSize;
                PopulateArenaPages(
                    state,
                    &arena,
                    ptr,
                    rawSize);

                blob = reinterpret_cast<TLargeBlobHeader*>(ptr);
                new (blob) TLargeBlobHeader(extent, rawSize, size, tag);

                __atomic_store_n(&extent->DisposedFlags[index], TLargeBlobExtent::DisposedFalse, __ATOMIC_RELEASE);

                break;
            }

            AllocateArenaExtent(state, &arena);
        }

        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BlobsAllocated, 1);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesAllocated, size);
        StatisticsManager->IncrementTotalCounter(state, tag, EBasicCounter::BytesAllocated, size);

        auto* result = HeaderToPtr(blob);
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(result) >= LargeZoneStart && reinterpret_cast<uintptr_t>(result) < LargeZoneEnd);
        PoisonUninitializedRange(result, size);
        return result;
    }

    template <class TState>
    void DoFree(TState* state, void* ptr)
    {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= LargeZoneStart && reinterpret_cast<uintptr_t>(ptr) < LargeZoneEnd);

        auto* blob = PtrToHeader<TLargeBlobHeader>(ptr);
        auto size = blob->BytesAllocated;
        PoisonFreedRange(ptr, size);

        auto rawSize = GetRawBlobSize<TLargeBlobHeader>(size);
        auto rank = GetLargeRank(rawSize);
        auto& arena = Arenas_[rank];
        PARANOID_CHECK(blob->BytesAcquired <= arena.SegmentSize);

        auto tag = blob->Tag;

        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BlobsFreed, 1);
        StatisticsManager->IncrementLargeArenaCounter(state, rank, ELargeArenaCounter::BytesFreed, size);
        StatisticsManager->IncrementTotalCounter(state, tag, EBasicCounter::BytesFreed, size);

        if (TryLockBlob(blob)) {
            MoveBlobToSpare(state, &arena, blob, true);
        } else {
            arena.LockedFreedBlobs.Put(blob);
        }
    }

private:
    TZoneAllocator ZoneAllocator_;
    std::array<TLargeArena, LargeRankCount> Arenas_;

    static constexpr size_t DisposedSegmentsBatchSize = 1024;
    TSystemPool<TDisposedSegment, DisposedSegmentsBatchSize> DisposedSegmentPool_;
};

TBox<TLargeBlobAllocator> LargeBlobAllocator;

////////////////////////////////////////////////////////////////////////////////
// Huge blob allocator
//
// Basically a wrapper for TZoneAllocator.

// Every huge blob (both tagged or not) is prepended with this header.
struct THugeBlobHeader
{
    THugeBlobHeader(TMemoryTag tag, size_t size)
        : Tag(tag)
        , Size(size)
    { }

    TMemoryTag Tag;
    size_t Size;
};

CHECK_HEADER_SIZE(THugeBlobHeader)

class THugeBlobAllocator
{
public:
    THugeBlobAllocator()
        : ZoneAllocator_(HugeZoneStart, HugeZoneEnd)
    { }

    void* Allocate(size_t size)
    {
        auto tag = TThreadManager::GetCurrentMemoryTag();
        auto rawSize = GetRawBlobSize<THugeBlobHeader>(size);
        auto* blob = static_cast<THugeBlobHeader*>(ZoneAllocator_.Allocate(rawSize, MAP_POPULATE));
        new (blob) THugeBlobHeader(tag, size);

        StatisticsManager->IncrementTotalCounter(tag, EBasicCounter::BytesAllocated, size);
        StatisticsManager->IncrementHugeCounter(EHugeCounter::BlobsAllocated, 1);
        StatisticsManager->IncrementHugeCounter(EHugeCounter::BytesAllocated, size);

        auto* result = HeaderToPtr(blob);
        PoisonUninitializedRange(result, size);
        return result;
    }

    void Free(void* ptr)
    {
        auto* blob = PtrToHeader<THugeBlobHeader>(ptr);
        auto tag = blob->Tag;
        auto size = blob->Size;
        PoisonFreedRange(ptr, size);

        auto rawSize = GetRawBlobSize<THugeBlobHeader>(size);
        ZoneAllocator_.Free(blob, rawSize);

        StatisticsManager->IncrementTotalCounter(tag, EBasicCounter::BytesFreed, size);
        StatisticsManager->IncrementHugeCounter(EHugeCounter::BlobsFreed, 1);
        StatisticsManager->IncrementHugeCounter(EHugeCounter::BytesFreed, size);
    }

    static size_t GetSize(void* ptr)
    {
        UnalignPtr<THugeBlobHeader>(ptr);
        const auto* blob = PtrToHeader<THugeBlobHeader>(ptr);
        return blob->Size;
    }

private:
    TZoneAllocator ZoneAllocator_;
};

TBox<THugeBlobAllocator> HugeBlobAllocator;

////////////////////////////////////////////////////////////////////////////////
// A thunk to large and huge blob allocators

class TBlobAllocator
{
public:
    static void* Allocate(size_t size)
    {
        InitializeGlobals();
        // NB: Account for the header. Also note that we may safely ignore the alignment since
        // HugeSizeThreshold is already page-aligned.
        if (size < HugeSizeThreshold - sizeof(TLargeBlobHeader)) {
            auto* result = LargeBlobAllocator->Allocate(size);
            PARANOID_CHECK(reinterpret_cast<uintptr_t>(result) >= LargeZoneStart && reinterpret_cast<uintptr_t>(result) < LargeZoneEnd);
            return result;
        } else {
            auto* result = HugeBlobAllocator->Allocate(size);
            PARANOID_CHECK(reinterpret_cast<uintptr_t>(result) >= HugeZoneStart && reinterpret_cast<uintptr_t>(result) < HugeZoneEnd);
            return result;
        }
    }

    static void Free(void* ptr)
    {
        InitializeGlobals();
        if (reinterpret_cast<uintptr_t>(ptr) < LargeZoneEnd) {
            PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= LargeZoneStart && reinterpret_cast<uintptr_t>(ptr) < LargeZoneEnd);
            UnalignPtr<TLargeBlobHeader>(ptr);
            LargeBlobAllocator->Free(ptr);
        } else if (reinterpret_cast<uintptr_t>(ptr) < HugeZoneEnd) {
            PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= HugeZoneStart && reinterpret_cast<uintptr_t>(ptr) < HugeZoneEnd);
            UnalignPtr<THugeBlobHeader>(ptr);
            HugeBlobAllocator->Free(ptr);
        } else {
            Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Base class for all background threads.
template <class T>
class TBackgroundThreadBase
{
public:
    TBackgroundThreadBase()
        : Thread_(ThreadMainStatic, this)
    {
        pthread_atfork(nullptr, nullptr, &OnFork);
    }

    virtual ~TBackgroundThreadBase()
    {
        if (Forked_) {
            Thread_.Detach();
        } else {
            StopEvent_.Signal();
            Thread_.Join();
        }
    }

    static T* Get()
    {
        // NB: Pass max priority to make sure these guys die first.
        // Indeed, no one depends on them but they depend on others
        // (e.g. TBackgroundThread implicitly depends on TPosixFadvise through TFileHandle).
        return SingletonWithPriority<T, std::numeric_limits<size_t>::max()>();
    }

private:
    TThread Thread_;
    TManualEvent StopEvent_;


    bool Forked_ = false;

private:
    static void OnFork()
    {
         auto* this_ = T::Get();
         this_->Forked_ = true;
    }
    
    static void* ThreadMainStatic(void* opaque)
    {
        auto* this_ = static_cast<TBackgroundThreadBase*>(opaque);
        this_->ThreadMain();
        return nullptr;
    }

    virtual void ThreadMain() = 0;

protected:
    void Start()
    {
        Thread_.Start();
    }
    
    bool IsDone(TDuration interval)
    {
        return StopEvent_.WaitT(interval);
    }
};

// Runs basic background activities: reclaim, logging, profiling etc.
class TBackgroundThread
    : public TBackgroundThreadBase<TBackgroundThread>
{
public:
    TBackgroundThread()
    {
        Start();
    }

private:
    virtual void ThreadMain() override
    {
        InitializeGlobals();
        TThread::CurrentThreadSetName(BackgroundThreadName);
        TimingManager->DisableForCurrentThread();

        while (!IsDone(BackgroundInterval)) {
            TBackgroundContext context;
            if (ConfigurationManager->IsLoggingEnabled()) {
                context.Logger = NLogging::TLogger(LoggerCategory);
            }
            if (ConfigurationManager->IsProfilingEnabled()) {
                context.Profiler = NProfiling::TProfiler(ProfilerPath);
            }

            StatisticsManager->RunBackgroundTasks(context);
            LargeBlobAllocator->RunBackgroundTasks(context);
            MappedMemoryManager->RunBackgroundTasks(context);
            TimingManager->RunBackgroundTasks(context);
        }
    }
};

class TBackgroundThreadInitializer
{
public:
    TBackgroundThreadInitializer()
    {
        // Like some others, this singleton depends on TLogManager and TProfileManager.
        // Luckily, these guys are configured to die after all other (default configured) singletons.
        TBackgroundThread::Get();
    }
} BackgroundThreadInitializer;

// Invokes madvise(MADV_STOCKPILE) periodically. 
class TStockpileThread
    : public TBackgroundThreadBase<TStockpileThread>
{
public:
    TStockpileThread()
    {
        Start();
    }

private:
    virtual void ThreadMain() override
    {
        InitializeGlobals();
        TThread::CurrentThreadSetName(StockpileThreadName);

        while (!IsDone(StockpileInterval)) {
            if (!MappedMemoryManager->Stockpile(StockpileSize)) {
                // No use to proceed.
                break;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TThreadState* TThreadManager::FindThreadState()
{
    if (Y_LIKELY(ThreadState_)) {
        return ThreadState_;
    }

    if (ThreadStateDestroyed_) {
        return nullptr;
    }

    InitializeGlobals();

    // InitializeGlobals must not allocate.
    YCHECK(!ThreadState_);
    ThreadState_ = ThreadManager->AllocateThreadState();

    return ThreadState_;
}

void TThreadManager::DestroyThread(void*)
{
    TSmallAllocator::PurgeCaches();

    auto* state = ThreadState_;
    ThreadState_ = nullptr;
    ThreadStateDestroyed_ = true;

    {
        auto guard = GuardWithTiming(ThreadManager->ThreadRegistryLock_);
        ThreadManager->UnrefThreadState(state);
    }
}

void TThreadManager::DestroyThreadState(TThreadState* state)
{
    StatisticsManager->AccumulateLocalCounters(state);
    ThreadRegistry_.Remove(state);
    ThreadStatePool_.Free(state);
}

////////////////////////////////////////////////////////////////////////////////

void InitializeGlobals()
{
    static std::once_flag Initialized;
    std::call_once(Initialized, [] () {
        StatisticsManager.Construct();
        MappedMemoryManager.Construct();
        ThreadManager.Construct();
        GlobalState.Construct();
        LargeBlobAllocator.Construct();
        HugeBlobAllocator.Construct();
        ConfigurationManager.Construct();
        SystemAllocator.Construct();
        TimingManager.Construct();

        SmallArenaAllocators.Construct();
        auto constructSmallArenaAllocators = [&] (EAllocationKind kind, uintptr_t zonesStart) {
            for (size_t rank = 1; rank < SmallRankCount; ++rank) {
                (*SmallArenaAllocators)[kind][rank].Construct(rank, zonesStart + rank * ZoneSize);
            }
        };
        constructSmallArenaAllocators(EAllocationKind::Untagged, UntaggedSmallZonesStart);
        constructSmallArenaAllocators(EAllocationKind::Tagged, TaggedSmallZonesStart);

        GlobalSmallChunkCaches.Construct();
        (*GlobalSmallChunkCaches)[EAllocationKind::Tagged].Construct(EAllocationKind::Tagged);
        (*GlobalSmallChunkCaches)[EAllocationKind::Untagged].Construct(EAllocationKind::Untagged);
    });
}

////////////////////////////////////////////////////////////////////////////////
// YTAlloc public API

void* YTAlloc(size_t size)
{
#define XX() \
    size_t rank; \
    if (Y_LIKELY(size <= 512)) { \
        rank = SmallSizeToRank1[1 + ((static_cast<int>(size) - 1) >> 3)]; \
    } else { \
        if (Y_LIKELY(size < LargeSizeThreshold)) { \
            rank = SmallSizeToRank2[(size - 1) >> 8]; \
        } else { \
            return TBlobAllocator::Allocate(size); \
        } \
    }

    auto tag = TThreadManager::GetCurrentMemoryTag();
    void* result;
    if (Y_LIKELY(tag == NullMemoryTag)) {
        XX()
        result = TSmallAllocator::Allocate<EAllocationKind::Untagged>(NullMemoryTag, rank);
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(result) >= MinUntaggedSmallPtr && reinterpret_cast<uintptr_t>(result) < MaxUntaggedSmallPtr);
    } else {
        size += sizeof (TTaggedSmallChunkHeader);
        XX()
        auto* ptr = TSmallAllocator::Allocate<EAllocationKind::Tagged>(tag, rank);
        auto* chunk = static_cast<TTaggedSmallChunkHeader*>(ptr);
        new (chunk) TTaggedSmallChunkHeader(tag);
        result = HeaderToPtr(chunk);
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(result) >= MinTaggedSmallPtr && reinterpret_cast<uintptr_t>(result) < MaxTaggedSmallPtr);
    }
    return result;
#undef XX
}

void* YTAllocPageAligned(size_t size)
{
    auto* ptr = TBlobAllocator::Allocate(size + PageSize);
    return AlignUp(ptr, PageSize);
}

void YTFree(void* ptr)
{
    if (Y_UNLIKELY(!ptr)) {
        return;
    }

    if (Y_LIKELY(reinterpret_cast<uintptr_t>(ptr) < UntaggedSmallZonesEnd)) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= MinUntaggedSmallPtr && reinterpret_cast<uintptr_t>(ptr) < MaxUntaggedSmallPtr);
        TSmallAllocator::Free<EAllocationKind::Untagged>(NullMemoryTag, ptr);
    } else if (Y_LIKELY(reinterpret_cast<uintptr_t>(ptr) < TaggedSmallZonesEnd)) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= MinTaggedSmallPtr && reinterpret_cast<uintptr_t>(ptr) < MaxTaggedSmallPtr);
        auto* chunk = PtrToHeader<TTaggedSmallChunkHeader>(ptr);
        auto tag = chunk->Tag;
        TSmallAllocator::Free<EAllocationKind::Tagged>(tag, chunk);
    } else {
        TBlobAllocator::Free(ptr);
    }
}

#if !defined(_darwin_) and !defined(_asan_enabled_) and !defined(_msan_enabled_)

size_t YTGetSize(void* ptr)
{
    if (Y_UNLIKELY(!ptr)) {
        return 0;
    }

    if (reinterpret_cast<uintptr_t>(ptr) < UntaggedSmallZonesEnd) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= MinUntaggedSmallPtr && reinterpret_cast<uintptr_t>(ptr) < MaxUntaggedSmallPtr);
        return TSmallAllocator::GetSize(ptr);
    } else if (reinterpret_cast<uintptr_t>(ptr) < TaggedSmallZonesEnd) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= MinTaggedSmallPtr && reinterpret_cast<uintptr_t>(ptr) < MaxTaggedSmallPtr);
        return TSmallAllocator::GetSize(ptr);
    } else if (reinterpret_cast<uintptr_t>(ptr) < LargeZoneEnd) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= LargeZoneStart && reinterpret_cast<uintptr_t>(ptr) < LargeZoneEnd);
        return TLargeBlobAllocator::GetSize(ptr);
    } else if (reinterpret_cast<uintptr_t>(ptr) < HugeZoneEnd) {
        PARANOID_CHECK(reinterpret_cast<uintptr_t>(ptr) >= HugeZoneStart && reinterpret_cast<uintptr_t>(ptr) < HugeZoneEnd);
        return THugeBlobAllocator::GetSize(ptr);
    } else {
        Y_UNREACHABLE();
    }
}

#endif

void EnableLogging()
{
    InitializeGlobals();
    ConfigurationManager->EnableLogging();
}

void EnableProfiling()
{
    InitializeGlobals();
    ConfigurationManager->EnableProfiling();
}

void EnableStockpile()
{
    InitializeGlobals();
    TStockpileThread::Get();
}

void SetLargeUnreclaimableCoeff(double value)
{
    InitializeGlobals();
    ConfigurationManager->SetLargeUnreclaimableCoeff(value);
}

void SetSlowCallWarningThreshold(TDuration value)
{
    InitializeGlobals();
    ConfigurationManager->SetSlowCallWarningThreshold(value);
}

TDuration GetSlowCallWarningThreshold()
{
    InitializeGlobals();
    return ConfigurationManager->GetSlowCallWarningThreshold();
}

void SetLargeUnreclaimableBytes(size_t value)
{
    InitializeGlobals();
    ConfigurationManager->SetLargeUnreclaimableBytes(value);
}

TEnumIndexedVector<ssize_t, ETotalCounter> GetTotalCounters()
{
    return StatisticsManager->GetTotalCounters();
}

TEnumIndexedVector<ssize_t, ESystemCounter> GetSystemCounters()
{
    return StatisticsManager->GetSystemCounters();
}

TEnumIndexedVector<ssize_t, ESmallCounter> GetSmallCounters()
{
    return StatisticsManager->GetSmallCounters();
}

TEnumIndexedVector<ssize_t, ESmallCounter> GetLargeCounters()
{
    return StatisticsManager->GetLargeCounters();
}

std::array<TEnumIndexedVector<ssize_t, ESmallArenaCounter>, SmallRankCount> GetSmallArenaCounters()
{
    return StatisticsManager->GetSmallArenaCounters();
}

std::array<TEnumIndexedVector<ssize_t, ELargeArenaCounter>, LargeRankCount> GetLargeArenaCounters()
{
    return StatisticsManager->GetLargeArenaCounters();
}

TEnumIndexedVector<ssize_t, EHugeCounter> GetHugeCounters()
{
    return StatisticsManager->GetHugeCounters();
}

TString FormatCounters()
{
    TStringBuilder builder;

    auto formatCounters = [&] (const auto& counters) {
        using T = typename std::decay_t<decltype(counters)>::TIndex;
        builder.AppendString("{");
        TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
        for (auto counter : TEnumTraits<T>::GetDomainValues()) {
            delimitedBuilder->AppendFormat("%v: %v", counter, counters[counter]);
        }
        builder.AppendString("}");
    };

    builder.AppendString("Total = {");
    formatCounters(GetTotalCounters());

    builder.AppendString("}, System = {");
    formatCounters(GetSystemCounters());

    builder.AppendString("}, Small = {");
    formatCounters(GetSmallCounters());

    builder.AppendString("}, Large = {");
    formatCounters(GetLargeCounters());

    builder.AppendString("}, Huge = {");
    formatCounters(GetHugeCounters());

    builder.AppendString("}");
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc
} // namespace NYT

