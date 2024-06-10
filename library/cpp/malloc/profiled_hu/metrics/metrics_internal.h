#pragma once

#include "metrics.h"

#include <mutex>

constexpr bool HuMetricsEnabled = true;
constexpr uint64_t HuPow2Max = 16;

struct THuExtendedMetrics : THuMetrics {
    uint64_t Pow2Allocations[HuPow2Max];
    uint64_t Pow2Frees[HuPow2Max];
};

struct THuAtomicMetrics {
    std::atomic<uint64_t> Pow2Allocations[HuPow2Max];
    std::atomic<uint64_t> Pow2Frees[HuPow2Max];
    std::atomic<uint64_t> AllocatedBytes;
    std::atomic<uint64_t> FreedBytes;
    std::atomic<uint64_t> AllocationsCount;
    std::atomic<uint64_t> FreesCount;
};

#define HU_METRICS_COUNTERS_FOR_EACH(macro) \
    macro(AllocatedBytes)                      \
    macro(FreedBytes)                          \
    macro(AllocationsCount)                    \
    macro(FreesCount)                          \
    for (uint64_t i = 0; i < HuPow2Max; ++i) { \
        macro(Pow2Allocations[i])              \
    }                                          \
    for (uint64_t i = 0; i < HuPow2Max; ++i) { \
        macro(Pow2Frees[i])                    \
    }

struct THuMetricsThreadListItem {
    THuMetricsThreadListItem* Left;
    THuMetricsThreadListItem* Right;

    THuAtomicMetrics& GetMetrics()
    {
        return *reinterpret_cast<THuAtomicMetrics*>(MetricsData_);
    }
private:
    // std::atomic is not trivially constructible
    // but we need THuMetricsThreadListItem to be trivially constructible, otherwise
    // global zero-initializing constructor can be called in a bad time (after some allocations have happened).
    char MetricsData_[sizeof(THuAtomicMetrics)];
};

class THuMetricsList {
public:
    static void Init() {
        std::lock_guard<std::mutex> guard(Mutex);
#define HU_METRICS_ZERO_INIT(field) \
    GetMetricsOfEjectedItems().field = 0;
        HU_METRICS_COUNTERS_FOR_EACH(HU_METRICS_ZERO_INIT)
#undef HU_METRICS_ZERO_INIT
        LeftCap.Left = nullptr;
        LeftCap.Right = &RightCap;
        RightCap.Left = &LeftCap;
        RightCap.Right = nullptr;
    }
    static void Insert(THuMetricsThreadListItem* item)
    {
        std::lock_guard<std::mutex> guard(Mutex);
        item->Left = RightCap.Left;
        item->Right = &RightCap;
        RightCap.Left->Right = item;
        RightCap.Left = item;
    }
    // Erase element from list and flush its counters to LeftCap.
    static void Eject(THuMetricsThreadListItem* item)
    {
        std::lock_guard<std::mutex> guard(Mutex);
#define HU_METRICS_FLUSH_COUNTER(field) \
    GetMetricsOfEjectedItems().field.fetch_add(item->GetMetrics().field.load(std::memory_order_relaxed), std::memory_order_relaxed);
        HU_METRICS_COUNTERS_FOR_EACH(HU_METRICS_FLUSH_COUNTER)
#undef HU_METRICS_FLUSH_COUNTER
        item->Left->Right = item->Right;
        item->Right->Left = item->Left;
        item->Left = nullptr;
        item->Right = nullptr;
    }
    static THuMetrics GetAggregatedMetrics()
    {
        THuExtendedMetrics metrics = {};
        if (!LeftCap.Right) {
            // Case of using before first allocation (or with another allocator).
            return metrics;
        }
        std::lock_guard<std::mutex> guard(Mutex);
        for (auto* item = &LeftCap; item != &RightCap; item = item->Right) {
#define HU_METRICS_AGGREGATE_COUNTER(field) \
    metrics.field += item->GetMetrics().field.load(std::memory_order_relaxed);
            HU_METRICS_COUNTERS_FOR_EACH(HU_METRICS_AGGREGATE_COUNTER)
#undef HU_METRICS_AGGREGATE_COUNTER
        }
        for (uint64_t i = 0; i < HuPow2Max; ++i) {
            metrics.AllocationsCount += metrics.Pow2Allocations[i];
            metrics.FreesCount += metrics.Pow2Frees[i];
            metrics.AllocatedBytes += metrics.Pow2Allocations[i] << i;
            metrics.FreedBytes += metrics.Pow2Frees[i] << i;
        }
        return metrics;
    }
    static THuAtomicMetrics& GetMetricsOfEjectedItems()
    {
        return LeftCap.GetMetrics();
    }
    static uint64_t Size()
    {
        uint64_t res = 0;
        if (!LeftCap.Right) {
            // Case of using before first allocation (or with another allocator).
            return res;
        }
        std::lock_guard<std::mutex> guard(Mutex);
        for (auto* item = LeftCap.Right; item != &RightCap; item = item->Right) {
            ++res;
        }
        return res;
    }
private:
    inline static std::mutex Mutex;
    // Not inclusive ends of list. There are aggregated counters from ejected items in LeftCap.ThreadMetrics.
    inline static THuMetricsThreadListItem LeftCap;
    inline static THuMetricsThreadListItem RightCap;
};


// Designed to be per-thread static variable with default zero initialization.
class THuThreadMetricsHandle {
public:
    // Safe to call after Finalize. For case of the first thread allocations in thread-local variables destructors.
    void Init()
    {
        if (Inited) [[likely]] {
            return;
        }
        THuMetricsList::Insert(&MainThreadItem);
        Inited = true;
        Normal = true;
    }

    void Finalize()
    {
        if (Finalized) {
            return;
        }
        Init();
        THuMetricsList::Eject(&MainThreadItem);
        Finalized = true;
        Normal = false;
    }

    THuAtomicMetrics& GetMetrics()
    {
        return MainThreadItem.GetMetrics();
    }

    // Report* can be safely called before Init and after Finalize.
    // All allocations must be called after .Init()
    // Frees can be called before .Init()
    // *Unsafe methods can be called only when we are sure that .Init() was called and .Finalize() was not.

    void ReportBlockAllocUnsafe()
    {
        return;
    }

    void ReportPow2AllocationUnsafe(uint8_t pow2)
    {
        if constexpr (!HuMetricsEnabled) {
            return;
        }
        auto& counter = GetMetrics().Pow2Allocations[pow2];
        counter.store(counter.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed); // addq
    }

    void ReportPow2FreeUnsafe(uint8_t pow2)
    {
        if constexpr (!HuMetricsEnabled) {
            return;
        }
        auto& counter = GetMetrics().Pow2Frees[pow2];
        counter.store(counter.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed); // addq
    }

    void ReportPow2Free(uint8_t pow2)
    {
        if constexpr (!HuMetricsEnabled) {
            return;
        }
        if (Normal) [[likely]] {
            ReportPow2FreeUnsafe(pow2);
        } else {
            THuMetricsList::GetMetricsOfEjectedItems().Pow2Frees[pow2].fetch_add(1u, std::memory_order_relaxed); // lock addq
        }
    }

    void ReportAllocation(uint64_t size)
    {
        if constexpr (!HuMetricsEnabled) {
            return;
        }
        if (!Finalized) [[likely]] {
            auto& count = GetMetrics().AllocationsCount;
            count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            auto& bytes = GetMetrics().AllocatedBytes;
            bytes.store(bytes.load(std::memory_order_relaxed) + size, std::memory_order_relaxed);
        } else {
            THuMetricsList::GetMetricsOfEjectedItems().AllocationsCount.fetch_add(1u, std::memory_order_relaxed);
            THuMetricsList::GetMetricsOfEjectedItems().AllocatedBytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

    void ReportFree(uint64_t size)
    {
        if constexpr (!HuMetricsEnabled) {
            return;
        }
        if (Normal) [[likely]] {
            auto& count = GetMetrics().FreesCount;
            count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
            auto& bytes = GetMetrics().FreedBytes;
            bytes.store(bytes.load(std::memory_order_relaxed) + size, std::memory_order_relaxed);
        } else {
            THuMetricsList::GetMetricsOfEjectedItems().FreesCount.fetch_add(1u, std::memory_order_relaxed);
            THuMetricsList::GetMetricsOfEjectedItems().FreedBytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

private:
    THuMetricsThreadListItem MainThreadItem;
    bool Inited;
    bool Finalized;
    bool Normal;
};
