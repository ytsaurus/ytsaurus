#pragma once

#include <library/cpp/threading/hazard_pointer/utils/macro.h>

#include <algorithm>
#include <atomic>

namespace NLockFreeMap::NPrivate {

    class TSizeTracker {
    public:
        void Inc(unsigned value = 1) noexcept;
        void Dec(unsigned value = 1) noexcept;

        unsigned long Size() const noexcept;

    private:
        CACHE_LINE_ALIGNAS std::atomic<long> Value_{};
    };

    inline void TSizeTracker::Inc(unsigned value) noexcept {
        Value_.fetch_add(value, std::memory_order_relaxed);
    }

    inline void TSizeTracker::Dec(unsigned value) noexcept {
        Value_.fetch_sub(value, std::memory_order_relaxed);
    }

    /**
     * Returns the current number of elements in the container.
     *
     * There can be a logical race between element insertion and deletion operations
     * where Value_ temporarily becomes negative. The scenario:
     *
     *   1. Thread A adds an element to the container,
     *      but hasn't yet called Inc().
     *   2. Thread B removes this element and calls Dec().
     *   3. Value_ becomes negative.
     *   4. Thread A finally calls Inc() - the value is corrected.
     *
     * To avoid returning an incorrect large value (result of converting
     * negative long to unsigned long), the value is clamped to zero
     * using std::max(0l, loaded).
     */
    inline unsigned long TSizeTracker::Size() const noexcept {
        auto loaded = Value_.load(std::memory_order_relaxed);
        return std::max(0l, loaded);
    }

} // namespace NLockFreeMap::NPrivate
