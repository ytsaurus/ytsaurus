#pragma once

#include <yt/server/scheduler/job.h>

#include <yt/core/misc/intrusive_ptr.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(ISimulatorOperationController)
DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TSimulatorOperationController)
DECLARE_REFCOUNTED_CLASS(TSimulatorNodeShard)

extern const NLogging::TLogger SchedulerSimulatorLogger;

// TODO: move to another place.
template <typename K, typename V>
class TLockProtectedMap
{
public:
    V& Get(const K& key)
    {
        auto guard = NConcurrency::TReaderGuard(Lock_);
        auto it = Map_.find(key);
        YCHECK(it != Map_.end());
        return it->second;
    }

    // TODO: get rid of copy-paste here
    const V& Get(const K& key) const
    {
        auto guard = NConcurrency::TReaderGuard(Lock_);
        auto it = Map_.find(key);
        YCHECK(it != Map_.end());
        return it->second;
    }

    template <typename F>
    void ApplyRead(F f) const
    {
        auto guard = NConcurrency::TReaderGuard(Lock_);
        for (const auto& elem : Map_) {
            f(elem);
        }
    }

    void Insert(const K& key, V value)
    {
        auto guard = NConcurrency::TWriterGuard(Lock_);
        YCHECK(Map_.find(key) == Map_.end());
        Map_.emplace(key, std::move(value));
    }

    void Erase(const K& key)
    {
        auto guard = NConcurrency::TWriterGuard(Lock_);
        auto it = Map_.find(key);
        YCHECK(it != Map_.end());
        Map_.erase(it);
    }

private:
    THashMap<K, V> Map_;
    NConcurrency::TReaderWriterSpinLock Lock_;
};

// This wrapper can be used in concurrent code to create a constant collection of non-constant objects.
// Such collection can be accessed concurrently without locking.
template <typename T>
class TMutable {
public:
    template<typename... TArgs>
    explicit TMutable(TArgs&&... args)
        : TObj_(std::forward<TArgs>(args)...)
    { }

    T& operator*() const noexcept
    {
        return TObj_;
    }

    T* operator->() const noexcept
    {
        return  &TObj_;
    }

private:
    mutable T TObj_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
