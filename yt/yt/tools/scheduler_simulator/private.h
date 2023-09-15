#pragma once

#include <yt/yt/server/scheduler/allocation.h>

#include <yt/yt/core/misc/intrusive_ptr.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(ISimulatorOperationController)
DECLARE_REFCOUNTED_CLASS(TOperation)
DECLARE_REFCOUNTED_CLASS(TSimulatorOperationController)
DECLARE_REFCOUNTED_CLASS(TSimulatorNodeWorker)
DECLARE_REFCOUNTED_CLASS(TSimulatorNodeShard)

inline const NLogging::TLogger SchedulerSimulatorLogger("Simulator");

// TODO: move to another place.
template <typename K, typename V>
class TLockProtectedMap
{
public:
    V& Get(const K& key)
    {
        auto guard = ReaderGuard(Lock_);
        return GetOrCrash(Map_, key);
    }

    const V& Get(const K& key) const
    {
        auto guard = ReaderGuard(Lock_);
        return GetOrCrash(Map_, key);
    }

    template <typename F>
    void ApplyRead(F f) const
    {
        auto guard = ReaderGuard(Lock_);
        for (const auto& elem : Map_) {
            f(elem);
        }
    }

    void Insert(const K& key, V value)
    {
        auto guard = WriterGuard(Lock_);
        YT_VERIFY(Map_.find(key) == Map_.end());
        Map_.emplace(key, std::move(value));
    }

    void Erase(const K& key)
    {
        auto guard = WriterGuard(Lock_);
        auto it = Map_.find(key);
        YT_VERIFY(it != Map_.end());
        Map_.erase(it);
    }

private:
    THashMap<K, V> Map_;
    NThreading::TReaderWriterSpinLock Lock_;
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

NChunkClient::TMediumDirectoryPtr CreateDefaultMediumDirectory();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator
