#include "parallel_reader_memory_manager.h"

#include "chunk_reader_memory_manager.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_api.h>

#include <yt/core/misc/collection_helpers.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

const TProfiler MultiReaderMemoryManagerProfiler("/chunk_reader/memory");

////////////////////////////////////////////////////////////////////////////////

TParallelReaderMemoryManagerOptions::TParallelReaderMemoryManagerOptions(
    i64 totalReservedMemorySize,
    i64 maxInitialReaderReservedMemory,
    i64 minRequiredMemorySize,
    TTagIdList profilingTagList,
    bool enableProfiling,
    TDuration profilingPeriod)
    : TotalReservedMemorySize(totalReservedMemorySize)
    , MaxInitialReaderReservedMemory(maxInitialReaderReservedMemory)
    , MinRequiredMemorySize(minRequiredMemorySize)
    , ProfilingTagList(std::move(profilingTagList))
    , EnableProfiling(enableProfiling)
    , ProfilingPeriod(profilingPeriod)
{ }

////////////////////////////////////////////////////////////////////////////////

class TParallelReaderMemoryManager
    : public IMultiReaderMemoryManager
    , public IReaderMemoryManagerHost
{
public:
    TParallelReaderMemoryManager(
        TParallelReaderMemoryManagerOptions options,
        TWeakPtr<IReaderMemoryManagerHost> host,
        IInvokerPtr invoker)
        : Options_(std::move(options))
        , Host_(std::move(host))
        , Invoker_(std::move(invoker))
        , TotalReservedMemory_(Options_.TotalReservedMemorySize)
        , FreeMemory_(Options_.TotalReservedMemorySize)
        , ProfilingTagList_(std::move(Options_.ProfilingTagList))
        , ProfilingExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TParallelReaderMemoryManager::OnProfiling, MakeWeak(this)),
            Options_.ProfilingPeriod))
    {
        if (Options_.EnableProfiling) {
            ProfilingExecutor_->Start();
        }
    }

    virtual TChunkReaderMemoryManagerPtr CreateChunkReaderMemoryManager(
        std::optional<i64> reservedMemorySize,
        TTagIdList profilingTagList) override
    {
        YT_VERIFY(!Finalized_);

        auto initialReaderMemory = std::min<i64>(reservedMemorySize.value_or(Options_.MaxInitialReaderReservedMemory), FreeMemory_);
        initialReaderMemory = std::min<i64>(initialReaderMemory, Options_.MaxInitialReaderReservedMemory);
        initialReaderMemory = std::max<i64>(initialReaderMemory, 0);

        auto memoryManager = New<TChunkReaderMemoryManager>(
            TChunkReaderMemoryManagerOptions(initialReaderMemory, std::move(profilingTagList)),
            MakeWeak(this));
        FreeMemory_ -= initialReaderMemory;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        ScheduleRebalancing();

        return memoryManager;
    }

    virtual IMultiReaderMemoryManagerPtr CreateMultiReaderMemoryManager(
        std::optional<i64> requiredMemorySize,
        TTagIdList profilingTagList) override
    {
        YT_VERIFY(!Finalized_);

        auto minRequiredMemorySize = std::min<i64>(requiredMemorySize.value_or(0), FreeMemory_);
        minRequiredMemorySize = std::max<i64>(minRequiredMemorySize, 0);

        TParallelReaderMemoryManagerOptions options{
            minRequiredMemorySize,
            Options_.MaxInitialReaderReservedMemory,
            minRequiredMemorySize,
            std::move(profilingTagList)
        };
        auto memoryManager = New<TParallelReaderMemoryManager>(options, MakeWeak(this), Invoker_);
        FreeMemory_ -= minRequiredMemorySize;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        ScheduleRebalancing();

        return memoryManager;
    }

    virtual void Unregister(IReaderMemoryManagerPtr readerMemoryManager) override
    {
        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoUnregister, MakeWeak(this), std::move(readerMemoryManager)));
    }

    virtual void UpdateMemoryRequirements(IReaderMemoryManagerPtr readerMemoryManager) override
    {
        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoUpdateReaderInfo, MakeWeak(this), std::move(readerMemoryManager)));
        ScheduleRebalancing();
    }

    virtual i64 GetRequiredMemorySize() const override
    {
        return std::max<i64>(Options_.MinRequiredMemorySize, TotalRequiredMemory_);
    }

    virtual i64 GetDesiredMemorySize() const override
    {
        return std::max<i64>(Options_.MinRequiredMemorySize, TotalDesiredMemory_);
    }

    virtual i64 GetReservedMemorySize() const override
    {
        return TotalReservedMemory_;
    }

    virtual void SetReservedMemorySize(i64 size) override
    {
        i64 reservedMemorySizeDelta = size - TotalReservedMemory_;
        TotalReservedMemory_ += reservedMemorySizeDelta;
        FreeMemory_ += reservedMemorySizeDelta;
        ScheduleRebalancing();
    }

    virtual void Finalize() override
    {
        Finalized_ = true;
        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::TryUnregister, MakeWeak(this)));
    }

    virtual const TTagIdList& GetProfilingTagList() const override
    {
        return ProfilingTagList_;
    }

private:
    struct TMemoryManagerState
    {
        i64 RequiredMemorySize;
        i64 DesiredMemorySize;
        i64 ReservedMemorySize;
    };

    void ScheduleRebalancing()
    {
        if (RebalancingsScheduled_ == 0) {
            ++RebalancingsScheduled_;
            Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoRebalance, MakeWeak(this)));
        }
    }

    //! Performs memory rebalancing between readers. After rebalancing one of the following holds:
    //! 1) All readers have reserved_memory <= required_memory.
    //! 2) All readers have reserved_memory >= required_memory.
    void DoRebalance()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        --RebalancingsScheduled_;

        // Part 1: try to satisfy as many requirements as possible.
        {
            i64 needExtraMemoryToSatisfyAllRequirements = std::max<i64>(0, RequiredMemoryDeficit_ - FreeMemory_);

            // To obtain extra memory we revoke some memory from readers with reserved_memory > required_memory.
            while (needExtraMemoryToSatisfyAllRequirements > 0 && !ReadersWithSatisfiedMemoryRequirement_.empty()) {
                // Take reader with maximum reserved_memory - required_memory.
                auto readerInfo = *ReadersWithSatisfiedMemoryRequirement_.rbegin();
                auto reader = readerInfo.second;

                auto memoryToRevoke = std::min<i64>(needExtraMemoryToSatisfyAllRequirements, readerInfo.first);

                // Can't revoke more memory.
                if (memoryToRevoke == 0) {
                    break;
                }

                auto newReservedMemory = GetOrCrash(State_, reader).ReservedMemorySize - memoryToRevoke;

                DoRemoveReaderInfo(reader, false);
                reader->SetReservedMemorySize(newReservedMemory);
                needExtraMemoryToSatisfyAllRequirements -= memoryToRevoke;
                DoAddReaderInfo(reader, false);
            }

            while (FreeMemory_ > 0 && !ReadersWithUnsatisfiedMemoryRequirement_.empty()) {
                // Take reader with minimum required_memory - reserved_memory.
                auto readerInfo = *ReadersWithUnsatisfiedMemoryRequirement_.begin();
                auto reader = readerInfo.second;

                auto memoryToAdd = std::min<i64>(FreeMemory_, readerInfo.first);
                memoryToAdd = std::max<i64>(memoryToAdd, 0);

                auto newReservedMemory = GetOrCrash(State_, reader).ReservedMemorySize + memoryToAdd;

                DoRemoveReaderInfo(reader, false);
                reader->SetReservedMemorySize(newReservedMemory);
                DoAddReaderInfo(reader, false);
            }
        }

        // Part 2: use free memory to give readers desired memory amount.
        {
            // NB(gritukan): doing all possible desired memory allocations can be slow (refer to `TestManyHeavyRebalancings` test)
            // so it's done granularly.
            constexpr auto MaxDesiredMemoryAllocationsPerRebalancing = 100;

            int allocationsLeft = MaxDesiredMemoryAllocationsPerRebalancing;

            while (FreeMemory_ > 0 && !ReadersWithoutDesiredMemoryAmount_.empty() && allocationsLeft) {
                // Take reader with minimum desired_memory - reserved_memory.
                auto readerInfo = *ReadersWithoutDesiredMemoryAmount_.begin();
                auto reader = readerInfo.second;

                auto memoryToAdd = std::min<i64>(FreeMemory_, readerInfo.first);
                memoryToAdd = std::max<i64>(memoryToAdd, 0);

                auto newReservedMemory = GetOrCrash(State_, reader).ReservedMemorySize + memoryToAdd;

                DoRemoveReaderInfo(reader, false);
                reader->SetReservedMemorySize(newReservedMemory);
                DoAddReaderInfo(reader, false);

                --allocationsLeft;
            }

            // More allocations can be done, so schedule another rebalancing.
            if (FreeMemory_ > 0 && !ReadersWithoutDesiredMemoryAmount_.empty()) {
                ScheduleRebalancing();
            }
        }
    }

    void DoUnregister(const IReaderMemoryManagerPtr& readerMemoryManager)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        DoRemoveReaderInfo(readerMemoryManager, true);
        readerMemoryManager->SetReservedMemorySize(0);
        TryUnregister();
        ScheduleRebalancing();
    }

    void DoUpdateReaderInfo(const IReaderMemoryManagerPtr& reader)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (State_.find(reader) == State_.end()) {
            // Reader is already unregistered. Do nothing.
            return;
        }

        DoRemoveReaderInfo(reader, false);
        DoAddReaderInfo(reader, true);
        ScheduleRebalancing();
    }

    void DoAddReaderInfo(const IReaderMemoryManagerPtr& reader, bool updateMemoryRequirements, bool updateFreeMemory = true)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto requiredMemory = reader->GetRequiredMemorySize();
        auto desiredMemory = reader->GetDesiredMemorySize();
        auto reservedMemory = reader->GetReservedMemorySize();

        // NB(gritukan): desired_memory < required_memory can happen due the requirements change
        // between desired_memory and required_memory fetching, however condition required_memory <= desired_memory
        // is important.
        desiredMemory = std::max(desiredMemory, requiredMemory);

        if (updateFreeMemory) {
            FreeMemory_ -= reservedMemory;
        }

        if (reservedMemory > desiredMemory) {
            FreeMemory_ += reservedMemory - desiredMemory;
            reader->SetReservedMemorySize(desiredMemory);
            reservedMemory = desiredMemory;
        }

        TotalRequiredMemory_ += requiredMemory;
        TotalDesiredMemory_ += desiredMemory;
        if (updateMemoryRequirements) {
            if (auto host = Host_.Lock()) {
                host->UpdateMemoryRequirements(MakeStrong(this));
            }
        }

        TMemoryManagerState state {
            .RequiredMemorySize = requiredMemory,
            .DesiredMemorySize = desiredMemory,
            .ReservedMemorySize = reservedMemory
        };
        YT_VERIFY(State_.emplace(reader, state).second);
        ReservedMemoryByProfilingTags_[reader->GetProfilingTagList()] += reservedMemory;

        if (reservedMemory < requiredMemory) {
            YT_VERIFY(ReadersWithUnsatisfiedMemoryRequirement_.emplace(requiredMemory - reservedMemory, reader).second);
            RequiredMemoryDeficit_ += requiredMemory - reservedMemory;
        } else {
            YT_VERIFY(ReadersWithSatisfiedMemoryRequirement_.emplace(reservedMemory - requiredMemory, reader).second);
        }

        if (reservedMemory < desiredMemory) {
            YT_VERIFY(ReadersWithoutDesiredMemoryAmount_.emplace(desiredMemory - reservedMemory, reader).second);
        }
    }

    void DoRemoveReaderInfo(const IReaderMemoryManagerPtr& reader, bool updateMemoryRequirements)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto requiredMemory = GetOrCrash(State_, reader).RequiredMemorySize;
        auto desiredMemory = GetOrCrash(State_, reader).DesiredMemorySize;
        auto reservedMemory = GetOrCrash(State_, reader).ReservedMemorySize;

        FreeMemory_ += reservedMemory;
        TotalRequiredMemory_ -= requiredMemory;
        TotalDesiredMemory_ -= desiredMemory;
        if (updateMemoryRequirements) {
            if (auto host = Host_.Lock()) {
                host->UpdateMemoryRequirements(MakeStrong(this));
            }
        }

        YT_VERIFY(State_.erase(reader));
        ReservedMemoryByProfilingTags_[reader->GetProfilingTagList()] -= reservedMemory;

        if (reservedMemory < requiredMemory) {
            YT_VERIFY(ReadersWithUnsatisfiedMemoryRequirement_.erase({requiredMemory - reservedMemory, reader}));
            RequiredMemoryDeficit_ -= requiredMemory - reservedMemory;
        } else {
            YT_VERIFY(ReadersWithSatisfiedMemoryRequirement_.erase({reservedMemory - requiredMemory, reader}));
        }

        if (reservedMemory < desiredMemory) {
            YT_VERIFY(ReadersWithoutDesiredMemoryAmount_.erase({desiredMemory - reservedMemory, reader}));
        }
    }

    void TryUnregister()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Finalized_ && State_.empty() && !Unregistered_) {
            Unregistered_ = true;
            if (auto host = Host_.Lock()) {
                host->Unregister(MakeStrong(this));
            }
        }
    }

    void OnProfiling() const
    {
        for (const auto& [tagIdList, memoryUsage] : ReservedMemoryByProfilingTags_) {
            MultiReaderMemoryManagerProfiler.Enqueue("/usage", memoryUsage, EMetricType::Gauge, tagIdList);
        }
    }

    const TParallelReaderMemoryManagerOptions Options_;
    const TWeakPtr<IReaderMemoryManagerHost> Host_;
    const IInvokerPtr Invoker_;

    std::atomic<int> RebalancingsScheduled_ = 0;

    //! Sum of required_memory over all child memory managers.
    std::atomic<i64> TotalRequiredMemory_ = 0;

    //! Sum of desired_memory over all child memory managers.
    std::atomic<i64> TotalDesiredMemory_ = 0;

    //! Amount of memory reserved for this memory manager.
    std::atomic<i64> TotalReservedMemory_ = 0;

    std::atomic<i64> FreeMemory_ = 0;

    THashMap<IReaderMemoryManagerPtr, TMemoryManagerState> State_;

    THashMap<TTagIdList, i64> ReservedMemoryByProfilingTags_;

    std::atomic<bool> Finalized_ = false;

    bool Unregistered_ = false;

    //! Memory managers with reserved_memory < required_memory ordered by required_memory - reserved_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithUnsatisfiedMemoryRequirement_;

    //! Sum of required_memory - reserved_memory over all memory managers with reserved_memory < required_memory.
    i64 RequiredMemoryDeficit_ = 0;

    //! Memory managers with reserved_memory >= required_memory ordered by reserved_memory - required_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithSatisfiedMemoryRequirement_;

    //! Memory managers with reserved_memory >= required_memory and reserved_memory < desired_memory
    //! ordered by desired_memory - reserved_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithoutDesiredMemoryAmount_;

    const TTagIdList ProfilingTagList_;

    const TPeriodicExecutorPtr ProfilingExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

IMultiReaderMemoryManagerPtr CreateParallelReaderMemoryManager(
    TParallelReaderMemoryManagerOptions options,
    IInvokerPtr invoker)
{
    return New<TParallelReaderMemoryManager>(std::move(options), nullptr, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChuckClient
