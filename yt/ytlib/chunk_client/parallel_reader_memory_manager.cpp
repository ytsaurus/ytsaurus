#include "parallel_reader_memory_manager.h"

#include "chunk_reader_memory_manager.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TParallelReaderMemoryManagerOptions::TParallelReaderMemoryManagerOptions(
    i64 totalMemorySize,
    i64 maxInitialReaderReservedMemory)
    : TotalMemorySize(totalMemorySize)
    , MaxInitialReaderReservedMemory(maxInitialReaderReservedMemory)
{ }

////////////////////////////////////////////////////////////////////////////////

class TParallelReaderMemoryManager
    : public IMultiReaderMemoryManager
    , public IReaderMemoryManagerHost
{
public:
    TParallelReaderMemoryManager(TParallelReaderMemoryManagerOptions options, IInvokerPtr invoker)
        : Options_(std::move(options))
        , Invoker_(std::move(invoker))
        , FreeMemory_(Options_.TotalMemorySize)
    { }

    virtual TChunkReaderMemoryManagerPtr CreateChunkReaderMemoryManager(std::optional<i64> reservedMemorySize = std::nullopt) override
    {
        auto initialReaderMemory = std::min<i64>(reservedMemorySize.value_or(Options_.MaxInitialReaderReservedMemory), FreeMemory_);
        initialReaderMemory = std::min<i64>(initialReaderMemory, Options_.MaxInitialReaderReservedMemory);
        initialReaderMemory = std::max<i64>(initialReaderMemory, 0);

        auto memoryManager = New<TChunkReaderMemoryManager>(
            TChunkReaderMemoryManagerOptions(initialReaderMemory),
            MakeWeak(this));
        FreeMemory_ -= initialReaderMemory;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeWeak(this), memoryManager));
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

private:
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

                auto newReservedMemory = GetOrCrash(ReservedMemory_, reader) - memoryToRevoke;

                DoRemoveReaderInfo(reader);
                reader->SetReservedMemorySize(newReservedMemory);
                needExtraMemoryToSatisfyAllRequirements -= memoryToRevoke;
                FreeMemory_ += memoryToRevoke;
                DoAddReaderInfo(reader);
            }

            while (FreeMemory_ > 0 && !ReadersWithUnsatisfiedMemoryRequirement_.empty()) {
                // Take reader with minimum required_memory - reserved_memory.
                auto readerInfo = *ReadersWithUnsatisfiedMemoryRequirement_.begin();
                auto reader = readerInfo.second;

                auto memoryToAdd = std::min<i64>(FreeMemory_, readerInfo.first);
                memoryToAdd = std::max<i64>(memoryToAdd, 0);

                auto newReservedMemory = GetOrCrash(ReservedMemory_, reader) + memoryToAdd;

                DoRemoveReaderInfo(reader);
                reader->SetReservedMemorySize(newReservedMemory);
                FreeMemory_ -= memoryToAdd;
                DoAddReaderInfo(reader);
            }
        }

        // Part 2: use free memory to give readers desired memory amount.
        {
            // NB(gritukan): doing all possible desired memory allocations can be slow (refer to `TestManyHeavyRebalancings` test)
            // so it's done granularly.
            constexpr auto MaxDesiredMemoryAllocationsPerRebalancing = 100;

            int allocationsLeft = MaxDesiredMemoryAllocationsPerRebalancing;

            while (FreeMemory_ && !ReadersWithoutDesiredMemoryAmount_.empty() && allocationsLeft) {
                // Take reader with minimum desired_memory - reserved_memory.
                auto readerInfo = *ReadersWithoutDesiredMemoryAmount_.begin();
                auto reader = readerInfo.second;

                auto memoryToAdd = std::min<i64>(FreeMemory_, readerInfo.first);
                memoryToAdd = std::max<i64>(memoryToAdd, 0);

                auto newReservedMemory = GetOrCrash(RequiredMemory_, reader) + memoryToAdd;

                DoRemoveReaderInfo(reader);
                reader->SetReservedMemorySize(newReservedMemory);
                FreeMemory_ -= memoryToAdd;
                DoAddReaderInfo(reader);

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

        DoRemoveReaderInfo(readerMemoryManager);
        FreeMemory_ += readerMemoryManager->GetReservedMemorySize();
        readerMemoryManager->SetReservedMemorySize(0);
        ScheduleRebalancing();
    }

    void DoUpdateReaderInfo(const IReaderMemoryManagerPtr& reader)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (RequiredMemory_.find(reader) == RequiredMemory_.end()) {
            // Reader is already unregistered. Do nothing.
            return;
        }

        DoRemoveReaderInfo(reader);
        DoAddReaderInfo(reader);
        ScheduleRebalancing();
    }

    void DoAddReaderInfo(const IReaderMemoryManagerPtr& reader)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto requiredMemory = reader->GetRequiredMemorySize();
        auto desiredMemory = reader->GetDesiredMemorySize();
        auto reservedMemory = reader->GetReservedMemorySize();

        // NB(gritukan): desired_memory < required_memory can happen due the requirements change
        // between desired_memory and required_memory fetching, however condition required_memory <= desired_memory
        // is important.
        desiredMemory = std::max(desiredMemory, requiredMemory);

        if (reservedMemory > desiredMemory) {
            FreeMemory_ += reservedMemory - desiredMemory;
            reader->SetReservedMemorySize(desiredMemory);
            reservedMemory = desiredMemory;
        }

        YT_VERIFY(RequiredMemory_.emplace(reader, requiredMemory).second);
        YT_VERIFY(DesiredMemory_.emplace(reader, desiredMemory).second);
        YT_VERIFY(ReservedMemory_.emplace(reader, reservedMemory).second);

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

    void DoRemoveReaderInfo(const IReaderMemoryManagerPtr& reader)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto requiredMemory = GetOrCrash(RequiredMemory_, reader);
        auto desiredMemory = GetOrCrash(DesiredMemory_, reader);
        auto reservedMemory = GetOrCrash(ReservedMemory_, reader);

        YT_VERIFY(RequiredMemory_.erase(reader));
        YT_VERIFY(DesiredMemory_.erase(reader));
        YT_VERIFY(ReservedMemory_.erase(reader));

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

    TParallelReaderMemoryManagerOptions Options_;
    IInvokerPtr Invoker_;

    std::atomic<int> RebalancingsScheduled_ = 0;

    std::atomic<i64> FreeMemory_ = 0;

    THashMap<IReaderMemoryManagerPtr, i64> RequiredMemory_;
    THashMap<IReaderMemoryManagerPtr, i64> DesiredMemory_;
    THashMap<IReaderMemoryManagerPtr, i64> ReservedMemory_;

    //! Readers with reserved_memory < required_memory ordered by required_memory - reserved_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithUnsatisfiedMemoryRequirement_;

    //! Sum of required_memory - reserved_memory over all readers with reserved_memory < required_memory.
    i64 RequiredMemoryDeficit_ = 0;

    //! Readers with reserved_memory >= required_memory ordered by reserved_memory - required_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithSatisfiedMemoryRequirement_;

    //! Readers with reserved_memory >= required_memory and reserved_memory < desired_memory
    //! ordered by desired_memory - reserved_memory.
    std::set<std::pair<i64, IReaderMemoryManagerPtr>> ReadersWithoutDesiredMemoryAmount_;
};

////////////////////////////////////////////////////////////////////////////////

IMultiReaderMemoryManagerPtr CreateParallelReaderMemoryManager(
    TParallelReaderMemoryManagerOptions options,
    IInvokerPtr invoker)
{
    return New<TParallelReaderMemoryManager>(std::move(options), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChuckClient
