#include "parallel_reader_memory_manager.h"
#include "chunk_reader_memory_manager.h"
#include "private.h"

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

const TProfiler MultiReaderMemoryManagerProfiler("/chunk_reader/memory");

////////////////////////////////////////////////////////////////////////////////

constexpr auto MaxRecordPerLogLine = 300;
constexpr auto FullStateLogPeriod = TDuration::Seconds(30);

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
        , RequiredMemoryLowerBound_(Options_.TotalReservedMemorySize)
        , FreeMemory_(Options_.TotalReservedMemorySize)
        , ProfilingTagList_(std::move(Options_.ProfilingTagList))
        , ProfilingExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TParallelReaderMemoryManager::OnProfiling, MakeWeak(this)),
            Options_.ProfilingPeriod))
        , Id_(TGuid::Create())
        , Logger(ReaderMemoryManagerLogger().WithTag("ManagerId: %v", Id_))
    {
        if (Options_.EnableProfiling) {
            ProfilingExecutor_->Start();
            Profiler_ = MultiReaderMemoryManagerProfiler.WithSparse();
        }

        YT_LOG_DEBUG("Parallel reader memory manager created (TotalReservedMemory: %v, MaxInitialReaderReservedMemory: %v, "
            "ProfilingEnabled: %v, ProfilingPeriod: %v, DetailedLoggingEnabled: %v)",
            TotalReservedMemory_.load(),
            Options_.MaxInitialReaderReservedMemory,
            Options_.EnableProfiling,
            Options_.ProfilingPeriod,
            Options_.EnableDetailedLogging);
    }

    TChunkReaderMemoryManagerHolderPtr CreateChunkReaderMemoryManager(
        std::optional<i64> reservedMemorySize,
        const TTagList& profilingTagList) override
    {
        YT_VERIFY(!Finalized_);

        auto initialReaderMemory = std::min<i64>(reservedMemorySize.value_or(Options_.MaxInitialReaderReservedMemory), FreeMemory_);
        initialReaderMemory = std::min<i64>(initialReaderMemory, Options_.MaxInitialReaderReservedMemory);
        initialReaderMemory = std::max<i64>(initialReaderMemory, 0);

        auto memoryManagerHolder = TChunkReaderMemoryManager::CreateHolder(
            TChunkReaderMemoryManagerOptions(
                initialReaderMemory,
                profilingTagList,
                Options_.EnableDetailedLogging,
                Options_.MemoryUsageTracker),
            MakeWeak(this));
        auto memoryManager = memoryManagerHolder->Get();
        FreeMemory_ -= initialReaderMemory;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        YT_LOG_DEBUG("Created child chunk reader memory manager (ChildId: %v, InitialReaderMemorySize: %v, FreeMemorySize: %v)",
            memoryManager->GetId(),
            initialReaderMemory,
            FreeMemory_.load());

        ScheduleRebalancing();

        return memoryManagerHolder;
    }

    IMultiReaderMemoryManagerPtr CreateMultiReaderMemoryManager(
        std::optional<i64> requiredMemorySize,
        const TTagList& profilingTagList) override
    {
        YT_VERIFY(!Finalized_);

        auto initialReservedMemory = std::min<i64>(requiredMemorySize.value_or(0), FreeMemory_);
        initialReservedMemory = std::max<i64>(initialReservedMemory, 0);

        TParallelReaderMemoryManagerOptions options{
            .TotalReservedMemorySize = initialReservedMemory,
            .MaxInitialReaderReservedMemory = Options_.MaxInitialReaderReservedMemory,
            .ProfilingTagList = profilingTagList,
            .EnableProfiling = Options_.EnableDetailedLogging,
            .MemoryUsageTracker = Options_.MemoryUsageTracker
        };
        auto memoryManager = New<TParallelReaderMemoryManager>(options, MakeWeak(this), Invoker_);
        FreeMemory_ -= initialReservedMemory;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        YT_LOG_DEBUG("Created child parallel reader memory manager (ChildId: %v, InitialReservedMemory: %v, FreeMemorySize: %v)",
            memoryManager->GetId(),
            initialReservedMemory,
            FreeMemory_.load());

        ScheduleRebalancing();

        return memoryManager;
    }

    void Unregister(IReaderMemoryManagerPtr readerMemoryManager) override
    {
        YT_LOG_DEBUG("Child memory manager unregistration scheduled (ChildId: %v)", readerMemoryManager->GetId());

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoUnregister, MakeWeak(this), std::move(readerMemoryManager)));
    }

    void UpdateMemoryRequirements(IReaderMemoryManagerPtr readerMemoryManager) override
    {
        YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Child memory manager memory requirements update scheduled (ChildId: %v, TotalRequiredMemorySize: %v, "
            "TotalDesiredMemorySize: %v, TotalReservedMemorySize: %v, FreeMemorySize: %v)",
            readerMemoryManager->GetId(),
            TotalRequiredMemory_.load(),
            TotalDesiredMemory_.load(),
            TotalReservedMemory_.load(),
            FreeMemory_.load());

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoUpdateReaderInfo, MakeWeak(this), std::move(readerMemoryManager)));
        ScheduleRebalancing();
    }

    i64 GetRequiredMemorySize() const override
    {
        return std::max<i64>(TotalRequiredMemory_, RequiredMemoryLowerBound_);
    }

    i64 GetDesiredMemorySize() const override
    {
        return std::max<i64>(TotalDesiredMemory_, RequiredMemoryLowerBound_);
    }

    i64 GetReservedMemorySize() const override
    {
        return TotalReservedMemory_;
    }

    void SetReservedMemorySize(i64 size) override
    {
        YT_LOG_DEBUG_UNLESS(GetReservedMemorySize() == size, "Updating reserved memory size (OldReservedMemorySize: %v, NewReservedMemorySize: %v)",
            GetReservedMemorySize(),
            size);

        i64 reservedMemorySizeDelta = size - TotalReservedMemory_;
        TotalReservedMemory_ += reservedMemorySizeDelta;
        FreeMemory_ += reservedMemorySizeDelta;
        ScheduleRebalancing();
    }

    i64 GetFreeMemorySize() override
    {
        return FreeMemory_;
    }

    TFuture<void> Finalize() override
    {
        YT_LOG_DEBUG("Finalizing parallel reader memory manager (AlreadyFinalized: %v)",
            Finalized_.load());

        Finalized_ = true;
        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::TryUnregister, MakeWeak(this)));
        return FinalizeEvent_
            .ToFuture()
            .ToUncancelable();
    }

    const TTagList& GetProfilingTagList() const override
    {
        return ProfilingTagList_;
    }

    void AddChunkReaderInfo(TGuid chunkReaderId) override
    {
        YT_LOG_DEBUG("Chunk reader info added (ChunkReaderId: %v)", chunkReaderId);
    }

    void AddReadSessionInfo(TGuid readSessionId) override
    {
        YT_LOG_DEBUG("Read session info added (ReadSessionId: %v)", readSessionId);
    }

    TGuid GetId() const override
    {
        return Id_;
    }

private:
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

    std::atomic<i64> RequiredMemoryLowerBound_ = 0;

    std::atomic<i64> FreeMemory_ = 0;

    struct TMemoryManagerState
    {
        i64 RequiredMemorySize;
        i64 DesiredMemorySize;
        i64 ReservedMemorySize;
    };
    THashMap<IReaderMemoryManagerPtr, TMemoryManagerState> State_;

    THashMap<TTagList, std::pair<i64, TGauge>> ReservedMemoryByProfilingTags_;

    void UpdateReservedMemory(const TTagList& tags, i64 delta)
    {
        if (!Options_.EnableProfiling) {
            return;
        }

        // TODO(prime@): Update this, once transition to new profiling API is done.
        auto it = ReservedMemoryByProfilingTags_.find(tags);
        if (it == ReservedMemoryByProfilingTags_.end()) {
            auto gauge = Profiler_.WithTags(TTagSet{tags}).Gauge("/usage");

            auto [newIt, ok] = ReservedMemoryByProfilingTags_.emplace(tags, std::pair<i64, TGauge>(0, gauge));
            it = newIt;
        }

        auto& [counter, gauge] = it->second;
        counter += delta;
        gauge.Update(counter);
    }

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

    TPromise<void> FinalizeEvent_ = NewPromise<void>();

    const TTagList ProfilingTagList_;
    TProfiler Profiler_;
    const TPeriodicExecutorPtr ProfilingExecutor_;

    const TGuid Id_;

    const TLogger Logger;

    mutable TInstant LastFullStateLoggingTime_ = TInstant::Now();

    void ScheduleRebalancing()
    {
        if (RebalancingsScheduled_ == 0) {
            YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Scheduling rebalancing");

            ++RebalancingsScheduled_;
            Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoRebalance, MakeWeak(this)));
        }
    }

    //! Performs memory rebalancing between readers. After rebalancing one of the following holds:
    //! 1) All readers have reserved_memory <= required_memory.
    //! 2) All readers have reserved_memory >= required_memory.
    void DoRebalance()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG("Starting memory rebalancing (RebalancingsScheduled: %v)",
            RebalancingsScheduled_.load());

        --RebalancingsScheduled_;

        // Part 1: try to satisfy as many requirements as possible.
        {
            i64 needExtraMemoryToSatisfyAllRequirements = std::max<i64>(0, RequiredMemoryDeficit_ - FreeMemory_);

            YT_LOG_DEBUG("Trying to satisfy children memory requirements (NeedExtraMemoryToSatisfyRequirements: %v)",
                needExtraMemoryToSatisfyAllRequirements);

            // To obtain extra memory we revoke some memory from readers with reserved_memory > required_memory.
            while (needExtraMemoryToSatisfyAllRequirements > 0 && !ReadersWithSatisfiedMemoryRequirement_.empty()) {
                // Take reader with maximum reserved_memory - required_memory.
                auto readerInfo = *ReadersWithSatisfiedMemoryRequirement_.rbegin();
                auto reader = readerInfo.second;

                auto memoryToRevoke = std::min<i64>(needExtraMemoryToSatisfyAllRequirements, readerInfo.first);

                YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Revoking extra memory from child reader (ChildId: %v, MemoryToRevoke: %v)",
                    reader->GetId(),
                    memoryToRevoke);

                // Can't revoke more memory.
                if (memoryToRevoke == 0) {
                    break;
                }

                auto newReservedMemory = GetOrCrash(State_, reader).ReservedMemorySize - memoryToRevoke;

                DoRemoveReaderInfo(reader, false);
                reader->SetReservedMemorySize(newReservedMemory);
                needExtraMemoryToSatisfyAllRequirements -= memoryToRevoke;
                DoAddReaderInfo(reader, false);

                YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Extra memory revoked (FreeMemorySize: %v)", FreeMemory_.load());
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

                YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Added memory to child memory manager to satisfy memory requirement (ChildId: %v, MemoryToAdd: %v, NewReservedMemory: %v, "
                    "FreeMemorySize: %v)",
                    reader->GetId(),
                    memoryToAdd,
                    newReservedMemory,
                    FreeMemory_.load());
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

                YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Added memory to child memory manager (ChildId: %v, MemoryToAdd: %v, NewReservedMemory: %v, "
                    "FreeMemorySize: %v)",
                    reader->GetId(),
                    memoryToAdd,
                    newReservedMemory,
                    FreeMemory_.load());

                --allocationsLeft;
            }

            // More allocations can be done, so schedule another rebalancing.
            if (FreeMemory_ > 0 && !ReadersWithoutDesiredMemoryAmount_.empty()) {
                YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "More allocations can be performed, scheduling another rebalancing "
                    "(FreeMemorySize: %v, ReaderWithoutDesiredMemorySizeCount: %v)",
                    FreeMemory_.load(),
                    ReadersWithoutDesiredMemoryAmount_.size());

                ScheduleRebalancing();
            }

            TryLogFullState();
        }

        YT_LOG_DEBUG("Memory rebalancing finished (FreeMemorySize: %v)", FreeMemory_.load());
    }

    void DoUnregister(const IReaderMemoryManagerPtr& readerMemoryManager)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        DoRemoveReaderInfo(readerMemoryManager, true);
        readerMemoryManager->SetReservedMemorySize(0);

        YT_LOG_DEBUG("Child memory manager unregistered (ChildId: %v, FreeMemorySize: %v)",
            readerMemoryManager->GetId(),
            FreeMemory_.load());
        TryLogFullState();

        TryUnregister();
        ScheduleRebalancing();
    }

    void DoUpdateReaderInfo(const IReaderMemoryManagerPtr& reader)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (State_.find(reader) == State_.end()) {
            // Reader is already unregistered. Do nothing.
            return;
        }

        YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Updating child info (ChildId: %v)",
            reader->GetId());
        TryLogFullState();

        DoRemoveReaderInfo(reader, false);
        DoAddReaderInfo(reader, true);
        ScheduleRebalancing();
    }

    void DoAddReaderInfo(const IReaderMemoryManagerPtr& reader, bool updateMemoryRequirements, bool updateFreeMemory = true)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        UpdateReservedMemory(reader->GetProfilingTagList(), reservedMemory);

        if (reservedMemory < requiredMemory) {
            YT_VERIFY(ReadersWithUnsatisfiedMemoryRequirement_.emplace(requiredMemory - reservedMemory, reader).second);
            RequiredMemoryDeficit_ += requiredMemory - reservedMemory;
        } else {
            YT_VERIFY(ReadersWithSatisfiedMemoryRequirement_.emplace(reservedMemory - requiredMemory, reader).second);
        }

        if (reservedMemory < desiredMemory) {
            YT_VERIFY(ReadersWithoutDesiredMemoryAmount_.emplace(desiredMemory - reservedMemory, reader).second);
        }

        YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Child reader info added (ChildId: %v, RequiredMemorySize: %v, DesiredMemorySize: %v, ReservedMemorySize: %v, "
            "TotalRequiredMemorySize: %v, TotalDesiredMemorySize: %v, FreeMemorySize: %v)",
            reader->GetId(),
            requiredMemory,
            desiredMemory,
            reservedMemory,
            TotalRequiredMemory_.load(),
            TotalDesiredMemory_.load(),
            FreeMemory_.load());
        TryLogFullState();
    }

    void DoRemoveReaderInfo(const IReaderMemoryManagerPtr& reader, bool updateMemoryRequirements)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

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
        UpdateReservedMemory(reader->GetProfilingTagList(), -reservedMemory);

        if (reservedMemory < requiredMemory) {
            YT_VERIFY(ReadersWithUnsatisfiedMemoryRequirement_.erase({requiredMemory - reservedMemory, reader}));
            RequiredMemoryDeficit_ -= requiredMemory - reservedMemory;
        } else {
            YT_VERIFY(ReadersWithSatisfiedMemoryRequirement_.erase({reservedMemory - requiredMemory, reader}));
        }

        if (reservedMemory < desiredMemory) {
            YT_VERIFY(ReadersWithoutDesiredMemoryAmount_.erase({desiredMemory - reservedMemory, reader}));
        }

        YT_LOG_DEBUG_IF(Options_.EnableDetailedLogging, "Child reader info removed (ChildId: %v, RequiredMemorySize: %v, DesiredMemorySize: %v, ReservedMemorySize: %v, "
            "TotalRequiredMemorySize: %v, TotalDesiredMemorySize: %v, FreeMemorySize: %v)",
            reader->GetId(),
            requiredMemory,
            desiredMemory,
            reservedMemory,
            TotalRequiredMemory_.load(),
            TotalDesiredMemory_.load(),
            FreeMemory_.load());
        TryLogFullState();
    }

    void TryUnregister()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (Finalized_ && State_.empty() && !Unregistered_) {
            YT_LOG_DEBUG("Parallel reader memory manager unregistered");
            Unregistered_ = true;
            if (auto host = Host_.Lock()) {
                host->Unregister(MakeStrong(this));
            }

            FinalizeEvent_.TrySet(TError());
        }

        TryLogFullState();
    }

    void OnProfiling() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        TryLogFullState();
    }

    void TryLogFullState() const
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto now = TInstant::Now();
        if (LastFullStateLoggingTime_ + FullStateLogPeriod > now) {
            return;
        }
        LastFullStateLoggingTime_ = now;

        YT_LOG_DEBUG("Logging full parallel reader memory manager state (RebalancingsScheduled: %v, TotalRequiredMemorySize: %v, "
            "TotalDesiredMemorySize: %v, TotalReservedMemorySize: %v, FreeMemorySize: %v, RequiredMemoryDeficit: %v, Finalized: %v, Unregistered: %v)",
            RebalancingsScheduled_.load(),
            TotalRequiredMemory_.load(),
            TotalDesiredMemory_.load(),
            TotalReservedMemory_.load(),
            FreeMemory_.load(),
            RequiredMemoryDeficit_,
            Finalized_.load(),
            Unregistered_);

        class TBatchLogger
        {
        public:
            TBatchLogger(TString name, TLogger logger)
                : Name_(name)
                , Logger(logger)
            { }

            ~TBatchLogger()
            {
                if (RecordCount_ > 0) {
                    Builder_.AppendString(")");
                    YT_LOG_DEBUG(Builder_.Flush());
                }
            }

            void LogRecord(const TString& record)
            {
                if (RecordCount_ == 0) {
                    Builder_.AppendFormat("Logging batch of %v (", Name_);
                } else {
                    Builder_.AppendString(", ");
                }
                Builder_.AppendString(record);
                ++RecordCount_;
                if (RecordCount_ == MaxRecordPerLogLine) {
                    Builder_.AppendString(")");
                    YT_LOG_DEBUG(Builder_.Flush());
                    Builder_.Reset();
                    RecordCount_ = 0;
                }
            }

        private:
            TString Name_;
            const TLogger Logger;
            TStringBuilder Builder_;
            int RecordCount_ = 0;
        };

        {
            TBatchLogger logger("reader infos", Logger);
            for (const auto& [reader, state] : State_) {
                logger.LogRecord(Format("%v: {RequiredMemorySize: %v, DesiredMemorySize: %v, ReservedMemorySize: %v}",
                    reader->GetId(),
                    state.RequiredMemorySize,
                    state.DesiredMemorySize,
                    state.ReservedMemorySize));
            }
        }
        {
            TBatchLogger logger("readers with unsatisfied memory requirement", Logger);
            for (const auto& [delta, reader] : ReadersWithUnsatisfiedMemoryRequirement_) {
                logger.LogRecord(Format("%v: %v", reader->GetId(), delta));
            }
        }
        {
            TBatchLogger logger("readers with satisfied memory requirement", Logger);
            for (const auto& [delta, reader] : ReadersWithSatisfiedMemoryRequirement_) {
                logger.LogRecord(Format("%v: %v", reader->GetId(), delta));
            }
        }
        {
            TBatchLogger logger("readers without desired memory amount", Logger);
            for (const auto& [delta, reader] : ReadersWithoutDesiredMemoryAmount_) {
                logger.LogRecord(Format("%v: %v", reader->GetId(), delta));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IMultiReaderMemoryManagerPtr CreateParallelReaderMemoryManager(
    TParallelReaderMemoryManagerOptions options,
    IInvokerPtr invoker)
{
    return New<TParallelReaderMemoryManager>(std::move(options), nullptr, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
