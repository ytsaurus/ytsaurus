#include "parallel_reader_memory_manager.h"
#include "chunk_reader_memory_manager.h"
#include "private.h"

#include <yt/core/actions/invoker.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_api.h>

#include <yt/core/misc/collection_helpers.h>

#include <yt/core/profiling/profiler.h>

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

TParallelReaderMemoryManagerOptions::TParallelReaderMemoryManagerOptions(
    i64 totalReservedMemorySize,
    i64 maxInitialReaderReservedMemory,
    i64 minRequiredMemorySize,
    TTagIdList profilingTagList,
    bool enableDetailedLogging,
    bool enableProfiling,
    TDuration profilingPeriod)
    : TotalReservedMemorySize(totalReservedMemorySize)
    , MaxInitialReaderReservedMemory(maxInitialReaderReservedMemory)
    , MinRequiredMemorySize(minRequiredMemorySize)
    , ProfilingTagList(std::move(profilingTagList))
    , EnableProfiling(enableProfiling)
    , ProfilingPeriod(profilingPeriod)
    , EnableDetailedLogging(enableDetailedLogging)
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
        , Id_(TGuid::Create())
        , Logger(TLogger{ReaderMemoryManagerLogger}
            .AddTag("Id: %v", Id_))
    {
        if (Options_.EnableProfiling) {
            ProfilingExecutor_->Start();
        }

        YT_LOG_DEBUG("Parallel reader memory manager created (TotalReservedMemory: %v, MaxInitialReaderReservedMemory: %v, MinRequiredMemorySize: %v, "
            "ProfilingEnabled: %v, ProfilingPeriod: %v, DetailedLoggingEnabled: %v)",
            TotalReservedMemory_.load(),
            Options_.MaxInitialReaderReservedMemory,
            Options_.MinRequiredMemorySize,
            Options_.EnableProfiling,
            Options_.ProfilingPeriod,
            Options_.EnableDetailedLogging);
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
            TChunkReaderMemoryManagerOptions(initialReaderMemory, std::move(profilingTagList), Options_.EnableDetailedLogging),
            MakeWeak(this));
        FreeMemory_ -= initialReaderMemory;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        YT_LOG_DEBUG("Created child chunk reader memory manager (ChildId: %v, InitialReaderMemorySize: %v, FreeMemorySize: %v)",
            memoryManager->GetId(),
            initialReaderMemory,
            FreeMemory_.load());

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
            std::move(profilingTagList),
            Options_.EnableDetailedLogging
        };
        auto memoryManager = New<TParallelReaderMemoryManager>(options, MakeWeak(this), Invoker_);
        FreeMemory_ -= minRequiredMemorySize;

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoAddReaderInfo, MakeStrong(this), memoryManager, true, false));
        YT_LOG_DEBUG("Created child parallel reader memory manager (ChildId: %v, InitialManagerMemorySize: %v, FreeMemorySize: %v)",
            memoryManager->GetId(),
            minRequiredMemorySize,
            FreeMemory_.load());

        ScheduleRebalancing();

        return memoryManager;
    }

    virtual void Unregister(IReaderMemoryManagerPtr readerMemoryManager) override
    {
        YT_LOG_DEBUG("Child memory manager unregistration scheduled (ChildId: %v)", readerMemoryManager->GetId());

        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::DoUnregister, MakeWeak(this), std::move(readerMemoryManager)));
    }

    virtual void UpdateMemoryRequirements(IReaderMemoryManagerPtr readerMemoryManager) override
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

    virtual i64 GetRequiredMemorySize() const override
    {
        return TotalRequiredMemory_;
    }

    virtual i64 GetDesiredMemorySize() const override
    {
        return TotalDesiredMemory_;
    }

    virtual i64 GetReservedMemorySize() const override
    {
        return TotalReservedMemory_;
    }

    virtual void SetReservedMemorySize(i64 size) override
    {
        YT_LOG_DEBUG_UNLESS(GetReservedMemorySize() == size, "Updating reserved memory size (OldReservedMemorySize: %v, NewReservedMemorySize: %v)",
            GetReservedMemorySize(),
            size);

        i64 reservedMemorySizeDelta = size - TotalReservedMemory_;
        TotalReservedMemory_ += reservedMemorySizeDelta;
        FreeMemory_ += reservedMemorySizeDelta;
        ScheduleRebalancing();
    }

    virtual i64 GetFreeMemorySize() override
    {
        return FreeMemory_;
    }

    virtual void Finalize() override
    {
        YT_LOG_DEBUG("Finalizing parallel reader memory manager (AlreadyFinalized: %v)",
            Finalized_.load());

        Finalized_ = true;
        Invoker_->Invoke(BIND(&TParallelReaderMemoryManager::TryUnregister, MakeWeak(this)));
    }

    virtual const TTagIdList& GetProfilingTagList() const override
    {
        return ProfilingTagList_;
    }

    virtual void AddChunkReaderInfo(TGuid chunkReaderId) override
    {
        Logger.AddTag("chunk_reader_id", chunkReaderId);
    }

    virtual void AddReadSessionInfo(TGuid readSessionId) override
    {
        Logger.AddTag("read_session_id", readSessionId);
    }

    virtual TGuid GetId() const override
    {
        return Id_;
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
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (Finalized_ && State_.empty() && !Unregistered_) {
            YT_LOG_DEBUG("Parallel reader memory manager unregistered");
            Unregistered_ = true;
            if (auto host = Host_.Lock()) {
                host->Unregister(MakeStrong(this));
            }
        }

        TryLogFullState();
    }

    void OnProfiling() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        for (const auto& [tagIdList, memoryUsage] : ReservedMemoryByProfilingTags_) {
            MultiReaderMemoryManagerProfiler.Enqueue("/usage", memoryUsage, EMetricType::Gauge, tagIdList);
        }

        TryLogFullState();
    }

    void TryLogFullState() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
            TLogger Logger;
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

    const TGuid Id_;

    TLogger Logger;

    mutable TInstant LastFullStateLoggingTime_ = TInstant::Now();
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
