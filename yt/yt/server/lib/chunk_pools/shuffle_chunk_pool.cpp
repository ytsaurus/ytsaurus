#include "shuffle_chunk_pool.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

constexpr int DataSliceCounterUpdatePeriod = 100;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EShuffleChunkPoolRunState,
    (Initializing)
    (Pending)
    (Running)
    (Completed)
);

class TShuffleChunkPool
    : public TChunkPoolInputBase
    , public IShuffleChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    DEFINE_SIGNAL(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL(void(), Completed);
    DEFINE_SIGNAL(void(), Uncompleted);

public:
    //! For persistence only.
    TShuffleChunkPool() = default;

    TShuffleChunkPool(
        int partitionCount,
        i64 dataSizeThreshold,
        i64 chunkSliceThreshold)
        : DataWeightThreshold_(dataSizeThreshold)
        , ChunkSliceThreshold_(chunkSliceThreshold)
    {
        Outputs_.reserve(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            Outputs_.push_back(New<TOutput>(this, index));
        }
    }

    // IShuffleChunkPool implementation.

    IPersistentChunkPoolInputPtr GetInput() override
    {
        return this;
    }

    IPersistentChunkPoolOutputPtr GetOutput(int partitionIndex) override
    {
        return Outputs_[partitionIndex];
    }

    // IPersistentChunkPoolInput implementation.

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(!Finished);

        auto cookie = static_cast<int>(InputStripes_.size());

        TInputStripe inputStripe;
        inputStripe.ElementaryIndexBegin = static_cast<int>(ElementaryStripes_.size());

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);

            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunk();

            int elementaryIndex = static_cast<int>(ElementaryStripes_.size());
            auto elementaryStripe = New<TChunkStripe>(dataSlice);
            ElementaryStripes_.push_back(elementaryStripe);

            const auto* partitionsExt = chunkSpec->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == std::ssize(Outputs_));
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == std::ssize(Outputs_));

            const auto* uncompressedDataSizes = partitionsExt->uncompressed_data_sizes().data();
            const auto* rowCounts = partitionsExt->row_counts().data();
            for (int index = 0; index < std::ssize(Outputs_); ++index) {
                YT_VERIFY(partitionsExt->row_counts(index) <= RowCountThreshold_);
                Outputs_[index]->AddStripe(
                    elementaryIndex,
                    // NB: currently uncompressed data size and data weight for partition chunks are roughly
                    // equal, since we use horizontal chunk format.
                    uncompressedDataSizes[index],
                    rowCounts[index]);
            }

            chunkSpec->ReleaseBoundaryKeys();
            chunkSpec->ReleasePartitionsExt();
        }

        // NB(gritukan): It's quite expensive to update data slice counters during each stripe
        // addition, so we batch such updates.
        if (cookie % DataSliceCounterUpdatePeriod == 0) {
            for (auto& output : Outputs_) {
                output->UpdateDataSliceCount();
            }
        }

        inputStripe.ElementaryIndexEnd = static_cast<int>(ElementaryStripes_.size());
        InputStripes_.push_back(inputStripe);

        return cookie;
    }

    void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        const auto& inputStripe = InputStripes_[cookie];
        for (int index = inputStripe.ElementaryIndexBegin; index < inputStripe.ElementaryIndexEnd; ++index) {
            for (const auto& output : Outputs_) {
                output->SuspendStripe(index);
            }
        }
    }

    void Resume(IChunkPoolInput::TCookie cookie) override
    {
        const auto& inputStripe = InputStripes_[cookie];

        for (int elementaryIndex = inputStripe.ElementaryIndexBegin;
             elementaryIndex < inputStripe.ElementaryIndexEnd;
             ++elementaryIndex)
        {
            for (const auto& output : Outputs_) {
                output->ResumeStripe(elementaryIndex);
            }
        }
    }

    void Finish() override
    {
        if (Finished) {
            return;
        }

        TChunkPoolInputBase::Finish();

        for (const auto& output : Outputs_) {
            output->UpdateDataSliceCount();
            output->FinishInput();
            output->CheckCompleted();
        }
    }

    // IPersistent implementation.

    void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);

        using NYT::Persist;
        Persist(context, Outputs_);
        Persist(context, InputStripes_);
        Persist(context, ElementaryStripes_);
        Persist(context, DataWeightThreshold_);
        Persist(context, ChunkSliceThreshold_);
        Persist(context, TotalJobCount_);
    }

    i64 GetTotalDataSliceCount() const override
    {
        return ElementaryStripes_.size();
    }

    i64 GetTotalJobCount() const override
    {
        return TotalJobCount_;
    }

private:
    using ERunState = EShuffleChunkPoolRunState;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool, 0xbacd518a);

    // NB: sort job cannot handle more than numeric_limits<i32>::max() rows.
    static const i64 RowCountThreshold_ = std::numeric_limits<i32>::max();

    class TOutput
        : public TChunkPoolOutputWithCountersBase
        , public TJobSplittingBase
        , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    {
    public:
        DEFINE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
        DEFINE_SIGNAL_OVERRIDE(void(), Completed);
        DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

    public:
        //! For persistence only.
        TOutput() = default;

        explicit TOutput(
            TShuffleChunkPool* owner,
            int partitionIndex)
            : Owner_(owner)
            , PartitionIndex_(partitionIndex)
        {
            AddNewRun();
        }

        // NB(gritukan): This function is probably the most loaded all over the controller agent
        // as it's called O(partition_job_count * partition_count) times during Sort/MR operations.
        // Keep it _really_ fast.
        void AddStripe(int elementaryIndex, i64 dataWeight, i64 rowCount)
        {
            auto* run = &Runs_.back();
            if (run->DataWeight > 0) {
                if (run->DataWeight + dataWeight > Owner_->DataWeightThreshold_ ||
                    run->RowCount + rowCount > Owner_->RowCountThreshold_ ||
                    run->GetSliceCount() >= Owner_->ChunkSliceThreshold_)
                {
                    SealLastRun();
                    AddNewRun();
                    run = &Runs_.back();
                }
            }

            YT_VERIFY(elementaryIndex == run->ElementaryIndexEnd);
            run->ElementaryIndexEnd = elementaryIndex + 1;
            run->RowCount += rowCount;
            run->DataWeight += dataWeight;
        }

        void SuspendStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                run->IsApproximate = true;
                ++run->SuspendCount;
                UpdateRun(run);
            }
        }

        void ResumeStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                --run->SuspendCount;
                YT_VERIFY(run->SuspendCount >= 0);
                UpdateRun(run);
            }
        }

        void FinishInput()
        {
            auto& lastRun = Runs_.back();
            if (lastRun.DataWeight > 0) {
                SealLastRun();
            } else {
                // Remove last run from counters.
                Runs_.back().CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, EProgressCategory::None);
                Runs_.pop_back();
            }
            CheckCompleted();
        }

        // IPersistentChunkPoolOutput implementation.

        NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
        {
            YT_VERIFY(!Runs_.empty());
            YT_VERIFY(JobCounter->GetPending() > 0);

            NTableClient::TChunkStripeStatisticsVector result(1);

            // This is the next run to be given by #Extract.
            auto it = PendingRuns_.begin();
            auto cookie = *it;
            auto& run = Runs_[cookie];

            auto& stat = result.front();

            // NB: cannot estimate MaxBlockSize here.
            stat.ChunkCount = run.GetSliceCount();
            stat.DataWeight = run.DataWeight;
            stat.RowCount = run.RowCount;

            if (run.IsApproximate) {
                stat.DataWeight *= ApproximateSizesBoostFactor;
                stat.RowCount *= ApproximateSizesBoostFactor;
            }

            return result;
        }

        bool IsCompleted() const override
        {
            return IsCompleted_;
        }

        TCookie Extract(TNodeId /*nodeId*/) override
        {
            if (JobCounter->GetPending() == 0) {
                return IChunkPoolOutput::NullCookie;
            }

            auto it = PendingRuns_.begin();
            auto cookie = *it;
            PendingRuns_.erase(it);

            auto& run = Runs_[cookie];
            YT_VERIFY(run.State == ERunState::Pending);
            run.State = ERunState::Running;
            UpdateRun(&run);

            return cookie;
        }

        TChunkStripeListPtr GetStripeList(TCookie cookie) override
        {
            const auto& run = Runs_[cookie];

            auto list = New<TChunkStripeList>();
            list->PartitionTag = PartitionIndex_;
            for (int index = run.ElementaryIndexBegin; index < run.ElementaryIndexEnd; ++index) {
                auto stripe = Owner_->ElementaryStripes_[index];
                list->Stripes.push_back(stripe);
                list->TotalChunkCount += stripe->GetChunkCount();
            }

            // NB: never ever make TotalDataWeight and TotalBoostFactor approximate.
            // Otherwise sort data size and row counters will be severely corrupted
            list->TotalDataWeight = run.DataWeight;
            list->TotalRowCount = run.RowCount;

            list->IsApproximate = run.IsApproximate;

            for (const auto& stripe : list->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    YT_VERIFY(!dataSlice->IsLegacy);
                }
            }

            return list;
        }

        int GetStripeListSliceCount(TCookie cookie) const override
        {
            const auto& run = Runs_[cookie];
            return run.GetSliceCount();
        }

        void Completed(TCookie cookie, const TCompletedJobSummary& /*jobSummary*/) override
        {
            auto& run = Runs_[cookie];
            YT_VERIFY(run.State == ERunState::Running);
            run.State = ERunState::Completed;
            UpdateRun(&run);
            CheckCompleted();
        }

        void Failed(TCookie cookie) override
        {
            auto& run = Runs_[cookie];
            YT_VERIFY(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            run.CallProgressCounterGuards(&TProgressCounterGuard::OnFailed);
            UpdateRun(&run);
        }

        void Aborted(TCookie cookie, EAbortReason reason) override
        {
            auto& run = Runs_[cookie];
            YT_VERIFY(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            run.CallProgressCounterGuards(&TProgressCounterGuard::OnAborted, reason);
            UpdateRun(&run);
        }

        void Lost(TCookie cookie) override
        {
            auto& run = Runs_[cookie];
            YT_VERIFY(run.State == ERunState::Completed);
            run.State = ERunState::Pending;

            run.CallProgressCounterGuards(&TProgressCounterGuard::OnLost);
            UpdateRun(&run);
            CheckCompleted();
        }

        // IPersistent implementation.

        void Persist(const TPersistenceContext& context) override
        {
            TChunkPoolOutputWithCountersBase::Persist(context);

            using NYT::Persist;
            Persist(context, Owner_);
            Persist(context, PartitionIndex_);
            Persist(context, Runs_);
            Persist(context, PendingRuns_);
            Persist(context, IsCompleted_);
        }

        void UpdateDataSliceCount()
        {
            // Pretend that each output pool has it's own fraction
            // of stripes to get proper estimated statistics.
            auto oldDataSliceCount = GetDataSliceCounter()->GetUncategorized();
            auto newDataSliceCount = DivCeil<i64>(Owner_->ElementaryStripes_.size(), Owner_->Outputs_.size());
            GetDataSliceCounter()->AddUncategorized(newDataSliceCount - oldDataSliceCount);
        }

        void CheckCompleted()
        {
            bool completed = Owner_->Finished && (JobCounter->GetCompletedTotal() == std::ssize(Runs_));
            if (!IsCompleted_ && completed) {
                Completed_.Fire();
            } else if (IsCompleted_ && !completed) {
                Uncompleted_.Fire();
            }

            IsCompleted_ = completed;
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput, 0xba17acf7);

        friend class TShuffleChunkPool;

        TShuffleChunkPool* Owner_ = nullptr;
        int PartitionIndex_ = -1;

        struct TRun
        {
            int ElementaryIndexBegin = 0;
            int ElementaryIndexEnd = 0;
            int SuspendCount = 0;
            ERunState State = ERunState::Initializing;
            bool IsApproximate = false;

            i64 RowCount = 0;
            i64 DataWeight = 0;

            // NB: These counters become active only after job seal.
            TProgressCounterGuard DataWeightProgressCounterGuard;
            TProgressCounterGuard RowProgressCounterGuard;
            TProgressCounterGuard JobProgressCounterGuard;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;
                Persist(context, ElementaryIndexBegin);
                Persist(context, ElementaryIndexEnd);
                Persist(context, SuspendCount);
                Persist(context, State);
                Persist(context, IsApproximate);
                Persist(context, RowCount);
                Persist(context, DataWeight);
                Persist(context, DataWeightProgressCounterGuard);
                Persist(context, RowProgressCounterGuard);
                Persist(context, JobProgressCounterGuard);
            }

            template <class... TArgs>
            void CallProgressCounterGuards(void (TProgressCounterGuard::*Method)(TArgs...), TArgs... args)
            {
                (DataWeightProgressCounterGuard.*Method)(std::forward<TArgs>(args)...);
                (RowProgressCounterGuard.*Method)(std::forward<TArgs>(args)...);
                (JobProgressCounterGuard.*Method)(std::forward<TArgs>(args)...);
            }

            int GetSliceCount() const
            {
                return ElementaryIndexEnd - ElementaryIndexBegin;
            }

            bool IsPending() const
            {
                return State == ERunState::Pending && SuspendCount == 0;
            }

            void UpdateState()
            {
                EProgressCategory newProgressCategory;
                switch (State) {
                    case ERunState::Initializing:
                        if (SuspendCount == 0) {
                            newProgressCategory = EProgressCategory::Blocked;
                        } else {
                            newProgressCategory = EProgressCategory::Suspended;
                        }
                        break;
                    case ERunState::Pending: {
                        if (SuspendCount == 0) {
                            newProgressCategory = EProgressCategory::Pending;
                        } else {
                            newProgressCategory = EProgressCategory::Suspended;
                        }
                        break;
                    }
                    case ERunState::Running:
                        newProgressCategory = EProgressCategory::Running;
                        break;
                    case ERunState::Completed:
                        newProgressCategory = EProgressCategory::Completed;
                        break;
                    default:
                        YT_ABORT();
                }

                CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, newProgressCategory);
            }
        };

        std::vector<TRun> Runs_;
        THashSet<TCookie> PendingRuns_;

        bool IsCompleted_ = false;

        void UpdateRun(TRun* run)
        {
            TCookie cookie = run - Runs_.data();
            if (run->IsPending()) {
                PendingRuns_.insert(cookie);
            } else {
                PendingRuns_.erase(cookie);
            }
            run->UpdateState();
            CheckCompleted();
        }

        void AddNewRun()
        {
            TRun run;
            run.ElementaryIndexBegin = Runs_.empty() ? 0 : Runs_.back().ElementaryIndexEnd;
            run.ElementaryIndexEnd = run.ElementaryIndexBegin;
            run.DataWeightProgressCounterGuard = TProgressCounterGuard(DataWeightCounter, /*value*/ 0);
            run.RowProgressCounterGuard = TProgressCounterGuard(RowCounter, /*value*/ 0);
            run.JobProgressCounterGuard = TProgressCounterGuard(JobCounter, /*value*/ 1);
            run.UpdateState();
            Runs_.push_back(run);
            ++Owner_->TotalJobCount_;
        }

        TRun* FindRun(int elementaryIndex)
        {
            if (Runs_.empty() || elementaryIndex >= Runs_.back().ElementaryIndexEnd) {
                return nullptr;
            }

            int lo = 0;
            int hi = static_cast<int>(Runs_.size());
            while (lo + 1 < hi) {
                int mid = (lo + hi) / 2;
                const auto& run = Runs_[mid];
                if (run.ElementaryIndexBegin <= elementaryIndex) {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }

            auto& run = Runs_[lo];
            YT_VERIFY(run.ElementaryIndexBegin <= elementaryIndex && run.ElementaryIndexEnd > elementaryIndex);
            return &run;
        }

        void SealLastRun()
        {
            auto& run = Runs_.back();
            YT_VERIFY(run.DataWeight > 0);
            YT_VERIFY(run.State == ERunState::Initializing);
            run.State = ERunState::Pending;

            // Set actual values to progress counter guards.
            run.DataWeightProgressCounterGuard.SetValue(run.DataWeight);
            run.RowProgressCounterGuard.SetValue(run.RowCount);
            run.JobProgressCounterGuard.SetValue(1);

            UpdateRun(&run);
        }
    };

    std::vector<TIntrusivePtr<TOutput>> Outputs_;

    struct TInputStripe
    {
        int ElementaryIndexBegin;
        int ElementaryIndexEnd;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, ElementaryIndexBegin);
            Persist(context, ElementaryIndexEnd);
        }
    };

    std::vector<TInputStripe> InputStripes_;
    std::vector<TChunkStripePtr> ElementaryStripes_;

    i64 DataWeightThreshold_ = -1;
    i64 ChunkSliceThreshold_ = -1;
    i64 TotalJobCount_ = 0;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool);
DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput);

IShuffleChunkPoolPtr CreateShuffleChunkPool(
    int partitionCount,
    i64 dataWeightThreshold,
    i64 chunkSliceThreshold)
{
    return New<TShuffleChunkPool>(
        partitionCount,
        dataWeightThreshold,
        chunkSliceThreshold);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
