#include "shuffle_chunk_pool.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

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

        auto cookie = std::ssize(InputStripes_);

        TInputStripe inputStripe;
        inputStripe.ElementaryIndexBegin = std::ssize(ElementaryStripes_);

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);

            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunk();

            int elementaryIndex = std::ssize(ElementaryStripes_);
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
                    // NB: Currently uncompressed data size and data weight for partition chunks are roughly
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

        inputStripe.ElementaryIndexEnd = std::ssize(ElementaryStripes_);
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

    // NB: Sort job cannot handle more than numeric_limits<i32>::max() rows.
    static const i64 RowCountThreshold_ = std::numeric_limits<i32>::max();

    class TOutput
        : public TChunkPoolOutputWithCountersBase
        , public TJobSplittingBase
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

            // NB: Cannot estimate MaxBlockSize here.
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
            list->Reserve(run.ElementaryIndexEnd - run.ElementaryIndexBegin);

            for (int index = run.ElementaryIndexBegin; index < run.ElementaryIndexEnd; ++index) {
                list->AddStripe(Owner_->ElementaryStripes_[index]);
            }

            // NB: Never ever make TotalDataWeight approximate.
            // Otherwise sort data size and row counters will be severely corrupted

            // NB(apollo1321): Actually, this data weight is uncompressed data size here.
            // This behaviour is incorrect and should be fixed in YT-26516.
            list->SetPartitionTag(PartitionIndex_, run.DataWeight, run.RowCount);

            for (const auto& stripe : list->Stripes()) {
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
            bool wasCompleted = IsCompleted_;
            IsCompleted_ = Owner_->Finished && (JobCounter->GetCompletedTotal() == std::ssize(Runs_));
            if (!wasCompleted && IsCompleted_) {
                Completed_.Fire();
            } else if (wasCompleted && !IsCompleted_) {
                Uncompleted_.Fire();
            }
        }

    private:
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

            PHOENIX_DECLARE_TYPE(TRun, 0x9e82f050);
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
            int hi = std::ssize(Runs_);
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

        PHOENIX_DECLARE_FRIEND();
        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOutput, 0xba17acf7);
    };

    std::vector<TIntrusivePtr<TOutput>> Outputs_;

    struct TInputStripe
    {
        int ElementaryIndexBegin;
        int ElementaryIndexEnd;

        PHOENIX_DECLARE_TYPE(TInputStripe, 0xbde9be7a);
    };

    std::vector<TInputStripe> InputStripes_;
    std::vector<TChunkStripePtr> ElementaryStripes_;

    i64 DataWeightThreshold_ = -1;
    i64 ChunkSliceThreshold_ = -1;
    i64 TotalJobCount_ = 0;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TShuffleChunkPool, 0xbacd518a);
};

void TShuffleChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolInputBase>();

    PHOENIX_REGISTER_FIELD(1, Outputs_);
    PHOENIX_REGISTER_FIELD(2, InputStripes_);
    PHOENIX_REGISTER_FIELD(3, ElementaryStripes_);
    PHOENIX_REGISTER_FIELD(4, DataWeightThreshold_);
    PHOENIX_REGISTER_FIELD(5, ChunkSliceThreshold_);
    PHOENIX_REGISTER_FIELD(6, TotalJobCount_);
}

PHOENIX_DEFINE_TYPE(TShuffleChunkPool);

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

void TShuffleChunkPool::TOutput::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolOutputWithCountersBase>();

    PHOENIX_REGISTER_FIELD(1, Owner_);
    PHOENIX_REGISTER_FIELD(2, PartitionIndex_);
    PHOENIX_REGISTER_FIELD(3, Runs_);
    PHOENIX_REGISTER_FIELD(4, PendingRuns_);
    PHOENIX_REGISTER_FIELD(5, IsCompleted_);
}

PHOENIX_DEFINE_TYPE(TShuffleChunkPool::TOutput);

////////////////////////////////////////////////////////////////////////////////

void TShuffleChunkPool::TOutput::TRun::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ElementaryIndexBegin);
    PHOENIX_REGISTER_FIELD(2, ElementaryIndexEnd);
    PHOENIX_REGISTER_FIELD(3, SuspendCount);
    PHOENIX_REGISTER_FIELD(4, State);
    PHOENIX_REGISTER_FIELD(5, IsApproximate);
    PHOENIX_REGISTER_FIELD(6, RowCount);
    PHOENIX_REGISTER_FIELD(7, DataWeight);
    PHOENIX_REGISTER_FIELD(8, DataWeightProgressCounterGuard);
    PHOENIX_REGISTER_FIELD(9, RowProgressCounterGuard);
    PHOENIX_REGISTER_FIELD(10, JobProgressCounterGuard);
}

PHOENIX_DEFINE_TYPE(TShuffleChunkPool::TOutput::TRun);

////////////////////////////////////////////////////////////////////////////////

void TShuffleChunkPool::TInputStripe::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, ElementaryIndexBegin);
    PHOENIX_REGISTER_FIELD(2, ElementaryIndexEnd);
}

PHOENIX_DEFINE_TYPE(TShuffleChunkPool::TInputStripe);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
