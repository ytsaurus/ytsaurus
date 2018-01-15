#include "shuffle_chunk_pool.h"

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NChunkPools {

using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;

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
    , public TRefTracked<TShuffleChunkPool>
{
public:
    //! For persistence only.
    TShuffleChunkPool()
        : DataWeightThreshold(-1)
    { }

    TShuffleChunkPool(
        int partitionCount,
        i64 dataSizeThreshold)
        : DataWeightThreshold(dataSizeThreshold)
    {
        Outputs.resize(partitionCount);
        for (int index = 0; index < partitionCount; ++index) {
            Outputs[index].reset(new TOutput(this, index));
        }
    }

    // IShuffleChunkPool implementation.

    virtual IChunkPoolInput* GetInput() override
    {
        return this;
    }

    virtual IChunkPoolOutput* GetOutput(int partitionIndex) override
    {
        return Outputs[partitionIndex].get();
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = static_cast<int>(InputStripes.size());

        TInputStripe inputStripe;
        inputStripe.ElementaryIndexBegin = static_cast<int>(ElementaryStripes.size());

        for (const auto& dataSlice : stripe->DataSlices) {
            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunkOrThrow();

            int elementaryIndex = static_cast<int>(ElementaryStripes.size());
            auto elementaryStripe = New<TChunkStripe>(dataSlice);
            ElementaryStripes.push_back(elementaryStripe);

            const auto* partitionsExt = chunkSpec->PartitionsExt().get();
            YCHECK(partitionsExt);
            YCHECK(partitionsExt->row_counts_size() == Outputs.size());
            YCHECK(partitionsExt->uncompressed_data_sizes_size() == Outputs.size());

            for (int index = 0; index < static_cast<int>(Outputs.size()); ++index) {
                YCHECK(partitionsExt->row_counts(index) <= RowCountThreshold);
                Outputs[index]->AddStripe(
                    elementaryIndex,
                    // NB: currently uncompressed data size and data weight for partition chunks are roughly
                    // equal, since we use horizontal chunk format.
                    partitionsExt->uncompressed_data_sizes(index),
                    partitionsExt->row_counts(index));
            }

            chunkSpec->ReleaseBoundaryKeys();
            chunkSpec->ReleasePartitionsExt();
        }

        inputStripe.ElementaryIndexEnd = static_cast<int>(ElementaryStripes.size());
        InputStripes.push_back(inputStripe);

        return cookie;
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        const auto& inputStripe = InputStripes[cookie];
        for (int index = inputStripe.ElementaryIndexBegin; index < inputStripe.ElementaryIndexEnd; ++index) {
            for (const auto& output : Outputs) {
                output->SuspendStripe(index);
            }
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        // Remove all partition extensions.
        for (const auto& dataSlice : stripe->DataSlices) {
            // NB: TShuffleChunkPool contains only chunks from unversioned tables.
            const auto& chunkSpec = dataSlice->GetSingleUnversionedChunkOrThrow();
            chunkSpec->ReleaseBoundaryKeys();
            chunkSpec->ReleasePartitionsExt();
        }

        // Although the sizes and even the row count may have changed (mind unordered reader and
        // possible undetermined mappers in partition jobs), we ignore it and use counter values
        // from the initial stripes, hoping that nobody will recognize it. This may lead to
        // incorrect memory consumption estimates but significant bias is very unlikely.
        const auto& inputStripe = InputStripes[cookie];
        int stripeCount = inputStripe.ElementaryIndexEnd - inputStripe.ElementaryIndexBegin;
        int limit = std::min(static_cast<int>(stripe->DataSlices.size()), stripeCount - 1);

        // Fill the initial range of elementary stripes with new chunks (one per stripe).
        for (int index = 0; index < limit; ++index) {
            auto dataSlice = stripe->DataSlices[index];
            int elementaryIndex = index + inputStripe.ElementaryIndexBegin;
            ElementaryStripes[elementaryIndex] = New<TChunkStripe>(dataSlice);
        }

        // Cleanup the rest of elementary stripes.
        for (int elementaryIndex = inputStripe.ElementaryIndexBegin + limit;
             elementaryIndex < inputStripe.ElementaryIndexEnd;
             ++elementaryIndex)
        {
            ElementaryStripes[elementaryIndex] = New<TChunkStripe>();
        }

        // Put remaining chunks (if any) into the last stripe.
        auto& lastElementaryStripe = ElementaryStripes[inputStripe.ElementaryIndexBegin + limit];
        for (int index = limit; index < static_cast<int>(stripe->DataSlices.size()); ++index) {
            auto dataSlice = stripe->DataSlices[index];
            lastElementaryStripe->DataSlices.push_back(dataSlice);
        }

        for (int elementaryIndex = inputStripe.ElementaryIndexBegin;
             elementaryIndex < inputStripe.ElementaryIndexEnd;
             ++elementaryIndex)
        {
            for (const auto& output : Outputs) {
                output->ResumeStripe(elementaryIndex);
            }
        }
    }

    virtual void Finish() override
    {
        if (Finished)
            return;

        TChunkPoolInputBase::Finish();

        for (const auto& output : Outputs) {
            output->FinishInput();
        }
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);

        using NYT::Persist;
        Persist(context, DataWeightThreshold);
        Persist(context, Outputs);
        Persist(context, InputStripes);
        Persist(context, ElementaryStripes);
    }

private:
    using ERunState = EShuffleChunkPoolRunState;

    DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool, 0xbacd518a);

    // NB: sort job cannot handle more than numeric_limits<i32>::max() rows.
    static const i64 RowCountThreshold = std::numeric_limits<i32>::max();

    i64 DataWeightThreshold;

    class TOutput
        : public TChunkPoolOutputWithCountersBase
        , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    {
    public:
        //! For persistence only.
        TOutput()
            : Owner(nullptr)
            , PartitionIndex(-1)
        { }

        explicit TOutput(
            TShuffleChunkPool* owner,
            int partitionIndex)
            : Owner(owner)
            , PartitionIndex(partitionIndex)
        {
            JobCounter->Set(0);
            AddNewRun();
        }

        void AddStripe(int elementaryIndex, i64 dataWeight, i64 rowCount)
        {
            auto* run = &Runs.back();
            if (run->TotalDataWeight > 0) {
                if (run->TotalDataWeight + dataWeight > Owner->DataWeightThreshold ||
                    run->TotalRowCount + rowCount > Owner->RowCountThreshold)
                {
                    SealLastRun();
                    AddNewRun();
                    run = &Runs.back();
                }
            }

            YCHECK(elementaryIndex == run->ElementaryIndexEnd);
            run->ElementaryIndexEnd = elementaryIndex + 1;
            run->TotalDataWeight += dataWeight;
            run->TotalRowCount += rowCount;

            DataWeightCounter->Increment(dataWeight);
            RowCounter->Increment(rowCount);
        }

        void SuspendStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                run->IsApproximate = true;
                ++run->SuspendCount;
                UpdatePendingRunSet(*run);
            }
        }

        void ResumeStripe(int elementaryIndex)
        {
            auto* run = FindRun(elementaryIndex);
            if (run) {
                --run->SuspendCount;
                YCHECK(run->SuspendCount >= 0);
                UpdatePendingRunSet(*run);
            }
        }

        void FinishInput()
        {
            auto& lastRun = Runs.back();
            if (lastRun.TotalDataWeight > 0) {
                SealLastRun();
            } else {
                Runs.pop_back();
            }
        }

        // IChunkPoolOutput implementation.

        virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
        {
            YCHECK(!Runs.empty());
            YCHECK(GetPendingJobCount() > 0);

            TChunkStripeStatisticsVector result(1);

            // This is the next run to be given by #Extract.
            auto it = PendingRuns.begin();
            auto cookie = *it;
            auto& run = Runs[cookie];

            auto& stat = result.front();

            // NB: cannot estimate MaxBlockSize here.
            stat.ChunkCount = run.ElementaryIndexEnd - run.ElementaryIndexBegin;
            stat.DataWeight = run.TotalDataWeight;
            stat.RowCount = run.TotalRowCount;

            if (run.IsApproximate) {
                stat.DataWeight *= ApproximateSizesBoostFactor;
                stat.RowCount *= ApproximateSizesBoostFactor;
            }

            return result;
        }

        virtual bool IsCompleted() const override
        {
            return
                Owner->Finished &&
                JobCounter->GetCompletedTotal() == Runs.size();
        }

        virtual int GetTotalJobCount() const override
        {
            int result = static_cast<int>(Runs.size());
            // Handle empty last run properly.
            if (!Runs.empty() && Runs.back().TotalDataWeight == 0) {
                --result;
            }
            return result;
        }

        virtual int GetPendingJobCount() const override
        {
            return static_cast<int>(PendingRuns.size());
        }

        virtual i64 GetLocality(TNodeId /*nodeId*/) const override
        {
            Y_UNREACHABLE();
        }

        virtual TCookie Extract(TNodeId /*nodeId*/) override
        {
            if (GetPendingJobCount() == 0) {
                return NullCookie;
            }

            auto it = PendingRuns.begin();
            auto cookie = *it;
            PendingRuns.erase(it);

            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Pending);
            run.State = ERunState::Running;

            JobCounter->Increment(1);
            JobCounter->Start(1);
            DataWeightCounter->Start(run.TotalDataWeight);
            RowCounter->Start(run.TotalRowCount);

            return cookie;
        }

        virtual TChunkStripeListPtr GetStripeList(TCookie cookie) override
        {
            const auto& run = Runs[cookie];

            auto list = New<TChunkStripeList>();
            list->PartitionTag = PartitionIndex;
            for (int index = run.ElementaryIndexBegin; index < run.ElementaryIndexEnd; ++index) {
                auto stripe = Owner->ElementaryStripes[index];
                list->Stripes.push_back(stripe);
                list->TotalChunkCount += stripe->GetChunkCount();
            }

            // NB: never ever make TotalDataWeight and TotalBoostFactor approximate.
            // Otherwise sort data size and row counters will be severely corrupted
            list->TotalDataWeight = run.TotalDataWeight;
            list->TotalRowCount = run.TotalRowCount;

            list->IsApproximate = run.IsApproximate;

            return list;
        }

        virtual int GetStripeListSliceCount(TCookie cookie) const override
        {
            const auto& run = Runs[cookie];
            return run.ElementaryIndexEnd - run.ElementaryIndexBegin;
        }

        virtual void Completed(TCookie cookie, const TCompletedJobSummary& /* jobSummary */) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Completed;

            JobCounter->Completed(1);
            DataWeightCounter->Completed(run.TotalDataWeight);
            RowCounter->Completed(run.TotalRowCount);
        }

        virtual void Failed(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter->Failed(1);
            DataWeightCounter->Failed(run.TotalDataWeight);
            RowCounter->Failed(run.TotalRowCount);
        }

        virtual void Aborted(TCookie cookie, EAbortReason reason) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Running);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter->Aborted(1, reason);
            DataWeightCounter->Aborted(run.TotalDataWeight, reason);
            RowCounter->Aborted(run.TotalRowCount, reason);
        }

        virtual void Lost(TCookie cookie) override
        {
            auto& run = Runs[cookie];
            YCHECK(run.State == ERunState::Completed);
            run.State = ERunState::Pending;

            UpdatePendingRunSet(run);

            JobCounter->Lost(1);
            DataWeightCounter->Lost(run.TotalDataWeight);
            RowCounter->Lost(run.TotalRowCount);
        }

        // IPersistent implementation.

        virtual void Persist(const TPersistenceContext& context) override
        {
            TChunkPoolOutputWithCountersBase::Persist(context);

            using NYT::Persist;
            Persist(context, Owner);
            Persist(context, PartitionIndex);
            Persist(context, Runs);
            Persist(context, PendingRuns);
        }

        virtual i64 GetDataSliceCount() const
        {
            // Pretend that each output pool has it's own fraction
            // of stripes to get proper estimated statistics.
            return DivCeil<i64>(Owner->ElementaryStripes.size(), Owner->Outputs.size());
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput, 0xba17acf7);

        friend class TShuffleChunkPool;

        TShuffleChunkPool* Owner;
        int PartitionIndex;

        struct TRun
        {
            int ElementaryIndexBegin = 0;
            int ElementaryIndexEnd = 0;
            i64 TotalDataWeight = 0;
            i64 TotalRowCount = 0;
            int SuspendCount = 0;
            ERunState State = ERunState::Initializing;
            bool IsApproximate = false;

            void Persist(const TPersistenceContext& context)
            {
                using NYT::Persist;
                Persist(context, ElementaryIndexBegin);
                Persist(context, ElementaryIndexEnd);
                Persist(context, TotalDataWeight);
                Persist(context, TotalRowCount);
                Persist(context, SuspendCount);
                Persist(context, State);
                Persist(context, IsApproximate);
            }
        };

        std::vector<TRun> Runs;
        yhash_set<TCookie> PendingRuns;


        void UpdatePendingRunSet(const TRun& run)
        {
            TCookie cookie = &run - Runs.data();
            if (run.State == ERunState::Pending && run.SuspendCount == 0) {
                PendingRuns.insert(cookie);
            } else {
                PendingRuns.erase(cookie);
            }
        }

        void AddNewRun()
        {
            TRun run;
            run.ElementaryIndexBegin = Runs.empty() ? 0 : Runs.back().ElementaryIndexEnd;
            run.ElementaryIndexEnd = run.ElementaryIndexBegin;
            Runs.push_back(run);
        }

        TRun* FindRun(int elementaryIndex)
        {
            if (Runs.empty() || elementaryIndex >= Runs.back().ElementaryIndexEnd) {
                return nullptr;
            }

            int lo = 0;
            int hi = static_cast<int>(Runs.size());
            while (lo + 1 < hi) {
                int mid = (lo + hi) / 2;
                const auto& run = Runs[mid];
                if (run.ElementaryIndexBegin <= elementaryIndex) {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }

            auto& run = Runs[lo];
            YCHECK(run.ElementaryIndexBegin <= elementaryIndex && run.ElementaryIndexEnd > elementaryIndex);
            return &run;
        }

        void SealLastRun()
        {
            auto& run = Runs.back();
            YCHECK(run.TotalDataWeight > 0);
            YCHECK(run.State == ERunState::Initializing);
            run.State = ERunState::Pending;
            UpdatePendingRunSet(run);
        }
    };

    std::vector<std::unique_ptr<TOutput>> Outputs;

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

    std::vector<TInputStripe> InputStripes;
    std::vector<TChunkStripePtr> ElementaryStripes;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool);
DEFINE_DYNAMIC_PHOENIX_TYPE(TShuffleChunkPool::TOutput);

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataWeightThreshold)
{
    return std::unique_ptr<IShuffleChunkPool>(new TShuffleChunkPool(
        partitionCount,
        dataWeightThreshold));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
