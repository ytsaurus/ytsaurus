#include "push_based_shuffle_chunk_pool.h"

#include "job_manager.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/misc/enum.h>

#include <algorithm>
#include <cmath>
#include <limits>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NDistributedChunkSessionClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TPushBasedShuffleChunkPoolOptions::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, PartitionCount);
    PHOENIX_REGISTER_FIELD(2, TargetUncompressedDataSizePerJob);
    PHOENIX_REGISTER_FIELD(3, MaxDataSliceCountPerJob);
    PHOENIX_REGISTER_FIELD(4, SealFallbackCompressionRatio);
    PHOENIX_REGISTER_FIELD(5, SealFallbackRowCountPerRecord);
    PHOENIX_REGISTER_FIELD(6, Logger);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPoolOptions);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESealEstimationSource,
    (None)
    (SessionProgress)
    (ObservedProgress)
    (ConfiguredFallback)
);

////////////////////////////////////////////////////////////////////////////////

i64 CheckedMultiply(i64 lhs, i64 rhs)
{
    YT_VERIFY(lhs >= 0);
    YT_VERIFY(rhs >= 0);

    YT_VERIFY(lhs == 0 || rhs <= std::numeric_limits<i64>::max() / lhs);

    return lhs * rhs;
}

i64 DivideAndRoundUpApproximately(i64 lhs, double rhs)
{
    YT_VERIFY(lhs >= 0);
    YT_VERIFY(rhs > 0 && rhs <= 1);

    double result = std::ceil(static_cast<double>(lhs) / rhs);
    YT_VERIFY(
        std::isfinite(result) &&
        result <= static_cast<double>(std::numeric_limits<i64>::max()));
    return SignedSaturationConversion(result);
}

i64 GetRecordCountToReachUncompressedDataSize(
    i64 recordCount,
    i64 uncompressedDataSize,
    i64 desiredUncompressedDataSize)
{
    YT_VERIFY(recordCount > 0);
    YT_VERIFY(uncompressedDataSize > 0);
    YT_VERIFY(desiredUncompressedDataSize > 0);

    double approximateRecordCount = std::ceil(
        static_cast<double>(desiredUncompressedDataSize) *
        recordCount /
        uncompressedDataSize);
    return std::clamp<i64>(
        SignedSaturationConversion(approximateRecordCount),
        1,
        recordCount);
}

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleChunkPool
    : public TChunkPoolInputBase
    , public IPushBasedShuffleChunkPool
{
public:
    TPushBasedShuffleChunkPool() = default;

    explicit TPushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions options)
        : Options_(std::move(options))
        , Logger(Options_.Logger)
    {
        YT_VERIFY(Options_.PartitionCount > 0);
        YT_VERIFY(Options_.TargetUncompressedDataSizePerJob > 0);
        YT_VERIFY(Options_.MaxDataSliceCountPerJob > 0);
        YT_VERIFY(
            Options_.SealFallbackCompressionRatio > 0 &&
            Options_.SealFallbackCompressionRatio <= 1);
        YT_VERIFY(Options_.SealFallbackRowCountPerRecord > 0);

        Outputs_.reserve(Options_.PartitionCount);
        for (int partitionIndex = 0; partitionIndex < Options_.PartitionCount; ++partitionIndex) {
            auto output = New<TOutput>(
                this,
                Logger.WithTag("PartitionIndex", partitionIndex));
            output->GetJobCounter()->AddParent(JobCounter_);
            output->GetDataSliceCounter()->AddParent(DataSliceCounter_);
            Outputs_.push_back(std::move(output));
        }

        YT_TLOG_INFO("Push-based shuffle chunk pool created")
            .With("PartitionCount", Options_.PartitionCount)
            .With("TargetUncompressedDataSizePerJob", Options_.TargetUncompressedDataSizePerJob)
            .With("MaxDataSliceCountPerJob", Options_.MaxDataSliceCountPerJob)
            .With("SealFallbackCompressionRatio", Options_.SealFallbackCompressionRatio)
            .With("SealFallbackRowCountPerRecord", Options_.SealFallbackRowCountPerRecord);
    }

    IPersistentChunkPoolInputPtr GetInput() final
    {
        return this;
    }

    IPersistentChunkPoolOutputPtr GetOutput(int partitionIndex) final
    {
        YT_VERIFY(partitionIndex >= 0 && partitionIndex < std::ssize(Outputs_));
        return Outputs_[partitionIndex];
    }

    i64 GetTotalDataSliceCount() const final
    {
        return DataSliceCounter_->GetTotal();
    }

    i64 GetTotalJobCount() const final
    {
        return JobCounter_->GetTotal();
    }

    IChunkPoolInput::TCookie Add(TChunkStripePtr /*stripe*/) final
    {
        YT_ABORT();
    }

    void Suspend(IChunkPoolInput::TCookie /*cookie*/) final
    {
        YT_ABORT();
    }

    void Resume(IChunkPoolInput::TCookie /*cookie*/) final
    {
        YT_ABORT();
    }

    void Finish() final
    {
        if (Finished) {
            return;
        }

        TChunkPoolInputBase::Finish();

        YT_TLOG_DEBUG("Push-based shuffle chunk pool input finished")
            .With("RegisteredSessionCount", Sessions_.size())
            .With("FinishedSessionCount", FinishedSessionCount_);

        TryFinalizeJobs();
    }

    void RegisterChunkWriteSession(
        int partitionIndex,
        TChunkId chunkId,
        const TChunkReplicaWithMediumList& replicas) final
    {
        YT_VERIFY(!Finished);
        YT_VERIFY(partitionIndex >= 0 && partitionIndex < std::ssize(Outputs_));

        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(chunkId);
        inputChunk->SetReplicas(TChunkReplicaWithMedium::ToChunkReplicas(replicas));
        inputChunk->SetTableIndex(0);
        inputChunk->SetChunkFormat(EChunkFormat::JournalDistributed);

        // TInputChunkSlice reads these before OverrideSize replaces them, and
        // TInputChunk::GetRowCount asserts against TotalRowCount, so they must be nonzero.
        // TInputChunkBase does the same for dynamic stores, whose sizes are equally unknown.
        inputChunk->SetTotalDataWeight(1);
        inputChunk->SetCompressedDataSize(1);
        inputChunk->SetTotalUncompressedDataSize(1);
        inputChunk->SetTotalRowCount(1);

        EmplaceOrCrash(
            Sessions_,
            chunkId,
            TChunkWriteSessionState{
                .PartitionIndex = partitionIndex,
                .InputChunk = std::move(inputChunk),
            });

        YT_TLOG_DEBUG("Chunk write session registered in push-based shuffle chunk pool")
            .With("ChunkId", chunkId)
            .With("PartitionIndex", partitionIndex)
            .With("ReplicaCount", replicas.size())
            .With("RegisteredSessionCount", Sessions_.size());
    }

    void UpdateChunkWriteSession(
        TChunkId chunkId,
        const TDistributedChunkSessionProgress& progress) final
    {
        auto& session = GetOrCrash(Sessions_, chunkId);
        YT_VERIFY(!session.Finished);
        ApplyExactProgress(&session, progress);
    }

    void FinishChunkWriteSession(
        TChunkId chunkId,
        const TDistributedChunkSessionProgress& progress) final
    {
        auto& session = GetOrCrash(Sessions_, chunkId);
        YT_VERIFY(!session.Finished);
        ApplyExactProgress(&session, progress);

        YT_TLOG_DEBUG("Chunk write session finished with exact statistics")
            .With("ChunkId", chunkId)
            .With("PartitionIndex", session.PartitionIndex)
            .With("DataWeight", progress.DataWeight)
            .With("CompressedDataSize", progress.CompressedDataSize)
            .With("UncompressedDataSize", progress.UncompressedDataSize)
            .With("RecordCount", progress.RecordCount)
            .With("RowCount", progress.RowCount);

        TerminateSession(&session);
    }

    void FinishChunkWriteSessionFromSeal(
        TChunkId chunkId,
        const TSessionSealSummary& summary) final
    {
        auto& session = GetOrCrash(Sessions_, chunkId);
        YT_VERIFY(!session.Finished);
        YT_VERIFY(summary.RecordCount >= 0);
        YT_VERIFY(summary.PhysicalCompressedDataSize >= 0);
        YT_VERIFY(summary.RecordCount >= session.Progress.RecordCount);
        YT_VERIFY(
            summary.PhysicalCompressedDataSize >= session.Progress.CompressedDataSize);

        i64 missingRecordCount = summary.RecordCount - session.Progress.RecordCount;
        // NB: Sealed compressed data size accounts for on-disk padding, so it may exceed
        // the sequencer-reported size even when the seal brings no new records. Such a
        // difference is expected and is dropped, since there are no records to attribute
        // it to and the already emitted slices carry the reported sizes.
        i64 missingCompressedDataSize =
            summary.PhysicalCompressedDataSize - session.Progress.CompressedDataSize;

        // Records are compressed independently, so the unobserved suffix must account for
        // at least one compressed byte per record it adds.
        YT_VERIFY(missingCompressedDataSize >= missingRecordCount);

        auto estimationSource = ESealEstimationSource::None;
        TDistributedChunkSessionProgress estimatedStatistics;
        if (missingRecordCount > 0) {
            if (session.Progress.RecordCount > 0) {
                estimationSource = ESealEstimationSource::SessionProgress;
                estimatedStatistics = Extrapolate(
                    session.Progress,
                    missingRecordCount,
                    missingCompressedDataSize);
            } else if (ObservedStatistics_.RecordCount > 0) {
                estimationSource = ESealEstimationSource::ObservedProgress;
                estimatedStatistics = Extrapolate(
                    ObservedStatistics_,
                    missingRecordCount,
                    missingCompressedDataSize);
            } else {
                estimationSource = ESealEstimationSource::ConfiguredFallback;
                i64 uncompressedDataSize = std::max(
                    missingRecordCount,
                    DivideAndRoundUpApproximately(
                        missingCompressedDataSize,
                        Options_.SealFallbackCompressionRatio));
                estimatedStatistics = {
                    .DataWeight = uncompressedDataSize,
                    .CompressedDataSize = missingCompressedDataSize,
                    .UncompressedDataSize = uncompressedDataSize,
                    .RecordCount = missingRecordCount,
                    .RowCount = CheckedMultiply(
                        missingRecordCount,
                        Options_.SealFallbackRowCountPerRecord),
                };
            }
            auto finalStatistics = session.Progress + estimatedStatistics;
            Outputs_[session.PartitionIndex]->AddRange(
                TChunkRange{
                    .InputChunk = session.InputChunk,
                    .LowerRecordIndex = session.Progress.RecordCount,
                    .UpperRecordIndex = summary.RecordCount,
                    .Statistics = estimatedStatistics,
                    .Approximate = true,
                });
            session.Progress = finalStatistics;
        }

        YT_TLOG_DEBUG("Chunk write session finished from seal summary")
            .With("ChunkId", chunkId)
            .With("PartitionIndex", session.PartitionIndex)
            .With("SealRecordCount", summary.RecordCount)
            .With("SealPhysicalCompressedDataSize", summary.PhysicalCompressedDataSize)
            .With("ReportedRecordCount", summary.RecordCount - missingRecordCount)
            .With("EstimationSource", estimationSource)
            .With("EstimatedProgress", estimatedStatistics)
            .With("ObservedProgress", ObservedStatistics_);

        TerminateSession(&session);
    }

private:
    struct TChunkWriteSessionState
    {
        int PartitionIndex = -1;
        TInputChunkPtr InputChunk;
        TDistributedChunkSessionProgress Progress;
        bool Finished = false;

        PHOENIX_DECLARE_TYPE(TChunkWriteSessionState, 0xce00c8f2);
    };

    struct TChunkRange
    {
        TInputChunkPtr InputChunk;
        i64 LowerRecordIndex = 0;
        i64 UpperRecordIndex = 0;
        TDistributedChunkSessionProgress Statistics;
        bool Approximate = false;

        bool CanCombine(const TChunkRange& other) const
        {
            return
                InputChunk == other.InputChunk &&
                UpperRecordIndex == other.LowerRecordIndex;
        }

        void Combine(const TChunkRange& other)
        {
            YT_VERIFY(CanCombine(other));

            UpperRecordIndex = other.UpperRecordIndex;
            Statistics += other.Statistics;
            Approximate |= other.Approximate;
        }

        //! Keeps the first #prefixRecordCount records in this range and returns the
        //! remainder, or null when the whole range is taken. Both halves become
        //! approximate, since the split apportions statistics by record count.
        std::optional<TChunkRange> SplitOff(i64 prefixRecordCount)
        {
            YT_VERIFY(prefixRecordCount > 0 && prefixRecordCount <= Statistics.RecordCount);

            if (prefixRecordCount == Statistics.RecordCount) {
                return std::nullopt;
            }

            auto [prefixStatistics, suffixStatistics] = Split(
                Statistics,
                prefixRecordCount);
            i64 splitRecordIndex = LowerRecordIndex + prefixRecordCount;
            Approximate = true;

            auto suffix = *this;
            suffix.LowerRecordIndex = splitRecordIndex;
            suffix.Statistics = suffixStatistics;

            UpperRecordIndex = splitRecordIndex;
            Statistics = prefixStatistics;

            return suffix;
        }

        PHOENIX_DECLARE_TYPE(TChunkRange, 0x3c0a8677);
    };

    struct TJobBuilder
    {
        std::vector<TChunkRange> Ranges;
        TDistributedChunkSessionProgress Statistics;
        bool Approximate = false;
        THashMap<TChunkId, i64> RangeIndexByChunkId;

        bool CanCombine(const TChunkRange& range) const
        {
            auto it = RangeIndexByChunkId.find(range.InputChunk->GetChunkId());
            return
                it != RangeIndexByChunkId.end() &&
                Ranges[it->second].CanCombine(range);
        }

        bool AppendRange(TChunkRange range)
        {
            Statistics += range.Statistics;
            Approximate |= range.Approximate;

            auto chunkId = range.InputChunk->GetChunkId();
            auto it = RangeIndexByChunkId.find(chunkId);
            if (it != RangeIndexByChunkId.end() && Ranges[it->second].CanCombine(range)) {
                Ranges[it->second].Combine(range);
                return false;
            }

            RangeIndexByChunkId[chunkId] = std::ssize(Ranges);
            Ranges.push_back(std::move(range));
            return true;
        }

        PHOENIX_DECLARE_TYPE(TJobBuilder, 0x9d01b218);
    };

    class TOutput
        : public TChunkPoolOutputWithJobManagerBase
    {
    public:
        TOutput() = default;

        TOutput(TPushBasedShuffleChunkPool* owner, const TLogger& logger)
            : TChunkPoolOutputWithJobManagerBase(logger)
            , Owner_(owner)
        { }

        bool IsCompleted() const final
        {
            return IsCompleted_;
        }

        bool IsSplittable(TCookie /*cookie*/) const final
        {
            return false;
        }

        void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) final
        {
            TChunkPoolOutputWithJobManagerBase::Completed(cookie, jobSummary);
            CheckCompleted();
        }

        void Lost(TCookie cookie) final
        {
            TChunkPoolOutputWithJobManagerBase::Lost(cookie);
            CheckCompleted();
        }

        void CheckCompleted()
        {
            bool wasCompleted = IsCompleted_;
            IsCompleted_ =
                Owner_->JobsFinalized_ &&
                JobManager_->JobCounter()->GetPending() == 0 &&
                JobManager_->JobCounter()->GetRunning() == 0 &&
                JobManager_->JobCounter()->GetSuspended() == 0 &&
                JobManager_->JobCounter()->GetBlocked() == 0;

            if (!wasCompleted && IsCompleted_) {
                Completed_.Fire();
            } else if (wasCompleted && !IsCompleted_) {
                Uncompleted_.Fire();
            }
        }

        void AddRange(TChunkRange range)
        {
            VerifyAtLeastOneUnitPerRecord(range.Statistics);
            YT_VERIFY(range.Statistics.RecordCount > 0);

            while (range.Statistics.RecordCount > 0) {
                if (std::ssize(Builder_.Ranges) >= Owner_->Options_.MaxDataSliceCountPerJob &&
                    !Builder_.CanCombine(range))
                {
                    YT_VERIFY(TryFlushJobBuilder());
                    continue;
                }

                i64 desiredUncompressedDataSize =
                    Owner_->Options_.TargetUncompressedDataSizePerJob - Builder_.Statistics.UncompressedDataSize;
                if (range.Statistics.UncompressedDataSize < desiredUncompressedDataSize) {
                    AppendRangeAndUpdateCounters(std::move(range));
                    return;
                }

                i64 prefixRecordCount = GetRecordCountToReachUncompressedDataSize(
                    range.Statistics.RecordCount,
                    range.Statistics.UncompressedDataSize,
                    desiredUncompressedDataSize);
                auto suffix = range.SplitOff(prefixRecordCount);
                AppendRangeAndUpdateCounters(std::move(range));
                YT_VERIFY(TryFlushJobBuilder());

                if (!suffix) {
                    return;
                }
                range = std::move(*suffix);
            }
        }

        //! Returns |false| when the builder held no ranges and no job was published.
        bool TryFlushJobBuilder()
        {
            if (Builder_.Ranges.empty()) {
                return false;
            }

            auto jobStub = std::make_unique<TJobStub>();
            for (const auto& range : Builder_.Ranges) {
                auto chunkSlice = CreateKeylessInputChunkSlice(range.InputChunk);
                // NB: A distributed journal chunk's row-index space is its record-index
                // space, so the slice limits are record indices; the estimated table row
                // count is attached separately by OverrideSize below.
                chunkSlice->LowerLimit().RowIndex = range.LowerRecordIndex;
                chunkSlice->UpperLimit().RowIndex = range.UpperRecordIndex;
                chunkSlice->OverrideSize(
                    range.Statistics.RowCount,
                    range.Statistics.DataWeight,
                    range.Statistics.CompressedDataSize,
                    range.Statistics.UncompressedDataSize);

                auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
                dataSlice->SetInputStreamIndex(0);
                jobStub->AddDataSlice(
                    dataSlice,
                    IChunkPoolInput::NullCookie,
                    /*isPrimary*/ true);
            }
            jobStub->Finalize();
            jobStub->GetStripeList()->SetApproximate(Builder_.Approximate);

            YT_VERIFY(JobManager_->JobCounter()->GetBlocked() == 1);
            JobManager_->JobCounter()->AddBlocked(-1);
            JobManager_->AddJob(std::move(jobStub));
            Builder_ = {};

            CheckCompleted();

            return true;
        }

    private:
        void AppendRangeAndUpdateCounters(TChunkRange range)
        {
            if (Builder_.Ranges.empty()) {
                // The job exists but cannot be scheduled until the builder fills up or the
                // input finishes; TryFlushJobBuilder hands this placeholder over to Pending.
                JobManager_->JobCounter()->AddBlocked(1);
            }
            if (Builder_.AppendRange(std::move(range))) {
                JobManager_->DataSliceCounter()->AddUncategorized(1);
            }
        }

        TPushBasedShuffleChunkPool* Owner_ = nullptr;
        bool IsCompleted_ = false;
        TJobBuilder Builder_;

        PHOENIX_DECLARE_FRIEND();
        PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOutput, 0x2f5dc106);
    };

    void ApplyExactProgress(
        TChunkWriteSessionState* session,
        const TDistributedChunkSessionProgress& progress)
    {
        auto chunkId = session->InputChunk->GetChunkId();
        VerifyNonnegative(progress);
        YT_VERIFY(IsComponentwiseLessOrEqual(session->Progress, progress));
        if (session->Progress == progress) {
            return;
        }
        auto delta = progress - session->Progress;
        YT_VERIFY(delta.RecordCount > 0);
        TChunkRange range{
            .InputChunk = session->InputChunk,
            .LowerRecordIndex = session->Progress.RecordCount,
            .UpperRecordIndex = progress.RecordCount,
            .Statistics = delta,
        };
        Outputs_[session->PartitionIndex]->AddRange(std::move(range));
        session->Progress = progress;
        ObservedStatistics_ += delta;

        YT_TLOG_TRACE("Chunk write session progress updated in push-based shuffle chunk pool")
            .With("ChunkId", chunkId)
            .With("PartitionIndex", session->PartitionIndex)
            .With("DataWeight", progress.DataWeight)
            .With("CompressedDataSize", progress.CompressedDataSize)
            .With("UncompressedDataSize", progress.UncompressedDataSize)
            .With("RecordCount", progress.RecordCount)
            .With("RowCount", progress.RowCount);
    }

    void TerminateSession(TChunkWriteSessionState* session)
    {
        YT_VERIFY(!session->Finished);
        YT_VERIFY(FinishedSessionCount_ < std::ssize(Sessions_));
        session->Finished = true;
        ++FinishedSessionCount_;
        TryFinalizeJobs();
    }

    void TryFinalizeJobs()
    {
        if (!Finished ||
            FinishedSessionCount_ != std::ssize(Sessions_) ||
            JobsFinalized_)
        {
            return;
        }

        for (const auto& output : Outputs_) {
            output->TryFlushJobBuilder();
        }

        JobsFinalized_ = true;

        YT_TLOG_INFO("Push-based shuffle chunk pool jobs finalized")
            .With("PartitionCount", Options_.PartitionCount)
            .With("SessionCount", Sessions_.size())
            .With("DataSliceCount", GetTotalDataSliceCount())
            .With("TotalJobCount", GetTotalJobCount())
            .With("ObservedProgress", ObservedStatistics_);

        for (const auto& output : Outputs_) {
            output->CheckCompleted();
        }
    }

    TPushBasedShuffleChunkPoolOptions Options_;
    TSerializableLogger Logger;

    TProgressCounterPtr JobCounter_ = New<TProgressCounter>();
    TProgressCounterPtr DataSliceCounter_ = New<TProgressCounter>();

    std::vector<TIntrusivePtr<TOutput>> Outputs_;

    i64 FinishedSessionCount_ = 0;
    bool JobsFinalized_ = false;

    THashMap<TChunkId, TChunkWriteSessionState> Sessions_;
    TDistributedChunkSessionProgress ObservedStatistics_;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPushBasedShuffleChunkPool, 0x9a83b742);
};

void TPushBasedShuffleChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolInputBase>();

    PHOENIX_REGISTER_FIELD(1, Outputs_);
    PHOENIX_REGISTER_FIELD(2, Options_);
    PHOENIX_REGISTER_FIELD(3, JobsFinalized_);
    PHOENIX_REGISTER_FIELD(4, FinishedSessionCount_);
    PHOENIX_REGISTER_FIELD(5, Sessions_);
    PHOENIX_REGISTER_FIELD(6, ObservedStatistics_);
    PHOENIX_REGISTER_FIELD(7, JobCounter_);
    PHOENIX_REGISTER_FIELD(8, DataSliceCounter_);
    PHOENIX_REGISTER_FIELD(9, Logger);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPool);

void TPushBasedShuffleChunkPool::TChunkWriteSessionState::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, PartitionIndex);
    PHOENIX_REGISTER_FIELD(2, InputChunk);
    PHOENIX_REGISTER_FIELD(3, Progress);
    PHOENIX_REGISTER_FIELD(4, Finished);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPool::TChunkWriteSessionState);

void TPushBasedShuffleChunkPool::TChunkRange::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, InputChunk);
    PHOENIX_REGISTER_FIELD(2, LowerRecordIndex);
    PHOENIX_REGISTER_FIELD(3, UpperRecordIndex);
    PHOENIX_REGISTER_FIELD(4, Statistics);
    PHOENIX_REGISTER_FIELD(5, Approximate);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPool::TChunkRange);

void TPushBasedShuffleChunkPool::TJobBuilder::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Ranges);
    PHOENIX_REGISTER_FIELD(2, Statistics);
    PHOENIX_REGISTER_FIELD(3, Approximate);
    PHOENIX_REGISTER_FIELD(4, RangeIndexByChunkId);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPool::TJobBuilder);

void TPushBasedShuffleChunkPool::TOutput::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolOutputWithJobManagerBase>();

    PHOENIX_REGISTER_FIELD(1, Owner_);
    PHOENIX_REGISTER_FIELD(2, IsCompleted_);
    PHOENIX_REGISTER_FIELD(3, Builder_);
}

PHOENIX_DEFINE_TYPE(TPushBasedShuffleChunkPool::TOutput);

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPushBasedShuffleChunkPoolPtr CreatePushBasedShuffleChunkPool(
    const TPushBasedShuffleChunkPoolOptions& options)
{
    return New<TPushBasedShuffleChunkPool>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
