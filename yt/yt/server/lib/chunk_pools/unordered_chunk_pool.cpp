#include "unordered_chunk_pool.h"

#include "helpers.h"
#include "job_size_adjuster.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/server/lib/chunk_pools/config.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(apollo1321): Remove in 25.2.
DEFINE_ENUM(EUnorderedChunkPoolMode,
    (Normal)
    (AutoMerge)
);

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithCountersBase
    , public IPersistentChunkPool
    , public TJobSplittingBase
    , public virtual NLogging::TLoggerOwner
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

public:
    //! For persistence only.
    TUnorderedChunkPool()
        : MaxBlockSize_(-1)
    { }

    TUnorderedChunkPool(
        const TUnorderedChunkPoolOptions& options,
        TInputStreamDirectory directory)
        : JobSizeConstraints_(options.JobSizeConstraints)
        , Sampler_(JobSizeConstraints_->GetSamplingRate())
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , MinTeleportChunkDataWeight_(options.MinTeleportChunkDataWeight)
        , SliceErasureChunksByParts_(options.SliceErasureChunksByParts)
        , RowBuffer_(options.RowBuffer)
        , InputStreamDirectory_(std::move(directory))
        , JobManager_(New<TNewJobManager>(options.Logger))
        , FreeJobCounter_(New<TProgressCounter>())
        , FreeDataWeightCounter_(New<TProgressCounter>())
        , FreeCompressedDataSizeCounter_(New<TProgressCounter>())
        , FreeRowCounter_(New<TProgressCounter>())
        , SingleChunkTeleportStrategy_(options.SingleChunkTeleportStrategy)
        , UseNewSlicingImplementation_(options.UseNewSlicingImplementation)
    {
        Logger = options.Logger;
        ValidateLogger(Logger);
        // TODO(max42): why do we need row buffer in unordered pool at all?
        YT_VERIFY(RowBuffer_);

        FreeJobCounter_->AddParent(JobCounter);
        FreeDataWeightCounter_->AddParent(DataWeightCounter);
        FreeRowCounter_->AddParent(RowCounter);

        JobManager_->JobCounter()->AddParent(JobCounter);
        JobManager_->DataWeightCounter()->AddParent(DataWeightCounter);
        JobManager_->RowCounter()->AddParent(RowCounter);

        if (JobSizeConstraints_->IsExplicitJobCount()) {
            FreeJobCounter_->SetPending(JobSizeConstraints_->GetJobCount());
        }

        if (options.JobSizeAdjusterConfig && JobSizeConstraints_->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster_ = CreateJobSizeAdjuster(
                JobSizeConstraints_->GetDataWeightPerJob(),
                options.JobSizeAdjusterConfig);
            YT_LOG_DEBUG("Job size adjuster created (Options: %v)",
                ConvertToYsonString(options.JobSizeAdjusterConfig, EYsonFormat::Text));
        }

        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v)",
                *JobSizeConstraints_->GetSamplingRate(),
                JobSizeConstraints_->GetSamplingDataWeightPerJob());
        }

        UpdateFreeJobCounter();
    }

    // IPersistentChunkPoolInput implementation.

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(!Finished);

        auto cookie = InputCookieToInternalCookies_.size();
        InputCookieToInternalCookies_.emplace_back();
        InputCookieIsSuspended_.emplace_back(false);

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);
            // XXX
            dataSlice->GetInputStreamIndex();
            AddDataSlice(dataSlice, cookie);
        }

        UpdateFreeJobCounter();
        CheckCompleted();

        return cookie;
    }

    void Finish() override
    {
        if (!Finished) {
            TChunkPoolInputBase::Finish();
        }

        UpdateFreeJobCounter();
        CheckCompleted();
    }

    void Suspend(IChunkPoolInput::TCookie inputCookie) override
    {
        YT_VERIFY(!InputCookieIsSuspended_[inputCookie]);
        InputCookieIsSuspended_[inputCookie] = true;
        for (auto cookie : InputCookieToInternalCookies_[inputCookie]) {
            DoSuspend(cookie);
        }

        UpdateFreeJobCounter();
    }

    void DoSuspend(IChunkPoolInput::TCookie cookie)
    {
        auto& suspendableStripe = Stripes_[cookie];
        if (!suspendableStripe.Suspend()) {
            return;
        }

        if (!ExtractedStripes_.contains(cookie)) {
            Unregister(cookie);
            const auto& statistics = suspendableStripe.GetStatistics();
            FreeDataWeightCounter_->AddSuspended(statistics.DataWeight);
            if (FreeCompressedDataSizeCounter_) {
                FreeCompressedDataSizeCounter_->AddSuspended(statistics.CompressedDataSize);
            }
            FreeRowCounter_->AddSuspended(statistics.RowCount);
        }

        JobManager_->Suspend(cookie);
    }

    void Resume(IChunkPoolInput::TCookie inputCookie) override
    {
        YT_VERIFY(InputCookieIsSuspended_[inputCookie]);
        InputCookieIsSuspended_[inputCookie] = false;
        for (auto cookie : InputCookieToInternalCookies_[inputCookie]) {
            DoResume(cookie);
        }

        UpdateFreeJobCounter();
    }

    void DoResume(IChunkPoolInput::TCookie cookie)
    {
        auto& suspendableStripe = Stripes_[cookie];
        if (!suspendableStripe.Resume()) {
            return;
        }

        if (!ExtractedStripes_.contains(cookie)) {
            Register(cookie);
            const auto& statistics = suspendableStripe.GetStatistics();
            FreeDataWeightCounter_->AddSuspended(-statistics.DataWeight);
            YT_VERIFY(FreeDataWeightCounter_->GetSuspended() >= 0);
            if (FreeCompressedDataSizeCounter_) {
                FreeCompressedDataSizeCounter_->AddSuspended(-statistics.CompressedDataSize);
                YT_VERIFY(FreeCompressedDataSizeCounter_->GetSuspended() >= 0);
            }
            FreeRowCounter_->AddSuspended(-statistics.RowCount);
            YT_VERIFY(FreeRowCounter_->GetSuspended() >= 0);
        }

        JobManager_->Resume(cookie);
    }

    // IPersistentChunkPoolOutput implementation.

    bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        if (JobManager_->JobCounter()->GetPending() > 0) {
            return JobManager_->GetApproximateStripeStatistics();
        }

        if (GetJobCounter()->GetPending() == 0) {
            return {};
        }

        TChunkStripeStatistics stat;
        // Typically unordered pool has one chunk per stripe.
        // NB: Cannot estimate MaxBlockSize to fill stat field here.
        stat.ChunkCount = std::max(
            static_cast<i64>(1),
            std::ssize(FreeStripes_) / GetJobCounter()->GetPending());
        stat.DataWeight = std::max(
            static_cast<i64>(1),
            GetDataWeightCounter()->GetPending() / GetJobCounter()->GetPending());
        stat.RowCount = std::max(
            static_cast<i64>(1),
            GetDataWeightCounter()->GetTotal() / GetJobCounter()->GetTotal());
        stat.MaxBlockSize = MaxBlockSize_;

        TChunkStripeStatisticsVector result;
        result.push_back(stat);
        return result;
    }

    i64 GetLocality(TNodeId nodeId) const override
    {
        auto it = NodeIdToEntry_.find(nodeId);
        return it == NodeIdToEntry_.end() ? 0 : it->second.Locality;
    }

    IChunkPoolOutput::TCookie Extract(TNodeId nodeId) override
    {
        auto cookie = IChunkPoolOutput::NullCookie;
        const auto& jobCounter = GetJobCounter();
        while (jobCounter->GetPending() + jobCounter->GetBlocked() > 0 && cookie == IChunkPoolOutput::NullCookie) {
            // There are no jobs in job manager, so we materialize a new one.
            const auto& jobManagerJobCounter = JobManager_->JobCounter();
            if (jobManagerJobCounter->GetPending() == 0) {
                YT_VERIFY(!FreeStripes_.empty());

                i64 idealDataWeightPerJob = UseNewSlicingImplementation_ ? GetAdjustedDataWeightPerJob() : GetDataWeightPerJobFromJobCounter();
                i64 idealCompressedDataSizePerJob = UseNewSlicingImplementation_ ? GetAdjustedCompressedDataSizePerJob() : std::numeric_limits<i64>::max() / 4;

                auto jobStub = std::make_unique<TNewJobStub>();

                // Take local chunks first.
                std::optional<int> stripeIndexesSize;
                if (nodeId != InvalidNodeId) {
                    auto it = NodeIdToEntry_.find(nodeId);
                    if (it != NodeIdToEntry_.end()) {
                        const auto& entry = it->second;
                        stripeIndexesSize = std::ssize(entry.StripeIndexes);
                        AddStripesToJob(
                            jobStub.get(),
                            entry.StripeIndexes.begin(),
                            entry.StripeIndexes.end(),
                            idealDataWeightPerJob,
                            idealCompressedDataSizePerJob);
                    }
                }

                // Take non-local chunks.
                int freeStripesSize = std::ssize(FreeStripes_);
                AddStripesToJob(
                    jobStub.get(),
                    FreeStripes_.begin(),
                    FreeStripes_.end(),
                    idealDataWeightPerJob,
                    idealCompressedDataSizePerJob);

                jobStub->Finalize();

                if (jobStub->GetStripeList()->TotalChunkCount == 1) {
                    YT_VERIFY(std::ssize(jobStub->GetStripeList()->Stripes) == 1);
                    auto dataSlice = jobStub->GetStripeList()->Stripes.back()->DataSlices.back();
                    if (SingleChunkTeleportStrategy_ == ESingleChunkTeleportStrategy::Enabled &&
                        TryTeleportChunk(dataSlice))
                    {
                        YT_LOG_DEBUG("Teleported single chunk (ChunkId: %v, NodeId: %v, IdealDataWeightPerJob: %v, TableIndex: %v, "
                                "ChunkCount: %v, DataWeight: %v, RowCount: %v, ValueCount: %v, MaxBlockSize: %v, RangeIndex: %v, IsTrivial: %v, "
                                "IsTeleportable: %v, IsLegacy: %v, HasLimits: %v, LegacyLowerLimit: %v, LegacyUpperLimit: %v, "
                                "LowerLimit: %v, UpperLimit: %v, StripeIndexesSize: %v, FreeStripesSize: %v, Pending: %v, Blocked: %v)",
                            dataSlice->GetSingleUnversionedChunk()->GetChunkId(),
                            nodeId,
                            idealDataWeightPerJob,
                            dataSlice->GetTableIndex(),
                            dataSlice->GetChunkCount(),
                            dataSlice->GetDataWeight(),
                            dataSlice->GetRowCount(),
                            dataSlice->GetValueCount(),
                            dataSlice->GetMaxBlockSize(),
                            dataSlice->GetRangeIndex(),
                            dataSlice->IsTrivial(),
                            dataSlice->IsTeleportable,
                            dataSlice->IsLegacy,
                            dataSlice->HasLimits(),
                            dataSlice->LegacyLowerLimit(),
                            dataSlice->LegacyUpperLimit(),
                            dataSlice->LowerLimit(),
                            dataSlice->UpperLimit(),
                            stripeIndexesSize,
                            freeStripesSize,
                            jobCounter->GetPending(),
                            jobCounter->GetBlocked());

                        // We have teleported single chunk so there is no need for a job.
                        UpdateFreeJobCounter();
                        CheckCompleted();
                        continue;
                    }
                }

                JobManager_->AddJob(std::move(jobStub));

                if (JobSizeConstraints_->IsExplicitJobCount()) {
                    FreeJobCounter_->AddPending(-1);
                    YT_VERIFY(FreeJobCounter_->GetPending() >= 0);
                }
            }

            YT_VERIFY(jobManagerJobCounter->GetPending() > 0);
            cookie = JobManager_->ExtractCookie();

            UpdateFreeJobCounter();
            CheckCompleted();
        }

        return cookie;
    }

    TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return JobManager_->GetStripeList(cookie);
    }

    int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override
    {
        return JobManager_->GetStripeList(cookie)->TotalChunkCount;
    }

    void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        TJobSplittingBase::Completed(cookie, jobSummary);

        if (jobSummary.InterruptionReason != EInterruptionReason::None) {
            YT_LOG_DEBUG(
                "Splitting job (OutputCookie: %v, InterruptionReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptionReason,
                jobSummary.SplitJobCount);
            auto childCookies = SplitJob(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount);
            RegisterChildCookies(cookie, std::move(childCookies));
        }

        //! If we don't have enough pending jobs - don't adjust data size per job.
        if (JobSizeAdjuster_ && JobCounter->GetPending() > JobCounter->GetRunning()) {
            JobSizeAdjuster_->UpdateStatistics(jobSummary);
            UpdateFreeJobCounter();
        }

        JobManager_->Completed(cookie, jobSummary.InterruptionReason);
        CheckCompleted();
    }

    std::vector<IChunkPoolOutput::TCookie> SplitJob(
        const std::vector<TLegacyDataSlicePtr>& dataSlices,
        int jobCount)
    {
        i64 unreadRowCount = GetCumulativeRowCount(dataSlices);
        i64 rowsPerJob = DivCeil<i64>(unreadRowCount, jobCount);
        i64 rowsToAdd = rowsPerJob;
        int sliceIndex = 0;
        auto currentDataSlice = dataSlices[0];
        TChunkStripePtr stripe = New<TChunkStripe>(false /*foreign*/);
        std::vector<IChunkPoolOutput::TCookie> childCookies;
        auto flushStripe = [&] {
            auto outputCookie = AddStripe(std::move(stripe), /*solid*/ true);
            if (outputCookie != IChunkPoolOutput::NullCookie) {
                childCookies.push_back(outputCookie);
            }
            stripe = New<TChunkStripe>(false /*foreign*/);
        };
        while (true) {
            i64 sliceRowCount = currentDataSlice->GetRowCount();
            if (currentDataSlice->Type == EDataSourceType::UnversionedTable && sliceRowCount > rowsToAdd) {
                auto split = currentDataSlice->SplitByRowIndex(rowsToAdd);
                stripe->DataSlices.emplace_back(std::move(split.first));
                rowsToAdd = 0;
                currentDataSlice = std::move(split.second);
            } else {
                stripe->DataSlices.emplace_back(std::move(currentDataSlice));
                rowsToAdd -= sliceRowCount;
                ++sliceIndex;
                if (sliceIndex == std::ssize(dataSlices)) {
                    break;
                }
                currentDataSlice = dataSlices[sliceIndex];
            }
            if (rowsToAdd <= 0) {
                flushStripe();
                rowsToAdd = rowsPerJob;
            }
        }
        if (!stripe->DataSlices.empty()) {
            flushStripe();
        }
        return childCookies;
    }

    void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Failed(cookie);
    }

    void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        JobManager_->Aborted(cookie, reason);
    }

    void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& list = GetStripeList(cookie);

        // No need to respect locality for restarted jobs.
        list->LocalChunkCount = 0;
        list->LocalDataWeight = 0;

        JobManager_->Lost(cookie);
        CheckCompleted();
    }

private:
    //! A mapping between input cookies (that are returned and used by controllers) and internal smaller
    //! stripe cookies that are obtained by slicing the input stripes.
    std::vector<std::vector<int>> InputCookieToInternalCookies_;
    std::vector<TSuspendableStripe> Stripes_;
    // char is used instead of bool because std::vector<bool> is not currently persistable,
    // and I am too lazy to fix that.
    std::vector<char> InputCookieIsSuspended_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    //! Used both for stripe sampling and teleport chunk sampling.
    TBernoulliSampler Sampler_;
    std::unique_ptr<IJobSizeAdjuster> JobSizeAdjuster_;

    //! Indexes in #Stripes.
    THashSet<int> FreeStripes_;

    THashSet<int> ExtractedStripes_;

    i64 MaxBlockSize_ = 0;

    struct TLocalityEntry
    {
        TLocalityEntry() = default;

        //! The total locality associated with this node.
        i64 Locality = 0;

        //! Indexes in #Stripes.
        THashSet<int> StripeIndexes;

        PHOENIX_DECLARE_TYPE(TLocalityEntry, 0xe973566a);
    };

    THashMap<TNodeId, TLocalityEntry> NodeIdToEntry_;

    TIdGenerator OutputCookieGenerator_;

    i64 MinTeleportChunkSize_ = std::numeric_limits<i64>::max() / 4;
    i64 MinTeleportChunkDataWeight_ = std::numeric_limits<i64>::max() / 4;
    bool SliceErasureChunksByParts_ = false;

    TRowBufferPtr RowBuffer_;

    TInputStreamDirectory InputStreamDirectory_;

    TNewJobManagerPtr JobManager_;

    TProgressCounterPtr FreeJobCounter_;
    TProgressCounterPtr FreeDataWeightCounter_;
    TProgressCounterPtr FreeCompressedDataSizeCounter_;
    TProgressCounterPtr FreeRowCounter_;

    bool IsCompleted_ = false;

    ESingleChunkTeleportStrategy SingleChunkTeleportStrategy_ = ESingleChunkTeleportStrategy::Disabled;

    bool UseNewSlicingImplementation_ = false;

    //! Teleport (move to destination pool) trivial (complete), unversioned, teleportable chunk.
    bool TryTeleportChunk(const TLegacyDataSlicePtr& dataSlice)
    {
        if (!dataSlice->IsTrivial() ||
            dataSlice->HasLimits() ||
            dataSlice->Type == NChunkClient::EDataSourceType::VersionedTable ||
            !InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsTeleportable())
        {
            return false;
        }

        const auto& chunk = dataSlice->GetSingleUnversionedChunk();

        if (chunk->IsCompleteChunk()) {
            if (Sampler_.Sample()) {
                ChunkTeleported_.Fire(chunk, /*tag=*/std::any{});
            } else {
                // Drop this teleport chunk.
            }

            return true;
        }

        return false;
    }

    static bool IsTrivialLimit(const TInputSliceLimit& limit, i64 defaultRowIndex)
    {
        return limit.RowIndex.value_or(defaultRowIndex) == defaultRowIndex && (!limit.KeyBound || limit.KeyBound.IsUniversal());
    };

    // XXX(max42): looks like this comment became obsolete even
    // before I got into this company.
    //! Convert data slice into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk. The
    //! resulting stripes are of approximately equal size. The size
    //! per stripe is either |maxSliceDataSize| or |TotalEstimateInputDataSize / jobCount|,
    //! whichever is smaller. If the resulting list contains less than
    //! |jobCount| stripes then |jobCount| is decreased appropriately.
    void AddDataSlice(const TLegacyDataSlicePtr dataSlice, IChunkPoolInput::TCookie inputCookie)
    {
        dataSlice->Tag = inputCookie;

        if (dataSlice->Type == EDataSourceType::VersionedTable) {
            AddStripe(New<TChunkStripe>(std::move(dataSlice)), /*solid*/ false);
            return;
        }

        const auto& chunk = dataSlice->GetSingleUnversionedChunk();

        if (IsLargeEnoughChunkSize(chunk->GetCompressedDataSize(), MinTeleportChunkSize_) ||
            IsLargeEnoughChunkWeight(chunk->GetDataWeight(), MinTeleportChunkDataWeight_))
        {
            if (TryTeleportChunk(dataSlice)) {
                return;
            }
        }

        int oldSize = Stripes_.size();

        bool hasNontrivialLimits = !chunk->IsCompleteChunk();

        auto codecId = chunk->GetErasureCodec();
        if (hasNontrivialLimits || codecId == NErasure::ECodec::None || !SliceErasureChunksByParts_) {
            // TODO(max42): rewrite slicing, SliceEvenly is a weird approach.
            auto slices = dataSlice->ChunkSlices[0]->SliceEvenly(
                JobSizeConstraints_->GetInputSliceDataWeight(),
                JobSizeConstraints_->GetInputSliceRowCount(),
                RowBuffer_);

            for (auto& slice : slices) {
                TInputSliceLimit lowerLimit;
                if (!IsTrivialLimit(slice->LowerLimit(), 0)) {
                    lowerLimit = slice->LowerLimit();
                }
                TInputSliceLimit upperLimit;
                if (!IsTrivialLimit(slice->UpperLimit(), chunk->GetTotalRowCount())) {
                    upperLimit = slice->UpperLimit();
                }

                auto newDataSlice = New<TLegacyDataSlice>(
                    EDataSourceType::UnversionedTable,
                    TLegacyDataSlice::TChunkSliceList{std::move(slice)},
                    lowerLimit,
                    upperLimit);
                newDataSlice->CopyPayloadFrom(*dataSlice);
                // XXX
                newDataSlice->GetInputStreamIndex();
                AddStripe(New<TChunkStripe>(std::move(newDataSlice)), /*solid*/ false);
            }
        } else {
            for (const auto& slice : CreateErasureInputChunkSlices(chunk, codecId)) {
                slice->TransformToNewKeyless();

                auto smallerSlices = slice->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount(),
                    RowBuffer_);

                for (auto& smallerSlice : smallerSlices) {
                    YT_VERIFY(!smallerSlice->IsLegacy);

                    TInputSliceLimit lowerLimit;
                    if (!IsTrivialLimit(smallerSlice->LowerLimit(), 0)) {
                        lowerLimit = smallerSlice->LowerLimit();
                    }
                    TInputSliceLimit upperLimit;
                    if (!IsTrivialLimit(smallerSlice->UpperLimit(), chunk->GetTotalRowCount())) {
                        upperLimit = smallerSlice->UpperLimit();
                    }

                    auto newDataSlice = New<TLegacyDataSlice>(
                        EDataSourceType::UnversionedTable,
                        TLegacyDataSlice::TChunkSliceList{std::move(smallerSlice)},
                        lowerLimit,
                        upperLimit);

                    newDataSlice->CopyPayloadFrom(*dataSlice);
                    // XXX
                    newDataSlice->GetInputStreamIndex();
                    AddStripe(New<TChunkStripe>(std::move(newDataSlice)), /*solid*/ false);
                }
            }
        }

        YT_LOG_TRACE("Slicing unversioned chunk (ChunkId: %v, DataWeight: %v, SliceDataWeight: %v, SliceRowCount: %v, "
            "SliceCount: %v)",
            chunk->GetChunkId(),
            chunk->GetDataWeight(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            Stripes_.size() - oldSize);
    }

    IChunkPoolOutput::TCookie AddStripe(TChunkStripePtr stripe, bool solid)
    {
        if (!solid && !Sampler_.Sample()) {
            return IChunkPoolOutput::NullCookie;
        }

        int internalCookie = Stripes_.size();

        Stripes_.push_back(TSuspendableStripe(std::move(stripe)));

        MaxBlockSize_ = std::max(MaxBlockSize_, Stripes_.back().GetStatistics().MaxBlockSize);

        GetDataSliceCounter()->AddUncategorized(Stripes_.back().GetStripe()->DataSlices.size());

        if (solid) {
            AddSolid(internalCookie);
        } else {
            Register(internalCookie);
        }

        for (const auto& dataSlice : Stripes_.back().GetStripe()->DataSlices) {
            YT_VERIFY(dataSlice->Tag);
            auto inputCookie = *dataSlice->Tag;

            InputCookieToInternalCookies_[inputCookie].push_back(internalCookie);
            if (InputCookieIsSuspended_[inputCookie]) {
                DoSuspend(internalCookie);
            }
        }

        return internalCookie;
    }

    i64 GetAdjustedDataWeightPerJob() const
    {
        return std::clamp<i64>(
            JobSizeAdjuster_ ? JobSizeAdjuster_->GetDataWeightPerJob() : JobSizeConstraints_->GetDataWeightPerJob(),
            1,
            JobSizeConstraints_->GetMaxDataWeightPerJob());
    }

    i64 GetAdjustedCompressedDataSizePerJob() const
    {
        return std::clamp<i64>(
            JobSizeConstraints_->GetCompressedDataSizePerJob(),
            1,
            JobSizeConstraints_->GetMaxCompressedDataSizePerJob());
    }

    i64 GetDataWeightPerJobFromJobCounter() const
    {
        i64 freePendingJobCount = FreeJobCounter_->GetTotal();
        YT_VERIFY(freePendingJobCount > 0);
        return std::max(
            static_cast<i64>(1),
            DivCeil<i64>(FreeDataWeightCounter_->GetTotal(), freePendingJobCount));
    }

    void UpdateFreeJobCounter()
    {
        i64 blockedJobCount = !Finished && !JobSizeConstraints_->IsExplicitJobCount() ? 1 : 0;

        auto computePendingJobCount = [&] (i64 sizeLeft, i64 sizePerJob) {
            if (JobSizeConstraints_->IsExplicitJobCount()) {
                return FreeJobCounter_->GetTotal();
            }

            i64 pendingJobCount = Finished ? DivCeil<i64>(sizeLeft, sizePerJob) : sizeLeft / sizePerJob;

            return std::max<i64>(
                pendingJobCount,
                std::ssize(FreeStripes_) / JobSizeConstraints_->GetMaxDataSlicesPerJob() - blockedJobCount);
        };

        i64 pendingJobCountByDataWeight = computePendingJobCount(FreeDataWeightCounter_->GetTotal(), GetAdjustedDataWeightPerJob());
        i64 pendingJobCountByCompressedDataSize = FreeCompressedDataSizeCounter_
            ? computePendingJobCount(FreeCompressedDataSizeCounter_->GetTotal(), GetAdjustedCompressedDataSizePerJob())
            : 0;

        i64 pendingJobCount = std::max(pendingJobCountByDataWeight, pendingJobCountByCompressedDataSize);

        if (Finished && FreeDataWeightCounter_->GetTotal() == 0) {
            pendingJobCount = 0;
            blockedJobCount = 0;
        }

        if (JobSizeConstraints_->IsExplicitJobCount() &&
            pendingJobCount + blockedJobCount == 0 &&
            FreeDataWeightCounter_->GetTotal() > 0)
        {
            // This abnormal case occurs when actual input data weight exceeds initial estimates,
            // typically due to unanticipated input chunks or chunks with higher weight than predicted.
            //
            // Under normal conditions (all below hold):
            // - Accurate data weight estimates
            // - No Max..PerJob violations
            // - Explicit job count >= row count
            // the algorithm preserves explicit job count via two guarantees:
            //
            // 1. Sliced job count cannot exceed the explicit job count. This is enforced by calculating
            //    dataWeightPerJob = ceil(TotalInputDataWeight / explicitJobCount), ensuring each job's
            //    data weight is >= dataWeightPerJob. Thus, ceil(actualTotalInputDataWeight / dataWeightPerJob) <= explicitJobCount
            //    when estimates are accurate.
            //
            // 2. Sliced jobs cannot be fewer than the explicit job count. The algorithm prioritizes this by finalizing
            //    a stripe early if remaining chunks are insufficient to produce the required number of jobs.
            YT_LOG_ALERT("Explicit job count guarantee cannot be satisfied; scheduling an extra job");
            ++pendingJobCount;
        }

        bool canScheduleJobs = !FreeStripes_.empty();

        // NB(gritukan): YT-14498, if job count is explicit,
        // last job cannot be scheduled unless all the data is available.
        if (JobSizeConstraints_->IsExplicitJobCount() &&
            pendingJobCount == 1 &&
            FreeDataWeightCounter_->GetSuspended() > 0)
        {
            canScheduleJobs = false;
        }

        if (canScheduleJobs) {
            FreeJobCounter_->SetPending(pendingJobCount);
            FreeJobCounter_->SetSuspended(0);
            FreeJobCounter_->SetBlocked(blockedJobCount);
        } else {
            FreeJobCounter_->SetPending(0);
            FreeJobCounter_->SetSuspended(pendingJobCount + blockedJobCount);
            FreeJobCounter_->SetBlocked(0);
        }
    }

    void Register(int stripeIndex)
    {
        const auto& suspendableStripe = Stripes_[stripeIndex];

        const auto& stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicas()) {
                    auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        auto& entry = NodeIdToEntry_[replica.GetNodeId()];
                        // NB: Do not check that stripe is unique, it may have already been inserted,
                        // since different replicas may reside on the same node during rebalancing.
                        entry.StripeIndexes.insert(stripeIndex);
                        entry.Locality += locality;
                    }
                }
            }
        }

        const auto& statistics = suspendableStripe.GetStatistics();
        FreeDataWeightCounter_->AddPending(statistics.DataWeight);
        if (FreeCompressedDataSizeCounter_) {
            // NB(apollo1321): statistics.CompressedDataSize may be zero for dynamic tables.
            FreeCompressedDataSizeCounter_->AddPending(statistics.CompressedDataSize);
        }
        FreeRowCounter_->AddPending(statistics.RowCount);

        YT_VERIFY(FreeStripes_.insert(stripeIndex).second);
    }

    void AddSolid(int stripeIndex)
    {
        const auto& suspendableStripe = Stripes_[stripeIndex];
        YT_VERIFY(!FreeStripes_.contains(stripeIndex));
        YT_VERIFY(ExtractedStripes_.insert(stripeIndex).second);

        auto jobStub = std::make_unique<TNewJobStub>();
        for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
            jobStub->AddDataSlice(dataSlice, stripeIndex, /*primary*/ true);
        }
        jobStub->Finalize();

        JobManager_->AddJob(std::move(jobStub));
    }

    void Unregister(int stripeIndex)
    {
        const auto& suspendableStripe = Stripes_[stripeIndex];

        const auto& stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicas()) {
                    i64 locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        auto& entry = NodeIdToEntry_[replica.GetNodeId()];
                        auto it = entry.StripeIndexes.find(stripeIndex);
                        if (it != entry.StripeIndexes.end()) {
                            entry.StripeIndexes.erase(it);
                        }
                        entry.Locality -= locality;
                    }
                }
            }
        }

        const auto& statistics = suspendableStripe.GetStatistics();
        FreeDataWeightCounter_->AddPending(-statistics.DataWeight);
        YT_VERIFY(FreeDataWeightCounter_->GetPending() >= 0);
        if (FreeCompressedDataSizeCounter_) {
            FreeCompressedDataSizeCounter_->AddPending(-statistics.CompressedDataSize);
            YT_VERIFY(FreeCompressedDataSizeCounter_->GetPending() >= 0);
        }
        FreeRowCounter_->AddPending(-statistics.RowCount);
        YT_VERIFY(FreeRowCounter_->GetPending() >= 0);

        YT_VERIFY(FreeStripes_.erase(stripeIndex) == 1);
    }

    void AddStripesToJob(
        TNewJobStub* jobStub,
        const THashSet<int>::const_iterator& begin,
        const THashSet<int>::const_iterator& end,
        i64 idealDataWeightPerJob,
        i64 idealCompressedDataSizePerJob)
    {
        const auto& jobCounter = GetJobCounter();
        i64 pendingStripesCount = std::ssize(FreeStripes_);
        std::vector<int> addedStripeIndexes;
        for (auto it = begin; it != end; ++it) {
            bool hasSufficientDataWeight = jobStub->GetDataWeight() >= idealDataWeightPerJob;
            bool hasSufficientCompressedDataSize = jobStub->GetCompressedDataSize() >= idealCompressedDataSizePerJob;

            if (JobSizeConstraints_->IsExplicitJobCount()) {
                if (hasSufficientDataWeight) {
                    if (FreeJobCounter_->GetPending() > 1) {
                        break;
                    } else {
                        // TODO(apollo1321): Change to alert later.
                        // This may happen if some input chunks were suspended.
                        YT_LOG_WARNING_IF(
                            pendingStripesCount > 1,
                            "Last job will be bigger than expected (PendingStripesCount: %v, JobDataWeight: %v)",
                            pendingStripesCount,
                            jobStub->GetDataWeight());
                    }

                }
            } else if (hasSufficientDataWeight || hasSufficientCompressedDataSize) {
                break;
            }

            // NB: We should ignore check of chunk stripe count in case of last job.
            if (jobStub->GetSliceCount() >= JobSizeConstraints_->GetMaxDataSlicesPerJob() &&
                (!JobSizeConstraints_->IsExplicitJobCount() || jobCounter->GetPending() > 1))
            {
                break;
            }

            int stripeIndex = *it;
            const auto& suspendableStripe = Stripes_[stripeIndex];
            const auto& stat = suspendableStripe.GetStatistics();

            // We should always return at least one stripe, even if we get Max...PerJob overflow.
            if (jobStub->GetDataWeight() > 0 &&
                (!JobSizeConstraints_->IsExplicitJobCount() || jobCounter->GetPending() > 1))
            {
                i64 nextDataWeight = jobStub->GetDataWeight() + stat.DataWeight;
                i64 nextCompressedDataSize = jobStub->GetCompressedDataSize() + stat.CompressedDataSize;
                if (nextDataWeight > JobSizeConstraints_->GetMaxDataWeightPerJob() ||
                    nextCompressedDataSize > JobSizeConstraints_->GetMaxCompressedDataSizePerJob())
                {
                    break;
                }
            }

            // Leave enough stripes if job count is explicitly given.
            if (jobStub->GetDataWeight() > 0 && pendingStripesCount < jobCounter->GetPending() && JobSizeConstraints_->IsExplicitJobCount()) {
                break;
            }

            --pendingStripesCount;
            addedStripeIndexes.push_back(stripeIndex);

            for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
                jobStub->AddDataSlice(dataSlice, stripeIndex, /*primary*/ true);
            }
        }

        for (int stripeIndex : addedStripeIndexes) {
            Unregister(stripeIndex);
            YT_VERIFY(ExtractedStripes_.insert(stripeIndex).second);
        }
    }

    void CheckCompleted()
    {
        bool completed =
            Finished &&
            FreeDataWeightCounter_->GetTotal() == 0 &&
            JobCounter->GetRunning() == 0 &&
            JobCounter->GetSuspended() == 0 &&
            JobCounter->GetPending() == 0 &&
            JobCounter->GetBlocked() == 0;

        if (!IsCompleted_ && completed) {
            Completed_.Fire();
        } else if (IsCompleted_ && !completed) {
            Uncompleted_.Fire();
        }

        IsCompleted_ = completed;
    }

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUnorderedChunkPool, 0xbacd26ad);
};

void TUnorderedChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolInputBase>();
    registrar.template BaseType<TChunkPoolOutputWithCountersBase>();
    registrar.template BaseType<TLoggerOwner>();

    PHOENIX_REGISTER_FIELD(1, InputCookieToInternalCookies_);
    PHOENIX_REGISTER_FIELD(2, Stripes_);
    PHOENIX_REGISTER_FIELD(3, InputCookieIsSuspended_);
    PHOENIX_REGISTER_FIELD(4, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(5, Sampler_);
    PHOENIX_REGISTER_FIELD(6, JobSizeAdjuster_);
    PHOENIX_REGISTER_FIELD(7, FreeStripes_);
    PHOENIX_REGISTER_FIELD(8, ExtractedStripes_);
    PHOENIX_REGISTER_FIELD(9, MaxBlockSize_);
    PHOENIX_REGISTER_FIELD(10, NodeIdToEntry_);
    PHOENIX_REGISTER_FIELD(11, OutputCookieGenerator_);
    // COMPAT(apollo1321): Remove in 25.2.
    registrar
        .template VirtualField<12>("Mode_", [] (TThis* /*this_*/, auto& context) {
            Load<EUnorderedChunkPoolMode>(context);
        })
        .BeforeVersion(ESnapshotVersion::NewUnorderedChunkPoolSlicing)();
    PHOENIX_REGISTER_FIELD(13, MinTeleportChunkSize_);
    PHOENIX_REGISTER_FIELD(14, MinTeleportChunkDataWeight_);
    PHOENIX_REGISTER_FIELD(15, SliceErasureChunksByParts_);
    PHOENIX_REGISTER_FIELD(16, InputStreamDirectory_);
    PHOENIX_REGISTER_FIELD(17, JobManager_);
    PHOENIX_REGISTER_FIELD(18, FreeJobCounter_);
    PHOENIX_REGISTER_FIELD(19, FreeDataWeightCounter_);
    PHOENIX_REGISTER_FIELD(20, FreeRowCounter_);
    PHOENIX_REGISTER_FIELD(21, IsCompleted_);

    PHOENIX_REGISTER_FIELD(22, SingleChunkTeleportStrategy_,
        .SinceVersion(ESnapshotVersion::SingleChunkTeleportStrategy));

    PHOENIX_REGISTER_FIELD(23, UseNewSlicingImplementation_,
        .SinceVersion(ESnapshotVersion::NewUnorderedChunkPoolSlicing));
    PHOENIX_REGISTER_FIELD(24, FreeCompressedDataSizeCounter_,
        // COMPAT(apollo1321): Make FreeCompressedDataSizeCounter_ non-null in 25.2.
        .SinceVersion(ESnapshotVersion::CompressedDataSizePerJob));

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        ValidateLogger(this_->Logger);
    });
}

PHOENIX_DEFINE_TYPE(TUnorderedChunkPool);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedChunkPool::TLocalityEntry::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Locality);
    PHOENIX_REGISTER_FIELD(2, StripeIndexes);
}

PHOENIX_DEFINE_TYPE(TUnorderedChunkPool::TLocalityEntry);

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolPtr CreateUnorderedChunkPool(
    const TUnorderedChunkPoolOptions& options,
    TInputStreamDirectory directory)
{
    return New<TUnorderedChunkPool>(
        options,
        std::move(directory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
