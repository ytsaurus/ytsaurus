#include "unordered_chunk_pool.h"

#include "helpers.h"
#include "job_size_adjuster.h"
#include "new_job_manager.h"
#include "config.h"

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/ytlib/chunk_client/input_chunk.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/library/random/bernoulli_sampler.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

#include <random>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithCountersBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! For persistence only.
    TUnorderedChunkPool()
        : MaxBlockSize_(-1)
    { }

    TUnorderedChunkPool(
        const TUnorderedChunkPoolOptions& options,
        TInputStreamDirectory directory)
        : OperationId_(options.OperationId)
        , Name_(options.Name)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , Sampler_(JobSizeConstraints_->GetSamplingRate())
        , Mode_(options.Mode)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , MinTeleportChunkDataWeight_(options.MinTeleportChunkDataWeight)
        , SliceErasureChunksByParts_(options.SliceErasureChunksByParts)
        , RowBuffer_(options.RowBuffer)
        , InputStreamDirectory_(std::move(directory))
        , JobManager_(New<TNewJobManager>())
        , FreeJobCounter_(New<TProgressCounter>())
        , FreeDataWeightCounter_(New<TProgressCounter>())
        , FreeRowCounter_(New<TProgressCounter>())
    {
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        Logger.AddTag("Name: %v", Name_);

        // TODO(max42): why do we need row buffer in unordered pool at all?
        YT_VERIFY(RowBuffer_);

        JobManager_->SetLogger(TLogger{Logger});

        FreeJobCounter_->AddParent(JobCounter);
        FreeDataWeightCounter_->AddParent(DataWeightCounter);
        FreeRowCounter_->AddParent(RowCounter);

        JobManager_->JobCounter()->AddParent(JobCounter);
        JobManager_->DataWeightCounter()->AddParent(DataWeightCounter);
        JobManager_->RowCounter()->AddParent(RowCounter);

        if (Mode_ == EUnorderedChunkPoolMode::Normal) {
            FreeJobCounter_->AddPending(JobSizeConstraints_->GetJobCount());
        }

        if (options.JobSizeAdjusterConfig && JobSizeConstraints_->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster_ = CreateJobSizeAdjuster(
                JobSizeConstraints_->GetDataWeightPerJob(),
                options.JobSizeAdjusterConfig);
            // ToDo(psushin): add logging here.
            // ToDo(max42): Hi psushin, which logging do you want here?
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

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(!Finished);

        auto cookie = InputCookieToInternalCookies_.size();
        InputCookieToInternalCookies_.emplace_back();
        InputCookieIsSuspended_.emplace_back(false);

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);
            AddDataSlice(dataSlice, cookie);
        }

        UpdateFreeJobCounter();
        CheckCompleted();

        return cookie;
    }

    virtual void Finish() override
    {
        if (!Finished) {
            TChunkPoolInputBase::Finish();
        }

        UpdateFreeJobCounter();
        CheckCompleted();
    }

    virtual void Suspend(IChunkPoolInput::TCookie inputCookie) override
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
            FreeRowCounter_->AddSuspended(statistics.RowCount);
        }

        JobManager_->Suspend(cookie);
    }

    virtual void Resume(IChunkPoolInput::TCookie inputCookie) override
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
            FreeRowCounter_->AddSuspended(-statistics.RowCount);
            YT_VERIFY(FreeRowCounter_->GetSuspended() >= 0);
        }

        JobManager_->Resume(cookie);
    }

    // IChunkPoolOutput implementation.

    virtual bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
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
            static_cast<i64>(FreeStripes_.size()) / GetJobCounter()->GetPending());
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

    virtual i64 GetLocality(TNodeId nodeId) const override
    {
        auto it = NodeIdToEntry_.find(nodeId);
        return it == NodeIdToEntry_.end() ? 0 : it->second.Locality;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId nodeId) override
    {
        const auto& jobCounter = GetJobCounter();
        if (jobCounter->GetPending() == 0) {
            return IChunkPoolOutput::NullCookie;
        }

        // There are no jobs in job manager, so we materialize a new one.
        const auto& jobManagerJobCounter = JobManager_->JobCounter();
        if (jobManagerJobCounter->GetPending() == 0) {
            auto idealDataWeightPerJob = GetIdealDataWeightPerJob();

            auto jobStub = std::make_unique<TNewJobStub>();
            // Take local chunks first.
            if (nodeId != InvalidNodeId) {
                auto it = NodeIdToEntry_.find(nodeId);
                if (it != NodeIdToEntry_.end()) {
                    const auto& entry = it->second;
                    AddStripesToJob(
                        jobStub.get(),
                        entry.StripeIndexes.begin(),
                        entry.StripeIndexes.end(),
                        nodeId,
                        idealDataWeightPerJob);
                }
            }

            // Take non-local chunks.
            AddStripesToJob(
                jobStub.get(),
                FreeStripes_.begin(),
                FreeStripes_.end(),
                nodeId,
                idealDataWeightPerJob);

            jobStub->Finalize(/* sortByPosition */ false);
            JobManager_->AddJob(std::move(jobStub));

            if (Mode_ == EUnorderedChunkPoolMode::Normal) {
                FreeJobCounter_->AddPending(-1);
                YT_VERIFY(FreeJobCounter_->GetPending() >= 0);
            }
        }

        YT_VERIFY(jobManagerJobCounter->GetPending() > 0);
        auto cookie = JobManager_->ExtractCookie();

        UpdateFreeJobCounter();
        CheckCompleted();

        return cookie;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return JobManager_->GetStripeList(cookie);
    }

    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override
    {
        return JobManager_->GetStripeList(cookie)->TotalChunkCount;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        if (jobSummary.InterruptReason != EInterruptReason::None) {
            YT_LOG_DEBUG("Splitting job (OutputCookie: %v, InterruptReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptReason,
                jobSummary.SplitJobCount);
            SplitJob(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount);
        }

        //! If we don't have enough pending jobs - don't adjust data size per job.
        if (JobSizeAdjuster_ && JobCounter->GetPending() > JobCounter->GetRunning()) {
            JobSizeAdjuster_->UpdateStatistics(jobSummary);
            UpdateFreeJobCounter();
        }

        JobManager_->Completed(cookie, jobSummary.InterruptReason);
        CheckCompleted();
    }

    void SplitJob(const std::vector<NChunkClient::TLegacyDataSlicePtr>& dataSlices, int jobCount)
    {
        i64 unreadRowCount = GetCumulativeRowCount(dataSlices);
        i64 rowsPerJob = DivCeil<i64>(unreadRowCount, jobCount);
        i64 rowsToAdd = rowsPerJob;
        int sliceIndex = 0;
        auto currentDataSlice = dataSlices[0];
        TChunkStripePtr stripe = New<TChunkStripe>(false /* foreign */, true /* solid */);
        auto flushStripe = [&] {
            AddStripe(std::move(stripe));
            stripe = New<TChunkStripe>(false /* foreign */, true /* solid */);
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
                if (sliceIndex == static_cast<int>(dataSlices.size())) {
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
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Failed(cookie);
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        JobManager_->Aborted(cookie, reason);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        const auto& list = GetStripeList(cookie);

        // No need to respect locality for restarted jobs.
        list->LocalChunkCount = 0;
        list->LocalDataWeight = 0;

        JobManager_->Lost(cookie);
        CheckCompleted();
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputWithCountersBase::Persist(context);

        using NYT::Persist;
        Persist(context, ChunkPoolId_);
        Persist(context, OperationId_);
        Persist(context, Name_);
        Persist(context, InputCookieToInternalCookies_);
        Persist(context, Stripes_);
        Persist(context, InputCookieIsSuspended_);
        Persist(context, JobSizeConstraints_);
        Persist(context, Sampler_);
        Persist(context, JobSizeAdjuster_);
        Persist(context, FreeStripes_);
        Persist(context, ExtractedStripes_);
        Persist(context, MaxBlockSize_);
        Persist(context, NodeIdToEntry_);
        Persist(context, OutputCookieGenerator_);
        Persist(context, Mode_);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, MinTeleportChunkDataWeight_);
        Persist(context, SliceErasureChunksByParts_);
        Persist(context, InputStreamDirectory_);
        Persist(context, JobManager_);
        Persist(context, FreeJobCounter_);
        Persist(context, FreeDataWeightCounter_);
        Persist(context, FreeRowCounter_);
        Persist(context, IsCompleted_);

        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            Logger.AddTag("Name: %v", Name_);
            JobManager_->SetLogger(TLogger{Logger});
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool, 0xbacd26ad);

    TLogger Logger = ChunkPoolLogger;
    TGuid ChunkPoolId_ = TGuid::Create();
    TOperationId OperationId_;
    TString Name_;

    //! A mappping between input cookies (that are returned and used by controllers) and internal smaller
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

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Locality);
            Persist(context, StripeIndexes);
        }
    };

    THashMap<TNodeId, TLocalityEntry> NodeIdToEntry_;

    TIdGenerator OutputCookieGenerator_;

    EUnorderedChunkPoolMode Mode_;
    i64 MinTeleportChunkSize_ = std::numeric_limits<i64>::max() / 4;
    i64 MinTeleportChunkDataWeight_ = std::numeric_limits<i64>::max() / 4;
    bool SliceErasureChunksByParts_ = false;

    TRowBufferPtr RowBuffer_;

    TInputStreamDirectory InputStreamDirectory_;

    TNewJobManagerPtr JobManager_;

    TProgressCounterPtr FreeJobCounter_;
    TProgressCounterPtr FreeDataWeightCounter_;
    TProgressCounterPtr FreeRowCounter_;

    bool IsCompleted_ = false;

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
            AddStripe(New<TChunkStripe>(dataSlice));
        } else {
            const auto& chunk = dataSlice->GetSingleUnversionedChunkOrThrow();

            if (chunk->IsCompleteChunk() &&
                ((chunk->IsLargeCompleteChunk(MinTeleportChunkSize_) ||
                chunk->GetDataWeight() >= MinTeleportChunkDataWeight_)) &&
                InputStreamDirectory_.GetDescriptor(dataSlice->GetTableIndex()).IsTeleportable())
            {
                if (Sampler_.Sample()) {
                    ChunkTeleported_.Fire(chunk, /*tag=*/std::any{});
                } else {
                    // Drop this teleport chunk.
                }
                return;
            }

            int oldSize = Stripes_.size();

            bool hasNontrivialLimits = !chunk->IsCompleteChunk();

            auto codecId = NErasure::ECodec(chunk->GetErasureCodec());
            if (hasNontrivialLimits || codecId == NErasure::ECodec::None || !SliceErasureChunksByParts_) {
                // TODO(max42): rewrite slicing, SliceEvenly is a weird approach.
                auto slices = dataSlice->ChunkSlices[0]->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount(),
                    RowBuffer_);

                for (auto& slice : slices) {
                    auto newDataSlice = New<TLegacyDataSlice>(
                        EDataSourceType::UnversionedTable,
                        TLegacyDataSlice::TChunkSliceList{slice},
                        slice->LowerLimit(),
                        slice->UpperLimit());
                    newDataSlice->CopyPayloadFrom(*dataSlice);
                    AddStripe(New<TChunkStripe>(newDataSlice));
                }
            } else {
                for (const auto& slice : CreateErasureInputChunkSlices(chunk, codecId)) {
                    auto smallerSlices = slice->SliceEvenly(
                        JobSizeConstraints_->GetInputSliceDataWeight(),
                        JobSizeConstraints_->GetInputSliceRowCount(),
                        RowBuffer_);

                    for (auto& smallerSlice : smallerSlices) {
                        auto newDataSlice = New<TLegacyDataSlice>(
                            EDataSourceType::UnversionedTable,
                            TLegacyDataSlice::TChunkSliceList{std::move(smallerSlice)});
                        newDataSlice->TransformToNewKeyless();
                        newDataSlice->CopyPayloadFrom(*dataSlice);
                        AddStripe(New<TChunkStripe>(newDataSlice));
                    }
                }
            }

            YT_LOG_TRACE("Slicing unversioned chunk (ChunkId: %v, DataWeight: %v, SliceDataWeight: %v, SliceRowCount: %v, "
                "SliceCount: %v)",
                chunk->ChunkId(),
                chunk->GetDataWeight(),
                JobSizeConstraints_->GetInputSliceDataWeight(),
                JobSizeConstraints_->GetInputSliceRowCount(),
                Stripes_.size() - oldSize);
        }
    }

    void AddStripe(const TChunkStripePtr& stripe)
    {
        if (!stripe->Solid && !Sampler_.Sample()) {
            return;
        }

        int internalCookie = Stripes_.size();

        TSuspendableStripe suspendableStripe(stripe);

        Stripes_.push_back(suspendableStripe);

        MaxBlockSize_ = std::max(MaxBlockSize_, suspendableStripe.GetStatistics().MaxBlockSize);

        GetDataSliceCounter()->AddUncategorized(stripe->DataSlices.size());

        if (stripe->Solid) {
            AddSolid(internalCookie);
        } else {
            Register(internalCookie);
        }

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(dataSlice->Tag);
            auto inputCookie = *dataSlice->Tag;

            InputCookieToInternalCookies_[inputCookie].push_back(internalCookie);
            if (InputCookieIsSuspended_[inputCookie]) {
                DoSuspend(internalCookie);
            }
        }
    }

    int GetFreeJobCount() const
    {
        return FreeJobCounter_->GetPending() + FreeJobCounter_->GetSuspended();
    }

    i64 GetIdealDataWeightPerJob() const
    {
        if (Mode_ == EUnorderedChunkPoolMode::AutoMerge) {
            return JobSizeConstraints_->GetDataWeightPerJob();
        }
        int freePendingJobCount = GetFreeJobCount();
        YT_VERIFY(freePendingJobCount > 0);
        return std::max(
            static_cast<i64>(1),
            DivCeil<i64>(FreeDataWeightCounter_->GetTotal(), freePendingJobCount));
    }

    void UpdateFreeJobCounter()
    {
        // At first, reset both pending and suspended free job counters.
        auto oldFreeJobCount = FreeJobCounter_->GetPending() + FreeJobCounter_->GetSuspended();
        FreeJobCounter_->AddPending(-FreeJobCounter_->GetPending());
        FreeJobCounter_->AddSuspended(-FreeJobCounter_->GetSuspended());

        i64 newFreeJobCount = 0;
        i64 dataWeightLeft = FreeDataWeightCounter_->GetTotal();
        if (Mode_ == EUnorderedChunkPoolMode::AutoMerge) {
            // If we have pending or suspended stripes there are more jobs, otherwise operation in completed.
            if (dataWeightLeft > 0) {
                newFreeJobCount = 1;
            } else {
                newFreeJobCount = 0;
            }
        } else {
            if (JobSizeConstraints_->IsExplicitJobCount()) {
                newFreeJobCount = oldFreeJobCount;
            } else {
                i64 dataWeightPerJob = JobSizeAdjuster_
                    ? JobSizeAdjuster_->GetDataWeightPerJob()
                    : JobSizeConstraints_->GetDataWeightPerJob();
                dataWeightPerJob = std::min(dataWeightPerJob, JobSizeConstraints_->GetMaxDataWeightPerJob());
                dataWeightPerJob = std::max<i64>(dataWeightPerJob, 1);
                newFreeJobCount = DivCeil(dataWeightLeft, dataWeightPerJob);
            }

            if (newFreeJobCount == 0 && dataWeightLeft > 0) {
                newFreeJobCount = 1;
            }

            if (Finished && dataWeightLeft == 0) {
                newFreeJobCount = 0;
            }
        }

        if (FreeStripes_.empty()) {
            FreeJobCounter_->AddSuspended(newFreeJobCount);
        } else {
            FreeJobCounter_->AddPending(newFreeJobCount);
        }
    }

    void Register(int stripeIndex)
    {
        auto& suspendableStripe = Stripes_[stripeIndex];

        auto stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                    auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        auto& entry = NodeIdToEntry_[replica.GetNodeId()];
                        // NB: do not check that stripe is unique, it may have already been inserted,
                        // since different replicas may reside on the same node during rebalancing.
                        entry.StripeIndexes.insert(stripeIndex);
                        entry.Locality += locality;
                    }
                }
            }
        }

        const auto& statistics = suspendableStripe.GetStatistics();
        FreeDataWeightCounter_->AddPending(statistics.DataWeight);
        FreeRowCounter_->AddPending(statistics.RowCount);

        YT_VERIFY(FreeStripes_.insert(stripeIndex).second);
    }

    void AddSolid(int stripeIndex)
    {
        auto& suspendableStripe = Stripes_[stripeIndex];
        YT_VERIFY(!FreeStripes_.contains(stripeIndex));
        YT_VERIFY(ExtractedStripes_.insert(stripeIndex).second);
        YT_VERIFY(suspendableStripe.GetStripe()->Solid);

        auto jobStub = std::make_unique<TNewJobStub>();
        for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
            jobStub->AddDataSlice(dataSlice, stripeIndex, /*primary=*/true);
        }
        jobStub->Finalize(/*sortByPosition=*/false);

        JobManager_->AddJob(std::move(jobStub));
    }

    void Unregister(int stripeIndex)
    {
        auto& suspendableStripe = Stripes_[stripeIndex];

        auto stripe = suspendableStripe.GetStripe();
        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                    auto locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
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
        FreeRowCounter_->AddPending(-statistics.RowCount);
        YT_VERIFY(FreeRowCounter_->GetPending() >= 0);

        YT_VERIFY(FreeStripes_.erase(stripeIndex) == 1);
    }

    template <class TIterator>
    void AddStripesToJob(
        TNewJobStub* jobStub,
        const TIterator& begin,
        const TIterator& end,
        TNodeId nodeId,
        i64 idealDataWeightPerJob)
    {
        const auto& jobCounter = GetJobCounter();
        auto pendingStripesCount = FreeStripes_.size();
        std::vector<int> addedStripeIndexes;
        for (auto it = begin; it != end; ++it) {
            if (jobStub->GetDataWeight() >= idealDataWeightPerJob) {
                break;
            }

            // NB: We should ignore check of chunk stripe count in case of last job.
            if (jobStub->GetSliceCount() >= JobSizeConstraints_->GetMaxDataSlicesPerJob() &&
                (!JobSizeConstraints_->IsExplicitJobCount() || jobCounter->GetPending() > 1))
            {
                break;
            }

            auto stripeIndex = *it;
            auto& suspendableStripe = Stripes_[stripeIndex];
            auto stat = suspendableStripe.GetStatistics();

            // We should always return at least one stripe, even we get MaxDataWeightPerJob overflow.
            if (jobStub->GetDataWeight() > 0 && jobStub->GetDataWeight() + stat.DataWeight >
                JobSizeConstraints_->GetMaxDataWeightPerJob() &&
                (!JobSizeConstraints_->IsExplicitJobCount() || jobCounter->GetPending() > 1))
            {
                break;
            }

            // Leave enough stripes if job count is explicitly given.
            if (jobStub->GetDataWeight() > 0 && pendingStripesCount < jobCounter->GetPending() && JobSizeConstraints_->IsExplicitJobCount()) {
                break;
            }

            --pendingStripesCount;
            addedStripeIndexes.push_back(stripeIndex);

            for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
                jobStub->AddDataSlice(dataSlice, stripeIndex, /*primary=*/true);
            }
        }

        for (auto stripeIndex : addedStripeIndexes) {
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
            JobCounter->GetPending() == 0;

        if (!IsCompleted_ && completed) {
            Completed_.Fire();
        } else if (IsCompleted_ && !completed) {
            Uncompleted_.Fire();
        }

        IsCompleted_ = completed;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedChunkPool);

////////////////////////////////////////////////////////////////////////////////

void TUnorderedChunkPoolOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Mode);
    Persist(context, JobSizeAdjusterConfig);
    Persist(context, JobSizeConstraints);
    Persist(context, MinTeleportChunkSize);
    Persist(context, MinTeleportChunkDataWeight);
    Persist(context, SliceErasureChunksByParts);
    Persist(context, OperationId);
    Persist(context, Name);
}

////////////////////////////////////////////////////////////////////////////////

IChunkPoolPtr CreateUnorderedChunkPool(
    const TUnorderedChunkPoolOptions& options,
    TInputStreamDirectory directory)
{
    return New<TUnorderedChunkPool>(
        options,
        std::move(directory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
