#include "sorted_chunk_pool.h"

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>
#include <yt/ytlib/table_client/row_buffer.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////

void TSortedChunkPoolOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, EnableKeyGuarantee);
    Persist(context, PrimaryPrefixLength);
    Persist(context, ForeignPrefixLength);
    Persist(context, MinTeleportChunkSize);
    Persist(context, JobSizeConstraints);
    Persist(context, SupportLocality);
}

////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EEndpointType,
    (Left)
    (Right)
    (ForeignRight)
);

////////////////////////////////////////////////////////////////////

class TJobBuilder
{
public:
    DEFINE_BYREF_RO_PROPERTY(TKey, LowerPrimaryKey, MaxKey().Get());
    DEFINE_BYREF_RO_PROPERTY(TKey, UpperPrimaryKey, MinKey().Get());

    DEFINE_BYVAL_RO_PROPERTY(int, PrimarySliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, ForeignSliceCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(int, PreliminaryForeignSliceCount, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryDataSize, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignDataSize, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignDataSize, 0);

    DEFINE_BYVAL_RO_PROPERTY(i64, PrimaryRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, ForeignRowCount, 0);
    DEFINE_BYVAL_RO_PROPERTY(i64, PreliminaryForeignRowCount, 0);

    friend class TJobManager;

public:
    TJobBuilder() = default;

    void AddDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary)
    {
        if (dataSlice->IsEmpty()) {
            return;
        }

        int tableIndex = dataSlice->GetTableIndex();
        auto& stripe = GetStripe(tableIndex, isPrimary);
        stripe->DataSlices.emplace_back(dataSlice);
        InputCookies_.emplace_back(cookie);

        if (isPrimary) {
            if (LowerPrimaryKey_ > dataSlice->LowerLimit().Key) {
                LowerPrimaryKey_ = dataSlice->LowerLimit().Key;
            }
            if (UpperPrimaryKey_ < dataSlice->UpperLimit().Key) {
                UpperPrimaryKey_ = dataSlice->UpperLimit().Key;
            }
            PrimaryDataSize_ += dataSlice->GetDataSize();
            PrimaryRowCount_ += dataSlice->GetRowCount();
            ++PrimarySliceCount_;
        } else {
            ForeignDataSize_ += dataSlice->GetDataSize();
            ForeignRowCount_ += dataSlice->GetRowCount();
            ++ForeignSliceCount_;
        }
    }

    void AddPreliminaryForeignDataSlice(const TInputDataSlicePtr& dataSlice)
    {
        PreliminaryForeignDataSize_ += dataSlice->GetDataSize();
        PreliminaryForeignRowCount_ += dataSlice->GetRowCount();
        ++PreliminaryForeignSliceCount_;
    }

    //! Removes all empty stripes, sets `Foreign` = true for all foreign stripes
    //! and calculates the statistics for the stripe list.
    void Finalize()
    {
        int nonEmptyStripeCount = 0;
        for (int index = 0; index < StripeList_->Stripes.size(); ++index) {
            if (StripeList_->Stripes[index]) {
                auto& stripe = StripeList_->Stripes[nonEmptyStripeCount];
                stripe = std::move(StripeList_->Stripes[index]);
                ++nonEmptyStripeCount;
                const auto& statistics = stripe->GetStatistics();
                StripeList_->TotalDataSize += statistics.DataSize;
                StripeList_->TotalRowCount += statistics.RowCount;
                StripeList_->TotalChunkCount += statistics.ChunkCount;
                // This is done to ensure that all the data slices inside a stripe
                // are not only sorted by key, but additionally by their position
                // in the original table.
                std::sort(
                    stripe->DataSlices.begin(),
                    stripe->DataSlices.end(),
                    [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
                        auto cmpResult = CompareRows(lhs->LowerLimit().Key, rhs->LowerLimit().Key);
                        if (cmpResult != 0 || lhs->Type == EDataSourceType::VersionedTable) {
                            return cmpResult < 0;
                        }

                        auto lhsChunk = lhs->GetSingleUnversionedChunkOrThrow();
                        auto rhsChunk = rhs->GetSingleUnversionedChunkOrThrow();

                        if (lhsChunk != rhsChunk) {
                            return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
                        }

                        if (lhs->LowerLimit().RowIndex &&
                            rhs->LowerLimit().RowIndex &&
                            *lhs->LowerLimit().RowIndex != *rhs->LowerLimit().RowIndex)
                        {
                            return *lhs->LowerLimit().RowIndex < *rhs->LowerLimit().RowIndex;
                        }

                        return false;
                    });
            }
        }
        StripeList_->Stripes.resize(nonEmptyStripeCount);
    }

    i64 GetDataSize()
    {
        return PrimaryDataSize_ + ForeignDataSize_;
    }

    i64 GetRowCount()
    {
        return PrimaryRowCount_ + ForeignRowCount_;
    }

    int GetSliceCount()
    {
        return PrimarySliceCount_ + ForeignSliceCount_;
    }

    i64 GetPreliminaryDataSize()
    {
        return PrimaryDataSize_ + PreliminaryForeignDataSize_;
    }

    i64 GetPreliminaryRowCount()
    {
        return PrimaryRowCount_ + PreliminaryForeignRowCount_;
    }

    int GetPreliminarySliceCount()
    {
        return PrimarySliceCount_ + PreliminaryForeignSliceCount_;
    }

private:
    TChunkStripeListPtr StripeList_ = New<TChunkStripeList>();

    //! All the input cookies that provided data that forms this job.
    std::vector<IChunkPoolInput::TCookie> InputCookies_;

    const TChunkStripePtr& GetStripe(int tableIndex, bool isStripePrimary)
    {
        if (tableIndex >= StripeList_->Stripes.size()) {
            StripeList_->Stripes.resize(tableIndex + 1);
        }
        auto& stripe = StripeList_->Stripes[tableIndex];
        if (!stripe) {
            stripe = New<TChunkStripe>(!isStripePrimary /* foreign */);
        }
        return stripe;
    }
};

// NB(max42): we can not call this enum EJobState since there is another Skywalker^W NYT::NScheduler::EJobState.
DEFINE_ENUM(EManagedJobState,
    (Pending)
    (Running)
    (Completed)
);

//! A helper class that is used in TSortedChunkPool to store all the jobs with their cookies
//! and deal with their suspends, resumes etc.
class TJobManager
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(TProgressCounter, DataSizeCounter);
    DEFINE_BYREF_RO_PROPERTY(TProgressCounter, RowCounter);
    DEFINE_BYREF_RO_PROPERTY(TProgressCounter, JobCounter);
    DEFINE_BYVAL_RO_PROPERTY(int, SuspendedJobCount);

public:
    TJobManager()
    {
        DataSizeCounter_.Set(0);
        RowCounter_.Set(0);
        JobCounter_.Set(0);
    }

    //! Add a job that is built in a given builder.
    void AddJob(std::unique_ptr<TJobBuilder> jobBuilder)
    {
        YCHECK(jobBuilder);
        IChunkPoolOutput::TCookie outputCookie = Jobs_.size();

        LOG_DEBUG("Sorted job finished (Index: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
            "PrimarySliceCount: %v, ForeignDataSize: %v, ForeignRowCount: %v, "
            "ForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
            outputCookie,
            jobBuilder->GetPrimaryDataSize(),
            jobBuilder->GetPrimaryRowCount(),
            jobBuilder->GetPrimarySliceCount(),
            jobBuilder->GetForeignDataSize(),
            jobBuilder->GetForeignRowCount(),
            jobBuilder->GetForeignSliceCount(),
            jobBuilder->LowerPrimaryKey(),
            jobBuilder->UpperPrimaryKey());

        //! We know which input cookie formed this job, so for each of them we
        //! have to remember newly created job in order to be able to suspend/resume it
        //! when some input cookie changes its state.
        for (auto inputCookie : jobBuilder->InputCookies_) {
            if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
                InputCookieToAffectedOutputCookies_.resize(inputCookie + 1);
            }
            InputCookieToAffectedOutputCookies_[inputCookie].emplace_back(outputCookie);
        }

        Jobs_.emplace_back(this /* owner */, std::move(jobBuilder), outputCookie);

        JobCounter_.Increment(1);
        DataSizeCounter_.Increment(Jobs_.back().GetDataSize());
        RowCounter_.Increment(Jobs_.back().GetRowCount());
    }

    void Completed(IChunkPoolOutput::TCookie cookie)
    {
        JobCounter_.Completed(1);
        DataSizeCounter_.Completed(Jobs_[cookie].GetDataSize());
        RowCounter_.Completed(Jobs_[cookie].GetRowCount());
        Jobs_[cookie].SetState(EManagedJobState::Completed);
    }

    void Interrupted(IChunkPoolOutput::TCookie cookie)
    {
        JobCounter_.Completed(1);
        DataSizeCounter_.Completed(Jobs_[cookie].GetDataSize());
        RowCounter_.Completed(Jobs_[cookie].GetRowCount());
        JobCounter_.Increment(1);
        DataSizeCounter_.Increment(Jobs_[cookie].GetDataSize());
        RowCounter_.Increment(Jobs_[cookie].GetRowCount());

        JobCounter_.Interrupted(1);

        Jobs_[cookie].SetState(EManagedJobState::Pending);
    }

    IChunkPoolOutput::TCookie ExtractCookie()
    {
        auto cookie = CookiePool_.front();

        JobCounter_.Start(1);
        DataSizeCounter_.Start(Jobs_[cookie].GetDataSize());
        RowCounter_.Start(Jobs_[cookie].GetRowCount());
        Jobs_[cookie].SetState(EManagedJobState::Running);

        return cookie;
    }

    void Failed(IChunkPoolOutput::TCookie cookie)
    {
        JobCounter_.Failed(1);
        DataSizeCounter_.Failed(Jobs_[cookie].GetDataSize());
        RowCounter_.Failed(Jobs_[cookie].GetRowCount());
        Jobs_[cookie].SetState(EManagedJobState::Pending);
    }

    void Aborted(IChunkPoolOutput::TCookie cookie)
    {
        JobCounter_.Aborted(1);
        DataSizeCounter_.Aborted(Jobs_[cookie].GetDataSize());
        RowCounter_.Aborted(Jobs_[cookie].GetRowCount());
        Jobs_[cookie].SetState(EManagedJobState::Pending);
    }

    void Lost(IChunkPoolOutput::TCookie /* cookie */)
    {
        // TODO(max42): YT-6565 =)
        Y_UNREACHABLE();
    }

    void Suspend(IChunkPoolInput::TCookie inputCookie)
    {
        if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
            // This may happen if the input cookie is large enough but it didn't actually produce
            // any slice that made it to the final data (e.g. it was foreign and useless for the given primary input).
            return;
        }

        for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
            Jobs_[outputCookie].ChangeSuspendedStripeCountBy(+1);
        }
    }

    void Resume(IChunkPoolInput::TCookie inputCookie)
    {
        if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
            // This may happen if the input cookie is large enough but it didn't actually produce
            // any slice that made it to the final data (e.g. it was foreign and useless for the given primary input).
            return;
        }

        for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
            Jobs_[outputCookie].ChangeSuspendedStripeCountBy(-1);
        }
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, InputCookieToAffectedOutputCookies_);
        Persist(context, DataSizeCounter_);
        Persist(context, RowCounter_);
        Persist(context, JobCounter_);
        Persist(context, Jobs_);
    }

    TChunkStripeStatisticsVector GetApproximateStripeStatistics() const
    {
        if (CookiePool_.size() == 0) {
            return TChunkStripeStatisticsVector();
        }
        const auto& job = Jobs_[CookiePool_.front()];
        return job.StripeList()->GetStatistics();
    }

    int GetPendingJobCount() const
    {
        return CookiePool_.size();
    }

    const TChunkStripeListPtr& GetStripeList(IChunkPoolOutput::TCookie cookie)
    {
        YCHECK(cookie < Jobs_.size());
        YCHECK(Jobs_[cookie].GetState() == EManagedJobState::Running);
        return Jobs_[cookie].StripeList();
    }

    void SetLogger(TLogger logger)
    {
        Logger = logger;
    }

private:
    //! бассейн с печеньками^W^W^W
    //! The list of all job cookies that are in state `Pending` (i.e. do not depend on suspended data).
    std::list<IChunkPoolOutput::TCookie> CookiePool_;

    //! A mapping between input cookie and all jobs that are affected by its suspension.
    std::vector<std::vector<IChunkPoolOutput::TCookie>> InputCookieToAffectedOutputCookies_;

    //! An internal representation of a finalized job.
    class TJob
    {
        using TJobManagerPtr = TIntrusivePtr<TJobManager>;
    public:
        DEFINE_BYVAL_RO_PROPERTY(EManagedJobState, State, EManagedJobState::Pending);
        DEFINE_BYVAL_RO_PROPERTY(i64, DataSize);
        DEFINE_BYVAL_RO_PROPERTY(i64, RowCount);
        DEFINE_BYREF_RO_PROPERTY(TChunkStripeListPtr, StripeList);

    public:
        //! Used only for persistence.
        TJob()
        { }

        TJob(TJobManager* owner, std::unique_ptr<TJobBuilder> jobBuilder, IChunkPoolOutput::TCookie cookie)
            : DataSize_(jobBuilder->GetDataSize())
            , RowCount_(jobBuilder->GetRowCount())
            , StripeList_(std::move(jobBuilder->StripeList_))
            , Owner_(owner)
            , CookiePoolIterator_(Owner_->CookiePool_.end())
            , Cookie_(cookie)
        {
            UpdateSelf();
        }

        void SetState(EManagedJobState state)
        {
            State_ = state;
            UpdateSelf();
        }

        void ChangeSuspendedStripeCountBy(int delta)
        {
            SuspendedStripeCount_ += delta;
            YCHECK(SuspendedStripeCount_ >= 0);

            UpdateSelf();
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Owner_);
            Persist(context, SuspendedStripeCount_);
            Persist(context, StripeList_);
            Persist(context, Cookie_);
            Persist(context, State_);
            Persist(context, DataSize_);
            Persist(context, RowCount_);
            if (context.IsLoad()) {
                // We must add ourselves to the job pool.
                CookiePoolIterator_ = Owner_->CookiePool_.end();
                UpdateSelf();
            }
        }

    private:
        TJobManager* Owner_ = nullptr;
        int SuspendedStripeCount_ = 0;
        std::list<int>::iterator CookiePoolIterator_;
        IChunkPoolOutput::TCookie Cookie_;

        //! Is true for a job if it is present in owner's CookiePool_.
        //! Changes of this flag are accompanied with AddSelf()/RemoveSelf().
        bool InPool_ = false;
        //! Is true for a job if it is in the pending state and has suspended stripes.
        //! Changes of this flag are accompanied with SuspendSelf()/ResumeSelf().
        bool Suspended_ = false;

        //! Adds or removes self from the job pool according to the job state and suspended stripe count.
        void UpdateSelf()
        {
            bool inPoolDesired = State_ == EManagedJobState::Pending && SuspendedStripeCount_ == 0;
            if (InPool_ && !inPoolDesired) {
                RemoveSelf();
            } else if (!InPool_ && inPoolDesired) {
                AddSelf();
            }

            bool suspendedDesired = State_ == EManagedJobState::Pending && SuspendedStripeCount_ > 0;
            if (Suspended_ && !suspendedDesired) {
                ResumeSelf();
            } else if (!Suspended_ && suspendedDesired) {
                SuspendSelf();
            }
        }

        void RemoveSelf()
        {
            YCHECK(CookiePoolIterator_ != Owner_->CookiePool_.end());
            Owner_->CookiePool_.erase(CookiePoolIterator_);
            CookiePoolIterator_ = Owner_->CookiePool_.end();
            InPool_ = false;
        }

        void AddSelf()
        {
            YCHECK(CookiePoolIterator_ == Owner_->CookiePool_.end());
            CookiePoolIterator_ = Owner_->CookiePool_.insert(Owner_->CookiePool_.end(), Cookie_);
            InPool_ = true;
        }

        void SuspendSelf()
        {
            YCHECK(!Suspended_);
            Suspended_ = true;
            YCHECK(++Owner_->SuspendedJobCount_ > 0);
        }

        void ResumeSelf()
        {
            YCHECK(Suspended_);
            YCHECK(--Owner_->SuspendedJobCount_ >= 0);
            Suspended_ = false;
        }
    };

    std::vector<TJob> Jobs_;

    TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TJobManager);
DECLARE_REFCOUNTED_TYPE(TJobManager);

////////////////////////////////////////////////////////////////////

class TSortedChunkPool
    : public TChunkPoolInputBase
    // We delegate dealing with progress counters to the TJobManager class,
    // so we can't inherit from TChunkPoolOutputBase since it binds all the
    // interface methods to the progress counters stored as pool fields.
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! Used only for persistence.
    TSortedChunkPool()
    { }

    TSortedChunkPool(
        const TSortedChunkPoolOptions& options,
        std::function<IChunkSliceFetcherPtr()> chunkSliceFetcherFactory,
        std::vector<TDataSource> sources)
        : EnableKeyGuarantee_(options.EnableKeyGuarantee)
        , Sources_(std::move(sources))
        , PrimaryPrefixLength_(options.PrimaryPrefixLength)
        , ForeignPrefixLength_(options.ForeignPrefixLength)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , MaxTotalSliceCount_(options.MaxTotalSliceCount)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , SupportLocality_(options.SupportLocality)
        , OperationId_(options.OperationId)
    {
        ChunkSliceFetcher_ = chunkSliceFetcherFactory();
        ForeignStripeCookiesByTableIndex_.resize(Sources_.size());
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        JobManager_->SetLogger(Logger);
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        auto cookie = static_cast<int>(Stripes_.size());
        Stripes_.emplace_back(stripe);

        for (const auto& dataSlice : stripe->DataSlices) {
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                InputChunkMapping_[chunkSlice->GetInputChunk()] = chunkSlice->GetInputChunk();
            }
        }

        int tableIndex = stripe->GetTableIndex();

        if (Sources_[tableIndex].IsForeign()) {
            ForeignStripeCookiesByTableIndex_[tableIndex].push_back(cookie);
        }

        return cookie;
    }

    virtual void Finish() override
    {
        YCHECK(!Finished);
        TChunkPoolInputBase::Finish();

        FindTeleportChunks();
        FetchNonTeleportPrimaryDataSlices();
        PrepareForeignSources();
        SortEndpoints();
        BuildJobs();
        AttachForeignSlices();
        FinalizeJobs();

        FreeMemory();
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        suspendableStripe.Suspend();
        if (Finished) {
            JobManager_->Suspend(cookie);
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        auto newChunkMapping = suspendableStripe.ResumeAndBuildChunkMapping(stripe);
        for (const auto& pair : newChunkMapping) {
            InputChunkMapping_[pair.first] = pair.second;
        }
        if (Finished) {
            JobManager_->Resume(cookie);
        }
    }

    // IChunkPoolOutput implementation.

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        return JobManager_->GetApproximateStripeStatistics();
    }

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            GetPendingJobCount() == 0 &&
            JobManager_->JobCounter().GetRunning() == 0 &&
            JobManager_->GetSuspendedJobCount() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return JobManager_->JobCounter().GetTotal();
    }

    virtual int GetPendingJobCount() const override
    {
        return CanScheduleJob() ? JobManager_->GetPendingJobCount() : 0;
    }

    virtual i64 GetLocality(TNodeId /* nodeId */) const override
    {
        if (SupportLocality_) {
            // TODO(max42): YT-6551
            Y_UNREACHABLE();
        }
        return 0;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId /* nodeId */) override
    {
        YCHECK(Finished);

        return JobManager_->ExtractCookie();
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        auto stripeList = JobManager_->GetStripeList(cookie);

        const auto& mapping = InputChunkMapping_;
        auto mappedStripeList = New<TChunkStripeList>(stripeList->Stripes.size());
        for (int stripeIndex = 0; stripeIndex < stripeList->Stripes.size(); ++stripeIndex) {
            const auto& stripe = stripeList->Stripes[stripeIndex];
            YCHECK(stripe);
            const auto& mappedStripe = (mappedStripeList->Stripes[stripeIndex] = New<TChunkStripe>(stripe->Foreign));
            for (const auto& dataSlice : stripe->DataSlices) {
                if (dataSlice->Disabled) {
                    continue;
                }
                TInputDataSlice::TChunkSliceList mappedChunkSlices;
                for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                    auto iterator = mapping.find(chunkSlice->GetInputChunk());
                    YCHECK(iterator != mapping.end());
                    mappedChunkSlices.emplace_back(New<TInputChunkSlice>(*chunkSlice));
                    mappedChunkSlices.back()->SetInputChunk(iterator->second);
                }
                if (!mappedChunkSlices.empty()) {
                    mappedStripe->DataSlices.emplace_back(New<TInputDataSlice>(
                        dataSlice->Type,
                        std::move(mappedChunkSlices),
                        dataSlice->LowerLimit(),
                        dataSlice->UpperLimit()));
                    mappedStripe->DataSlices.back()->Tag = dataSlice->Tag;
                }
            }
        }
        return mappedStripeList;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        if (jobSummary.Interrupted) {
            yhash_set<i64> partiallyReadSliceTags;
            for (const auto& dataSlice : jobSummary.UnreadInputDataSlices) {
                auto tag = dataSlice->Tag;
                YCHECK(tag);
                YCHECK(0 <= *tag && *tag < DataSlicesByTag_.size());
                partiallyReadSliceTags.insert(*tag);
                auto originalDataSlice = DataSlicesByTag_[*tag];
                originalDataSlice->LowerLimit() = dataSlice->LowerLimit();
                for (const auto& chunkSlice : originalDataSlice->ChunkSlices) {
                    chunkSlice->LowerLimit() = dataSlice->LowerLimit();
                }
            }
            // The slices that weren't even mentioned in UnreadInputDataSlices were completely read, so
            // we should disable them for further extraction.
            auto stripeList = JobManager_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe->Foreign) {
                    continue;
                }
                for (const auto& dataSlice : stripe->DataSlices) {
                    YCHECK(dataSlice->Tag);
                    if (!partiallyReadSliceTags.has(*dataSlice->Tag)) {
                        dataSlice->Disabled = true;
                    }
                }
            }
            JobManager_->Interrupted(cookie);
        } else {
            JobManager_->Completed(cookie);
        }
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Failed(cookie);
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Aborted(cookie);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Lost(cookie);
    }

    virtual i64 GetTotalDataSize() const override
    {
        return JobManager_->DataSizeCounter().GetTotal();
    }

    virtual i64 GetRunningDataSize() const override
    {
        return JobManager_->DataSizeCounter().GetRunning();
    }

    virtual i64 GetCompletedDataSize() const override
    {
        return JobManager_->DataSizeCounter().GetCompleted();
    }

    virtual i64 GetPendingDataSize() const override
    {
        return JobManager_->DataSizeCounter().GetPending();
    }

    virtual i64 GetTotalRowCount() const override
    {
        return JobManager_->RowCounter().GetTotal();
    }

    const TProgressCounter& GetJobCounter() const
    {
        return JobManager_->JobCounter();
    }

    const std::vector<TInputChunkPtr>& GetTeleportChunks() const
    {
        return TeleportChunks_;
    }

    virtual void Persist(const TPersistenceContext& context) final override
    {
        TChunkPoolInputBase::Persist(context);

        using NYT::Persist;
        Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, InputChunkMapping_);
        Persist(context, Stripes_);
        Persist(context, SupportLocality_);
        Persist(context, JobManager_);
        Persist(context, OperationId_);
        Persist(context, ChunkPoolId_);
        Persist(context, DataSlicesByTag_);
        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            JobManager_->SetLogger(Logger);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSortedChunkPool, 0x91bca805);

    //! A data structure responsible for keeping the prepared jobs, extracting them and dealing with suspend/resume
    //! events.
    TJobManagerPtr JobManager_ = New<TJobManager>();

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<std::unique_ptr<TJobBuilder>> Jobs_;

    //! Stores correspondence between primary data slices added via `AddPrimaryDataSlice`
    //! (both unversioned and versioned) and their input cookies.
    yhash_map<TInputDataSlicePtr, IChunkPoolInput::TCookie> PrimaryDataSliceToInputCookie_;

    //! Used to additionally slice the unversioned slices.
    IChunkSliceFetcherPtr ChunkSliceFetcher_;

    struct TEndpoint
    {
        EEndpointType Type;
        TInputDataSlicePtr DataSlice;
        TKey Key;
        i64 RowIndex;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Type);
            Persist(context, DataSlice);
            Persist(context, Key);
        }
    };

    //! Endpoints of primary table slices in SortedReduce and SortedMerge.
    std::vector<TEndpoint> Endpoints_;

    //! During the pool lifetime some input chunks may be suspended and replaced with
    //! another chunks on resumption. We keep track of all such substitutions in this
    //! map and apply it whenever the `GetStripeList` is called.
    yhash_map<TInputChunkPtr, TInputChunkPtr> InputChunkMapping_;

    //! Guarantee that each key goes to the single job.
    bool EnableKeyGuarantee_;

    //! Information about input sources (e.g. input tables for sorted reduce operation).
    std::vector<TDataSource> Sources_;

    //! Length of the key according to which primary tables should be sorted during
    //! sorted reduce / sorted merge.
    int PrimaryPrefixLength_;

    //! Length of the key that defines a range in foreign tables that should be joined
    //! to the job.
    int ForeignPrefixLength_;

    //! An option to control chunk teleportation logic. Only large complete
    //! chunks of at least that size will be teleported.
    i64 MinTeleportChunkSize_;

    //! An upper bound for a total number of slices that is allowed. If this value
    //! is exceeded, an exception is thrown.
    i64 MaxTotalSliceCount_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    //! Stores input cookies of all foreign stripes grouped by input table index.
    std::vector<std::vector<int>> ForeignStripeCookiesByTableIndex_;

    //! Stores the number of slices in all jobs up to current moment.
    i64 TotalSliceCount_ = 0;

    //! Stores all input chunks to be teleported.
    std::vector<TInputChunkPtr> TeleportChunks_;

    //! Stores all data slices inside this pool indexed by their tags.
    std::vector<TInputDataSlicePtr> DataSlicesByTag_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    bool SupportLocality_ = false;

    TLogger Logger = TLogger("Operation");

    TOperationId OperationId_;

    TGuid ChunkPoolId_ = TGuid::Create();

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportPrimaryDataSlices()
    {
        yhash_map<TInputChunkPtr, IChunkPoolInput::TCookie> unversionedInputChunkToInputCookie;
        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& suspendableStripe = Stripes_[inputCookie];
            const auto& stripe = suspendableStripe.GetStripe();

            if (suspendableStripe.GetTeleport() || !Sources_[stripe->GetTableIndex()].IsPrimary()) {
                continue;
            }

            // Unversioned data slices should be additionally sliced using ChunkSliceFetcher_,
            // while versioned slices are taken as is.
            if (stripe->DataSlices.front()->Type == EDataSourceType::UnversionedTable) {
                auto inputChunk = stripe->DataSlices.front()->GetSingleUnversionedChunkOrThrow();
                ChunkSliceFetcher_->AddChunk(inputChunk);
                unversionedInputChunkToInputCookie[inputChunk] = inputCookie;
            } else {
                AddPrimaryDataSlice(stripe->DataSlices.front(), inputCookie);
            }
        }

        WaitFor(ChunkSliceFetcher_->Fetch())
            .ThrowOnError();

        for (const auto& chunkSlice : ChunkSliceFetcher_->GetChunkSlices()) {
            auto it = unversionedInputChunkToInputCookie.find(chunkSlice->GetInputChunk());
            YCHECK(it != unversionedInputChunkToInputCookie.end());

            // We additionally slice maniac slices by evenly by row indices.
            auto chunk = chunkSlice->GetInputChunk();
            if (chunk->IsCompleteChunk() &&
                CompareRows(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey, PrimaryPrefixLength_) == 0 &&
                chunkSlice->GetDataSize() > JobSizeConstraints_->GetInputSliceDataSize())
            {
                chunkSlice->UpperLimit().RowIndex = chunk->GetRowCount();
                auto smallerSlices = chunkSlice->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataSize(),
                    JobSizeConstraints_->GetInputSliceRowCount());
                for (const auto& smallerSlice : smallerSlices) {
                    AddPrimaryDataSlice(CreateUnversionedInputDataSlice(smallerSlice), it->second);
                }
            } else {
                AddPrimaryDataSlice(CreateUnversionedInputDataSlice(chunkSlice), it->second);
            }
        }
        unversionedInputChunkToInputCookie.clear();
    }

    //! In this function all data slices that correspond to teleportable unversioned input chunks
    //! are added to `TeleportChunks_`.
    void FindTeleportChunks()
    {
        // Consider each chunk as a segment [minKey, maxKey]. Chunk may be teleported if:
        // 1) it is unversioned;
        // 2) it is complete (i. e. does not contain non-trivial read limits);
        // 3a) in case of SortedMerge: no other key (belonging to the different input chunk) lies in the interval (minKey, maxKey);
        // 3b) in case of SortedReduce: no other key lies in the s [minKey', maxKey'] (NB: if some other chunk shares endpoint
        //     with our chunk, our chunk can not be teleported since all instances of each key must be either teleported or
        //     be processed in the same job).
        //
        // We use the following logic to determine how different kinds of intervals are located on a line:
        //
        // * with respect to the s [a; b] data slice [l; r) is located:
        // *** to the left iff r <= a;
        // *** to the right iff l > b;
        // * with respect to the interval (a; b) interval [l; r) is located:
        // *** to the left iff r <= succ(a);
        // *** to the right iff l >= b.
        //
        // Using it, we find how many upper/lower keys are located to the left/right of candidate s/interval using the binary search
        // over the vectors of all lower and upper limits (including or excluding the chunk endpoints if `includeChunkBoundaryKeys`)
        // and check that in total there are exactly |ss| - 1 segments, this would exactly mean that all data slices (except the one
        // that corresponds to the chosen chunk) are either to the left or to the right of the chosen chunk.
        //
        // Unfortunately, there is a tricky corner case that should be treated differently: when chunk pool type is SortedMerge
        // (i. e. `includeChunkBoundaryKeys` is true) and the interval (a, b) is empty. For example, consider the following data slices:
        // (1) ["a"; "n",max), (2) ["n"; "n",max), (3) ["n"; "z",max) and suppose that we test the chunk (*) ["n"; "n"] to be teleportable.
        // In fact it is teleportable: (*) can be located between slices (1) and (2) or between slices (2) and (3). But if we try
        // to use the approach described above and calculate number of slices to the left of ("n"; "n") and slices to the right of ("n"; "n"),
        // we will account slice (2) twice: it is located to the left of ("n"; "n") because "n",max <= succ("n") and it is also located
        // to the right of ("n"; "n") because "n" >= "n". In the other words, when chunk pool type is SortedMerge teleporting of single-key chunks
        // is a hard work :(
        //
        // To overcome this difficulty, we additionally subtract the number of single-key slices that define the same key as our chunk.
        TPeriodicYielder yielder(PrepareYieldPeriod);

        std::vector<TKey> lowerLimits, upperLimits;
        yhash_map<TKey, int> singleKeySliceNumber;
        std::vector<std::pair<TInputChunkPtr, IChunkPoolInput::TCookie>> teleportCandidates;

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            if (Sources_[stripe->GetTableIndex()].IsPrimary()) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    yielder.TryYield();

                    if (Sources_[stripe->GetTableIndex()].IsTeleportable() &&
                        dataSlice->GetSingleUnversionedChunkOrThrow()->IsLargeCompleteChunk(MinTeleportChunkSize_))
                    {
                        teleportCandidates.emplace_back(dataSlice->GetSingleUnversionedChunkOrThrow(), inputCookie);
                    }

                    lowerLimits.emplace_back(GetKeyPrefix(dataSlice->LowerLimit().Key, PrimaryPrefixLength_, RowBuffer_));
                    if (dataSlice->UpperLimit().Key.GetCount() > PrimaryPrefixLength_) {
                        upperLimits.emplace_back(GetKeySuccessor(GetKeyPrefix(dataSlice->UpperLimit().Key, PrimaryPrefixLength_, RowBuffer_), RowBuffer_));
                    } else {
                        upperLimits.emplace_back(dataSlice->UpperLimit().Key);
                    }

                    if (CompareRows(dataSlice->LowerLimit().Key, dataSlice->UpperLimit().Key, PrimaryPrefixLength_) == 0) {
                        ++singleKeySliceNumber[lowerLimits.back()];
                    }
                }
            }
        }

        if (teleportCandidates.empty()) {
            return;
        }

        std::sort(lowerLimits.begin(), lowerLimits.end());
        std::sort(upperLimits.begin(), upperLimits.end());
        yielder.TryYield();

        int dataSlicesCount = lowerLimits.size();

        for (const auto& pair : teleportCandidates) {
            yielder.TryYield();

            const auto& teleportCandidate = pair.first;
            auto cookie = pair.second;

            // NB: minKey and maxKey are inclusive, in contrast to the lower/upper limits.
            auto minKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MinKey, PrimaryPrefixLength_, RowBuffer_);
            auto maxKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MaxKey, PrimaryPrefixLength_, RowBuffer_);

            int slicesToTheLeft = (EnableKeyGuarantee_
                ? std::upper_bound(upperLimits.begin(), upperLimits.end(), minKey)
                : std::upper_bound(upperLimits.begin(), upperLimits.end(), GetKeySuccessor(minKey, RowBuffer_))) - upperLimits.begin();
            int slicesToTheRight = lowerLimits.end() - (EnableKeyGuarantee_
                ? std::upper_bound(lowerLimits.begin(), lowerLimits.end(), maxKey)
                : std::lower_bound(lowerLimits.begin(), lowerLimits.end(), maxKey));
            int extraCoincidingSingleKeySlices = 0;
            if (minKey == maxKey && !EnableKeyGuarantee_) {
                auto it = singleKeySliceNumber.find(minKey);
                YCHECK(it != singleKeySliceNumber.end());
                // +1 because we accounted data slice for the current chunk twice (in slicesToTheLeft and slicesToTheRight),
                // but we actually want to account it zero time since we condier only data slices different from current.
                extraCoincidingSingleKeySlices = it->second + 1;
            }
            int nonIntersectingSlices = slicesToTheLeft + slicesToTheRight - extraCoincidingSingleKeySlices;
            YCHECK(nonIntersectingSlices <= dataSlicesCount - 1);
            if (nonIntersectingSlices == dataSlicesCount - 1) {
                Stripes_[cookie].SetTeleport(true);
                TeleportChunks_.emplace_back(teleportCandidate);
            }
        }

        // The last step is to sort the resulting teleport chunks in order to be able to use
        // them while we build the jobs (they provide us with mandatory places where we have to
        // break the jobs).
        std::sort(
            TeleportChunks_.begin(),
            TeleportChunks_.end(),
            [] (const TInputChunkPtr& lhs, const TInputChunkPtr& rhs) {
                int cmpMin = CompareRows(lhs->BoundaryKeys()->MinKey, rhs->BoundaryKeys()->MinKey);
                if (cmpMin != 0) {
                    return cmpMin < 0;
                }
                int cmpMax = CompareRows(lhs->BoundaryKeys()->MaxKey, rhs->BoundaryKeys()->MaxKey);
                if (cmpMax != 0) {
                    return cmpMax < 0;
                }
                // This is possible only when both chunks contain the same only key.
                YCHECK(lhs->BoundaryKeys()->MinKey == lhs->BoundaryKeys()->MaxKey);
                return false;
            });

        i64 totalTeleportChunkSize = 0;
        for (const auto& teleportChunk : TeleportChunks_) {
            totalTeleportChunkSize += teleportChunk->GetUncompressedDataSize();
        }

        LOG_INFO("Teleported %v chunks of total size %v", TeleportChunks_.size(), totalTeleportChunkSize);
    }

    void AddPrimaryDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie)
    {
        if (dataSlice->LowerLimit().Key >= dataSlice->UpperLimit().Key) {
            // This can happen if ranges were specified.
            // Chunk slice fetcher can produce empty slices.
            return;
        }

        PrimaryDataSliceToInputCookie_[dataSlice] = cookie;

        TEndpoint leftEndpoint;
        TEndpoint rightEndpoint;

        if (EnableKeyGuarantee_) {
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetKeyPrefix(dataSlice->LowerLimit().Key, PrimaryPrefixLength_, RowBuffer_),
                0LL /* RowIndex */
            };

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetKeySuccessor(GetKeyPrefix(dataSlice->UpperLimit().Key, PrimaryPrefixLength_, RowBuffer_), RowBuffer_),
                0LL /* RowIndex */
            };
        } else {
            leftEndpoint = {
                EEndpointType::Left,
                dataSlice,
                GetStrictKey(dataSlice->LowerLimit().Key, PrimaryPrefixLength_ + 1, RowBuffer_, EValueType::Max),
                dataSlice->LowerLimit().RowIndex.Get(0)
            };

            rightEndpoint = {
                EEndpointType::Right,
                dataSlice,
                GetStrictKeySuccessor(dataSlice->UpperLimit().Key, PrimaryPrefixLength_, RowBuffer_, EValueType::Max),
                dataSlice->UpperLimit().RowIndex.Get(std::numeric_limits<i64>::max())
            };
        }


        try {
            ValidateClientKey(leftEndpoint.Key);
            ValidateClientKey(rightEndpoint.Key);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                "Error validating sample key in input table %v",
                dataSlice->GetTableIndex())
                << ex;
        }

        Endpoints_.push_back(leftEndpoint);
        Endpoints_.push_back(rightEndpoint);
    }

    void SortEndpoints()
    {
        LOG_INFO("Sorting %v endpoints", static_cast<int>(Endpoints_.size()));
        std::sort(
            Endpoints_.begin(),
            Endpoints_.end(),
            [=] (const TEndpoint& lhs, const TEndpoint& rhs) -> bool {
                {
                    auto cmpResult = CompareRows(lhs.Key, rhs.Key);
                    if (cmpResult != 0) {
                        return cmpResult < 0;
                    }
                }

                if (lhs.RowIndex != rhs.RowIndex) {
                    return lhs.RowIndex < rhs.RowIndex;
                }

                if (lhs.DataSlice->Type == EDataSourceType::UnversionedTable &&
                    rhs.DataSlice->Type == EDataSourceType::UnversionedTable)
                {
                    // If keys are equal, we put slices in the same order they are in the original table.
                    const auto& lhsChunk = lhs.DataSlice->GetSingleUnversionedChunkOrThrow();
                    const auto& rhsChunk = rhs.DataSlice->GetSingleUnversionedChunkOrThrow();

                    auto cmpResult = lhsChunk->GetTableRowIndex() - rhsChunk->GetTableRowIndex();
                    if (cmpResult != 0) {
                        return cmpResult < 0;
                    }
                }

                {
                    auto cmpResult = static_cast<int>(lhs.Type) - static_cast<int>(rhs.Type);
                    if (cmpResult != 0) {
                        return cmpResult > 0;
                    }
                }

                return false;
            });
    }

    void CheckTotalSliceCountLimit() const
    {
        if (TotalSliceCount_ > MaxTotalSliceCount_) {
            // TODO(max42): error text should be more generic, there may be no input tables.
            THROW_ERROR_EXCEPTION("Total number of data slices in operation is too large. Consider reducing job count or reducing chunk count in input tables.")
                    << TErrorAttribute("actual_total_slice_count", TotalSliceCount_)
                    << TErrorAttribute("max_total_slice_count", MaxTotalSliceCount_)
                    << TErrorAttribute("current_job_count", Jobs_.size());
        }
    }

    void BuildJobs()
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        Jobs_.emplace_back(std::make_unique<TJobBuilder>());

        yhash_map<TInputDataSlicePtr, TKey> openedSlicesLowerLimits;

        // An index of a closest teleport chunk to the right of current endpoint.
        int nextTeleportChunk = 0;

        auto endJob = [&] (const TKey& lastKey) {
            auto upperKey = GetKeyPrefixSuccessor(lastKey, PrimaryPrefixLength_, RowBuffer_);
            for (auto iterator = openedSlicesLowerLimits.begin(); iterator != openedSlicesLowerLimits.end(); ) {
                auto nextIterator = std::next(iterator);
                const auto& dataSlice = iterator->first;
                auto& lowerLimit = iterator->second;
                auto exactDataSlice = CreateInputDataSlice(dataSlice, lowerLimit, upperKey);
                TagPrimaryDataSlice(exactDataSlice);
                Jobs_.back()->AddDataSlice(
                    exactDataSlice,
                    PrimaryDataSliceToInputCookie_[dataSlice],
                    true /* isPrimary */);
                lowerLimit = upperKey;
                if (lowerLimit >= dataSlice->UpperLimit().Key) {
                    openedSlicesLowerLimits.erase(iterator);
                }
                iterator = nextIterator;
            }
            // If current job does not contain primary data slices then we can re-use it,
            // otherwise we should create a new job.
            if (Jobs_.back()->GetSliceCount() > 0) {
                LOG_DEBUG("Sorted job created (Index: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
                    "PrimarySliceCount_: %v, PreliminaryForeignDataSize: %v, PreliminaryForeignRowCount: %v, "
                    "PreliminaryForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
                    static_cast<int>(Jobs_.size()) - 1,
                    Jobs_.back()->GetPrimaryDataSize(),
                    Jobs_.back()->GetPrimaryRowCount(),
                    Jobs_.back()->GetPrimarySliceCount(),
                    Jobs_.back()->GetPreliminaryForeignDataSize(),
                    Jobs_.back()->GetPreliminaryForeignRowCount(),
                    Jobs_.back()->GetPreliminaryForeignSliceCount(),
                    Jobs_.back()->LowerPrimaryKey(),
                    Jobs_.back()->UpperPrimaryKey());

                TotalSliceCount_ += Jobs_.back()->GetSliceCount();
                CheckTotalSliceCountLimit();
                Jobs_.emplace_back(std::make_unique<TJobBuilder>());
            }
        };

        for (int index = 0, nextKeyIndex = 0; index < Endpoints_.size(); ++index) {
            yielder.TryYield();
            auto key = Endpoints_[index].Key;
            while (nextKeyIndex != Endpoints_.size() && Endpoints_[nextKeyIndex].Key == key) {
                ++nextKeyIndex;
            }

            auto nextKey = (nextKeyIndex == Endpoints_.size()) ? TKey() : Endpoints_[nextKeyIndex].Key;
            bool nextKeyIsLeft = (nextKeyIndex == Endpoints_.size()) ? false : Endpoints_[nextKeyIndex].Type == EEndpointType::Left;

            while (nextTeleportChunk < TeleportChunks_.size() &&
                CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, key, PrimaryPrefixLength_) < 0)
            {
                ++nextTeleportChunk;
            }

            if (Endpoints_[index].Type == EEndpointType::Left) {
                openedSlicesLowerLimits[Endpoints_[index].DataSlice] = TKey();
            } else if (Endpoints_[index].Type == EEndpointType::Right) {
                const auto& dataSlice = Endpoints_[index].DataSlice;
                auto it = openedSlicesLowerLimits.find(dataSlice);
                // It might have happened that we already removed this slice from the
                // `openedSlicesLowerLimits` during one of the previous `endJob` calls.
                if (it != openedSlicesLowerLimits.end()) {
                    auto exactDataSlice = CreateInputDataSlice(dataSlice, it->second);
                    TagPrimaryDataSlice(exactDataSlice);
                    Jobs_.back()->AddDataSlice(exactDataSlice, PrimaryDataSliceToInputCookie_[dataSlice], true /* isPrimary */);
                    openedSlicesLowerLimits.erase(it);
                }
            } else if (Endpoints_[index].Type == EEndpointType::ForeignRight) {
                Jobs_.back()->AddPreliminaryForeignDataSlice(Endpoints_[index].DataSlice);
            }

            // If next teleport chunk is closer than next data slice then we are obligated to close the job here.
            bool shouldEndHere = nextKeyIndex == index + 1 &&
                nextKeyIsLeft &&
                (nextTeleportChunk != TeleportChunks_.size() &&
                CompareRows(TeleportChunks_[nextTeleportChunk]->BoundaryKeys()->MinKey, nextKey, PrimaryPrefixLength_) <= 0);

            // If key guarantee is enabled, we can not end here if next data slice may contain the same reduce key.
            bool canEndHere = !EnableKeyGuarantee_ || index + 1 == nextKeyIndex;

            // The contrary would mean that teleport chunk was chosen incorrectly, because teleport chunks
            // should not normally intersect the other data slices.
            YCHECK(!(shouldEndHere && !canEndHere));

            bool jobIsLargeEnough =
                Jobs_.back()->GetPreliminarySliceCount() + openedSlicesLowerLimits.size() > JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
                Jobs_.back()->GetPreliminaryDataSize() > JobSizeConstraints_->GetDataSizePerJob();

            if (canEndHere && (shouldEndHere || jobIsLargeEnough)) {
                endJob(key);
            }
        }
        endJob(MaxKey());
        if (!Jobs_.empty() && Jobs_.back()->GetSliceCount() == 0) {
            Jobs_.pop_back();
        }

        LOG_INFO("Created %v jobs", Jobs_.size());
    }

    void PrepareForeignSources()
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        for (int tableIndex = 0; tableIndex < ForeignStripeCookiesByTableIndex_.size(); ++tableIndex) {
            if (!Sources_[tableIndex].IsForeign()) {
                continue;
            }

            yielder.TryYield();

            auto& tableStripeCookies = ForeignStripeCookiesByTableIndex_[tableIndex];

            // In most cases the foreign table stripes follow in sorted order, but still let's ensure that.
            auto cmpStripesByKey = [&] (int lhs, int rhs) {
                const auto& lhsLowerLimit = Stripes_[lhs].GetStripe()->DataSlices.front()->LowerLimit().Key;
                const auto& lhsUpperLimit = Stripes_[lhs].GetStripe()->DataSlices.back()->UpperLimit().Key;
                const auto& rhsLowerLimit = Stripes_[rhs].GetStripe()->DataSlices.front()->LowerLimit().Key;
                const auto& rhsUpperLimit = Stripes_[rhs].GetStripe()->DataSlices.back()->UpperLimit().Key;
                if (lhsLowerLimit != rhsLowerLimit) {
                    return lhsLowerLimit < rhsLowerLimit;
                } else if (lhsUpperLimit != rhsUpperLimit) {
                    return lhsUpperLimit < rhsUpperLimit;
                } else {
                    // If lower limits coincide and upper limits coincide too, these stripes
                    // must either be the same stripe or they both are maniac stripes with the same key.
                    // In both cases they may follow in any order.
                    return false;
                }
            };
            if (!std::is_sorted(tableStripeCookies.begin(), tableStripeCookies.end(), cmpStripesByKey)) {
                std::stable_sort(tableStripeCookies.begin(), tableStripeCookies.end(), cmpStripesByKey);
            }
            for (const auto& stripeCookie : tableStripeCookies) {
                for (const auto& dataSlice : Stripes_[stripeCookie].GetStripe()->DataSlices) {
                    // NB: We do not need to shorten keys here. Endpoints of type "Foreign" only make
                    // us to stop, to add all foreign slices up to the current moment and to check
                    // if we already have to end the job due to the large data size or slice count.
                    TEndpoint leftEndpoint = {
                        EEndpointType::ForeignRight,
                        dataSlice,
                        dataSlice->LowerLimit().Key,
                        dataSlice->LowerLimit().RowIndex.Get(0)
                    };
                    TEndpoint rightEndpoint = {
                        EEndpointType::ForeignRight,
                        dataSlice,
                        dataSlice->UpperLimit().Key,
                        dataSlice->UpperLimit().RowIndex.Get(std::numeric_limits<i64>::max())
                    };
                    Endpoints_.emplace_back(leftEndpoint);
                    Endpoints_.emplace_back(rightEndpoint);
                }
            }
        }
    }

    void AttachForeignSlices()
    {
        TPeriodicYielder yielder(PrepareYieldPeriod);

        for (int tableIndex = 0; tableIndex < ForeignStripeCookiesByTableIndex_.size(); ++tableIndex) {
            if (!Sources_[tableIndex].IsForeign()) {
                continue;
            }

            yielder.TryYield();

            int startJobIndex = 0;

            for (int inputCookie : ForeignStripeCookiesByTableIndex_[tableIndex]) {
                const auto& stripe = Stripes_[inputCookie].GetStripe();
                for (const auto& foreignDataSlice : stripe->DataSlices) {
                    yielder.TryYield();

                    while (
                        startJobIndex != Jobs_.size() &&
                        CompareRows(Jobs_[startJobIndex]->UpperPrimaryKey(), foreignDataSlice->LowerLimit().Key, ForeignPrefixLength_) < 0)
                    {
                        ++startJobIndex;
                    }
                    if (startJobIndex == Jobs_.size()) {
                        break;
                    }
                    for (
                        int jobIndex = startJobIndex;
                        jobIndex < Jobs_.size() &&
                        CompareRows(Jobs_[jobIndex]->LowerPrimaryKey(), foreignDataSlice->UpperLimit().Key, ForeignPrefixLength_) <= 0;
                        ++jobIndex)
                    {
                        yielder.TryYield();

                        auto exactForeignDataSlice = CreateInputDataSlice(
                            foreignDataSlice,
                            GetKeyPrefix(Jobs_[jobIndex]->LowerPrimaryKey(), ForeignPrefixLength_, RowBuffer_),
                            GetKeyPrefixSuccessor(Jobs_[jobIndex]->UpperPrimaryKey(), ForeignPrefixLength_, RowBuffer_));
                        TotalSliceCount_++;
                        Jobs_[jobIndex]->AddDataSlice(exactForeignDataSlice, inputCookie, false /* isPrimary */);
                    }
                }
                CheckTotalSliceCountLimit();
            }
        }
    }

    void FinalizeJobs()
    {
        for (auto& job : Jobs_) {
            job->Finalize();
            JobManager_->AddJob(std::move(job));
        }

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie];
            if (stripe.IsSuspended() && !stripe.GetTeleport()) {
                JobManager_->Suspend(inputCookie);
            }
        }
    }

    bool CanScheduleJob() const
    {
        return Finished && JobManager_->GetPendingJobCount() != 0;
    }

    void TagPrimaryDataSlice(const TInputDataSlicePtr& dataSlice)
    {
        dataSlice->Tag = DataSlicesByTag_.size();
        DataSlicesByTag_.emplace_back(dataSlice);
    }

    void FreeMemory()
    {
        PrimaryDataSliceToInputCookie_.clear();
        Endpoints_.clear();
        Sources_.clear();
        ForeignStripeCookiesByTableIndex_.clear();
        JobSizeConstraints_.Reset();
        Jobs_.clear();
    }
};

////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TSortedChunkPool);

std::unique_ptr<IChunkPool> CreateSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    std::function<IChunkSliceFetcherPtr()> chunkSliceFetcherFactory,
    std::vector<TDataSource> sources)
{
    return std::make_unique<TSortedChunkPool>(options, std::move(chunkSliceFetcherFactory), std::move(sources));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
