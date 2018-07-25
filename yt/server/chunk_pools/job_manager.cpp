#include "job_manager.h"

namespace NYT {
namespace NChunkPools {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void TJobStub::AddDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary)
{
    if (dataSlice->IsEmpty()) {
        return;
    }

    int streamIndex = dataSlice->InputStreamIndex;
    auto& stripe = GetStripe(streamIndex, isPrimary);
    stripe->DataSlices.emplace_back(dataSlice);
    InputCookies_.emplace_back(cookie);

    if (isPrimary) {
        if (LowerPrimaryKey_ > dataSlice->LowerLimit().Key) {
            LowerPrimaryKey_ = dataSlice->LowerLimit().Key;
        }
        if (UpperPrimaryKey_ < dataSlice->UpperLimit().Key) {
            UpperPrimaryKey_ = dataSlice->UpperLimit().Key;
        }
        PrimaryDataWeight_ += dataSlice->GetDataWeight();
        PrimaryRowCount_ += dataSlice->GetRowCount();
        ++PrimarySliceCount_;
    } else {
        ForeignDataWeight_ += dataSlice->GetDataWeight();
        ForeignRowCount_ += dataSlice->GetRowCount();
        ++ForeignSliceCount_;
    }
}

void TJobStub::AddPreliminaryForeignDataSlice(const TInputDataSlicePtr& dataSlice)
{
    PreliminaryForeignDataWeight_ += dataSlice->GetDataWeight();
    PreliminaryForeignRowCount_ += dataSlice->GetRowCount();
    ++PreliminaryForeignSliceCount_;
}

void TJobStub::Finalize(bool sortByPosition)
{
    int nonEmptyStripeCount = 0;
    for (int index = 0; index < StripeList_->Stripes.size(); ++index) {
        if (StripeList_->Stripes[index]) {
            auto& stripe = StripeList_->Stripes[nonEmptyStripeCount];
            stripe = std::move(StripeList_->Stripes[index]);
            ++nonEmptyStripeCount;
            const auto& statistics = stripe->GetStatistics();
            StripeList_->TotalDataWeight += statistics.DataWeight;
            StripeList_->TotalRowCount += statistics.RowCount;
            StripeList_->TotalChunkCount += statistics.ChunkCount;
            if (sortByPosition) {
                // This is done to ensure that all the data slices inside a stripe
                // are not only sorted by key, but additionally by their position
                // in the original table.
                std::sort(
                    stripe->DataSlices.begin(),
                    stripe->DataSlices.end(),
                    [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
                        if (lhs->Type == EDataSourceType::UnversionedTable) {
                            auto lhsChunk = lhs->GetSingleUnversionedChunkOrThrow();
                            auto rhsChunk = rhs->GetSingleUnversionedChunkOrThrow();
                            if (lhsChunk != rhsChunk) {
                                return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
                            }
                        }

                        if (lhs->LowerLimit().RowIndex &&
                            rhs->LowerLimit().RowIndex &&
                            *lhs->LowerLimit().RowIndex != *rhs->LowerLimit().RowIndex)
                        {
                            return *lhs->LowerLimit().RowIndex < *rhs->LowerLimit().RowIndex;
                        }

                        auto cmpResult = CompareRows(lhs->LowerLimit().Key, rhs->LowerLimit().Key);
                        if (cmpResult != 0) {
                            return cmpResult < 0;
                        }

                        return false;
                    });
            }
        }
    }
    StripeList_->Stripes.resize(nonEmptyStripeCount);
}

i64 TJobStub::GetDataWeight() const
{
    return PrimaryDataWeight_ + ForeignDataWeight_;
}

i64 TJobStub::GetRowCount() const
{
    return PrimaryRowCount_ + ForeignRowCount_;
}

int TJobStub::GetSliceCount() const
{
    return PrimarySliceCount_ + ForeignSliceCount_;
}

i64 TJobStub::GetPreliminaryDataWeight() const
{
    return PrimaryDataWeight_ + PreliminaryForeignDataWeight_;
}

i64 TJobStub::GetPreliminaryRowCount() const
{
    return PrimaryRowCount_ + PreliminaryForeignRowCount_;
}

int TJobStub::GetPreliminarySliceCount() const
{
    return PrimarySliceCount_ + PreliminaryForeignSliceCount_;
}

void TJobStub::SetUnsplittable()
{
    StripeList_->IsSplittable = false;
}

const TChunkStripePtr& TJobStub::GetStripe(int streamIndex, bool isStripePrimary)
{
    if (streamIndex >= StripeList_->Stripes.size()) {
        StripeList_->Stripes.resize(streamIndex + 1);
    }
    auto& stripe = StripeList_->Stripes[streamIndex];
    if (!stripe) {
        stripe = New<TChunkStripe>(!isStripePrimary /* foreign */);
    }
    return stripe;
}

////////////////////////////////////////////////////////////////////////////////

//! An internal representation of a finalized job.
TJobManager::TJob::TJob()
{ }

TJobManager::TJob::TJob(TJobManager* owner, std::unique_ptr<TJobStub> jobBuilder, IChunkPoolOutput::TCookie cookie)
    : DataWeight_(jobBuilder->GetDataWeight())
    , RowCount_(jobBuilder->GetRowCount())
    , StripeList_(std::move(jobBuilder->StripeList_))
    , Owner_(owner)
    , CookiePoolIterator_(Owner_->CookiePool_->end())
    , Cookie_(cookie)
{ }

void TJobManager::TJob::SetState(EJobState state)
{
    State_ = state;
    UpdateSelf();
}

void TJobManager::TJob::ChangeSuspendedStripeCountBy(int delta)
{
    SuspendedStripeCount_ += delta;
    YCHECK(SuspendedStripeCount_ >= 0);
    UpdateSelf();
}

void TJobManager::TJob::Invalidate()
{
    YCHECK(!Invalidated_);
    Invalidated_ = true;
    StripeList_->Stripes.clear();
    UpdateSelf();
}

bool TJobManager::TJob::IsInvalidated() const
{
    return Invalidated_;
}

void TJobManager::TJob::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Owner_);
    Persist(context, SuspendedStripeCount_);
    Persist(context, StripeList_);
    Persist(context, Cookie_);
    Persist(context, State_);
    Persist(context, DataWeight_);
    Persist(context, RowCount_);
    Persist(context, Invalidated_);
    if (context.IsLoad()) {
        // We must add ourselves to the job pool.
        CookiePoolIterator_ = Owner_->CookiePool_->end();
        UpdateSelf();
    }
}

void TJobManager::TJob::UpdateSelf()
{
    bool inPoolDesired =
        State_ == EJobState::Pending &&
        SuspendedStripeCount_ == 0 &&
        !Invalidated_;
    if (InPool_ && !inPoolDesired) {
        RemoveSelf();
    } else if (!InPool_ && inPoolDesired) {
        AddSelf();
    }

    bool suspendedDesired =
        State_ == EJobState::Pending &&
        SuspendedStripeCount_ > 0 &&
        !Invalidated_;
    if (Suspended_ && !suspendedDesired) {
        ResumeSelf();
    } else if (!Suspended_ && suspendedDesired) {
        SuspendSelf();
    }
}

void TJobManager::TJob::RemoveSelf()
{
    YCHECK(CookiePoolIterator_ != Owner_->CookiePool_->end());
    Owner_->CookiePool_->erase(CookiePoolIterator_);
    CookiePoolIterator_ = Owner_->CookiePool_->end();
    InPool_ = false;
}

void TJobManager::TJob::AddSelf()
{
    YCHECK(CookiePoolIterator_ == Owner_->CookiePool_->end());
    CookiePoolIterator_ = Owner_->CookiePool_->insert(Owner_->CookiePool_->end(), Cookie_);
    InPool_ = true;
}

void TJobManager::TJob::SuspendSelf()
{
    YCHECK(!Suspended_);
    Suspended_ = true;
    YCHECK(++Owner_->SuspendedJobCount_ > 0);
}

void TJobManager::TJob::ResumeSelf()
{
    YCHECK(Suspended_);
    YCHECK(--Owner_->SuspendedJobCount_ >= 0);
    Suspended_ = false;
}

template <class... TArgs>
void TJobManager::TJob::UpdateCounters(void (TProgressCounter::*Method)(i64, TArgs...), TArgs... args)
{
    (Owner_->JobCounter_.Get()->*Method)(1, std::forward<TArgs>(args)...);
    (Owner_->DataWeightCounter_.Get()->*Method)(DataWeight_, std::forward<TArgs>(args)...);
    (Owner_->RowCounter_.Get()->*Method)(RowCount_, std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

TJobManager::TStripeListComparator::TStripeListComparator(TJobManager* owner)
    : Owner_(owner)
{ }

bool TJobManager::TStripeListComparator::operator()(IChunkPoolOutput::TCookie lhs, IChunkPoolOutput::TCookie rhs) const
{
    const auto& lhsJob = Owner_->Jobs_[lhs];
    const auto& rhsJob = Owner_->Jobs_[rhs];
    if (lhsJob.GetDataWeight() != rhsJob.GetDataWeight()) {
        return lhsJob.GetDataWeight() > rhsJob.GetDataWeight();
    }
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

TJobManager::TJobManager()
    : CookiePool_(std::make_unique<TCookiePool>(TJobManager::TStripeListComparator(this /* owner */)))
{
    DataWeightCounter_->Set(0);
    RowCounter_->Set(0);
    JobCounter_->Set(0);
}

void TJobManager::AddJobs(std::vector<std::unique_ptr<TJobStub>> jobStubs)
{
    for (auto& jobStub : jobStubs) {
        AddJob(std::move(jobStub));
    }
}

//! Add a job that is built from the given stub.
IChunkPoolOutput::TCookie TJobManager::AddJob(std::unique_ptr<TJobStub> jobStub)
{
    YCHECK(jobStub);
    IChunkPoolOutput::TCookie outputCookie = Jobs_.size();

    LOG_DEBUG("Sorted job finished (Index: %v, PrimaryDataWeight: %v, PrimaryRowCount: %v, "
        "PrimarySliceCount: %v, ForeignDataWeight: %v, ForeignRowCount: %v, "
        "ForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
        outputCookie,
        jobStub->GetPrimaryDataWeight(),
        jobStub->GetPrimaryRowCount(),
        jobStub->GetPrimarySliceCount(),
        jobStub->GetForeignDataWeight(),
        jobStub->GetForeignRowCount(),
        jobStub->GetForeignSliceCount(),
        jobStub->LowerPrimaryKey(),
        jobStub->UpperPrimaryKey());

    int initialSuspendedStripeCount = 0;

    //! We know which input cookie formed this job, so for each of them we
    //! have to remember newly created job in order to be able to suspend/resume it
    //! when some input cookie changes its state.
    for (auto inputCookie : jobStub->InputCookies_) {
        if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
            InputCookieToAffectedOutputCookies_.resize(inputCookie + 1);
        }
        InputCookieToAffectedOutputCookies_[inputCookie].emplace_back(outputCookie);
        if (SuspendedInputCookies_.has(inputCookie)) {
            ++initialSuspendedStripeCount;
        }
    }

    Jobs_.emplace_back(this /* owner */, std::move(jobStub), outputCookie);
    Jobs_.back().SetState(EJobState::Pending);
    Jobs_.back().ChangeSuspendedStripeCountBy(initialSuspendedStripeCount);

    Jobs_.back().UpdateCounters(&TProgressCounter::Increment);
    return outputCookie;
}

void TJobManager::Completed(IChunkPoolOutput::TCookie cookie, EInterruptReason reason)
{
    Jobs_[cookie].UpdateCounters(&TProgressCounter::Completed, reason);
    if (reason == EInterruptReason::None) {
        Jobs_[cookie].SetState(EJobState::Completed);
    }
}

IChunkPoolOutput::TCookie TJobManager::ExtractCookie()
{
    auto cookie = *CookiePool_->begin();

    Jobs_[cookie].UpdateCounters(&TProgressCounter::Start);
    Jobs_[cookie].SetState(EJobState::Running);

    return cookie;
}

void TJobManager::Failed(IChunkPoolOutput::TCookie cookie)
{
    Jobs_[cookie].UpdateCounters(&TProgressCounter::Failed);
    Jobs_[cookie].SetState(EJobState::Pending);
}

void TJobManager::Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason)
{
    Jobs_[cookie].UpdateCounters(&TProgressCounter::Aborted, reason);
    Jobs_[cookie].SetState(EJobState::Pending);
}

void TJobManager::Lost(IChunkPoolOutput::TCookie cookie)
{
    Jobs_[cookie].UpdateCounters(&TProgressCounter::Lost);
    Jobs_[cookie].SetState(EJobState::Pending);
}

void TJobManager::Suspend(IChunkPoolInput::TCookie inputCookie)
{
    YCHECK(SuspendedInputCookies_.insert(inputCookie).second);

    if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
        // This may happen if jobs that use this input were not added yet
        // (note that suspend may happen in Finish() before DoFinish()).
        return;
    }

    for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
        Jobs_[outputCookie].ChangeSuspendedStripeCountBy(+1);
    }
}

void TJobManager::Resume(IChunkPoolInput::TCookie inputCookie)
{
    YCHECK(SuspendedInputCookies_.erase(inputCookie) == 1);

    if (InputCookieToAffectedOutputCookies_.size() <= inputCookie) {
        // This may happen if jobs that use this input were not added yet
        // (note that suspend may happen in Finish() before DoFinish()).
        return;
    }

    for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
        Jobs_[outputCookie].ChangeSuspendedStripeCountBy(-1);
    }
}

void TJobManager::Invalidate(IChunkPoolInput::TCookie inputCookie)
{
    YCHECK(0 <= inputCookie && inputCookie < Jobs_.size());
    Jobs_[inputCookie].Invalidate();
}

std::vector<TInputDataSlicePtr> TJobManager::ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie)
{
    YCHECK(0 <= inputCookie && inputCookie < Jobs_.size());
    std::vector<TInputDataSlicePtr> foreignSlices;
    for (const auto& stripe : Jobs_[inputCookie].StripeList()->Stripes) {
        if (stripe->Foreign) {
            std::move(stripe->DataSlices.begin(), stripe->DataSlices.end(), std::back_inserter(foreignSlices));
            stripe->DataSlices.clear();
        }
    }
    return foreignSlices;
}

void TJobManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    if (context.IsLoad()) {
        CookiePool_ = std::make_unique<TCookiePool>(TStripeListComparator(this /* owner */));
    }

    Persist(context, InputCookieToAffectedOutputCookies_);
    Persist(context, DataWeightCounter_);
    Persist(context, RowCounter_);
    Persist(context, JobCounter_);
    Persist(context, Jobs_);
    Persist(context, FirstValidJobIndex_);
    Persist(context, SuspendedInputCookies_);
}

TChunkStripeStatisticsVector TJobManager::GetApproximateStripeStatistics() const
{
    if (CookiePool_->size() == 0) {
        return TChunkStripeStatisticsVector();
    }
    auto cookie = *(CookiePool_->begin());
    const auto& job = Jobs_[cookie];
    return job.StripeList()->GetStatistics();
}

int TJobManager::GetPendingJobCount() const
{
    return CookiePool_->size();
}

const TChunkStripeListPtr& TJobManager::GetStripeList(IChunkPoolOutput::TCookie cookie)
{
    YCHECK(cookie < Jobs_.size());
    YCHECK(Jobs_[cookie].GetState() == EJobState::Running);
    return Jobs_[cookie].StripeList();
}

void TJobManager::InvalidateAllJobs()
{
    while (FirstValidJobIndex_ < Jobs_.size()) {
        if (!Jobs_[FirstValidJobIndex_].IsInvalidated()) {
            Jobs_[FirstValidJobIndex_].Invalidate();
        }
        FirstValidJobIndex_++;
    }
}

void TJobManager::SetLogger(TLogger logger)
{
    Logger = logger;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
