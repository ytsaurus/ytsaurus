#include "job_manager.h"

#include <yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TJobStub::AddDataSlice(const TInputDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary)
{
    if (dataSlice->IsEmpty()) {
        return;
    }

    int streamIndex = dataSlice->InputStreamIndex;
    int rangeIndex = dataSlice->GetRangeIndex();
    auto& stripe = GetStripe(streamIndex, rangeIndex, isPrimary);
    stripe->DataSlices.emplace_back(dataSlice);
    if (cookie != IChunkPoolInput::NullCookie) {
        InputCookies_.emplace_back(cookie);
    }

    if (isPrimary) {
        if (LowerPrimaryKey_ > dataSlice->LowerLimit().Key) {
            LowerPrimaryKey_ = dataSlice->LowerLimit().Key;
        }
        if (UpperPrimaryKey_ < dataSlice->UpperLimit().Key) {
            UpperPrimaryKey_ = dataSlice->UpperLimit().Key;
        }

        ++PrimarySliceCount_;
        PrimaryDataWeight_ += dataSlice->GetDataWeight();
        PrimaryRowCount_ += dataSlice->GetRowCount();
    } else {
        ++ForeignSliceCount_;
        ForeignDataWeight_ += dataSlice->GetDataWeight();
        ForeignRowCount_ += dataSlice->GetRowCount();
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
    for (auto& pair : StripeMap_) {
        auto& stripe = pair.second;
        const auto& statistics = stripe->GetStatistics();
        StripeList_->TotalDataWeight += statistics.DataWeight;
        StripeList_->TotalRowCount += statistics.RowCount;
        StripeList_->TotalChunkCount += statistics.ChunkCount;
        if (sortByPosition) {
            // This is done to ensure that all the data slices inside a stripe
            // are not only sorted by key, but additionally by their position
            // in the original table.

            auto lessThan = [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) -> bool {
                if (lhs->UpperLimit().Key <= rhs->LowerLimit().Key) {
                    return true;
                }

                if (lhs->UpperLimit().RowIndex &&
                    rhs->LowerLimit().RowIndex &&
                    *lhs->UpperLimit().RowIndex <= *rhs->LowerLimit().RowIndex)
                {
                    return true;
                }

                return false;
            };

            std::sort(
                stripe->DataSlices.begin(),
                stripe->DataSlices.end(),
                [&] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
                    // Compare slice with itself.
                    if (lhs.Get() == rhs.Get()) {
                        return false;
                    }

                    if (lhs->Type == EDataSourceType::UnversionedTable) {
                        auto lhsChunk = lhs->GetSingleUnversionedChunkOrThrow();
                        auto rhsChunk = rhs->GetSingleUnversionedChunkOrThrow();
                        if (lhsChunk != rhsChunk) {
                            return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
                        }
                    }

                    if (lessThan(lhs, rhs)) {
                        return true;
                    }

                    // Since slices of the single table must be disjoint, if lhs is not less than rhs,
                    // then rhs must be less then lhs.
                    YT_VERIFY(lessThan(rhs, lhs));

                    return false;
                });
        }
        StripeList_->Stripes.emplace_back(std::move(stripe));
    }
    StripeMap_.clear();

    // This order is crucial for ordered map.
    std::sort(StripeList_->Stripes.begin(), StripeList_->Stripes.end(), [] (const TChunkStripePtr& lhs, const TChunkStripePtr& rhs) {
        auto& lhsSlice = lhs->DataSlices.front();
        auto& rhsSlice = rhs->DataSlices.front();

        if (lhsSlice->GetTableIndex() != rhsSlice->GetTableIndex()) {
            return lhsSlice->GetTableIndex() < rhsSlice->GetTableIndex();
        }

        return lhsSlice->GetRangeIndex() < rhsSlice->GetRangeIndex();
    });
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

TString TJobStub::GetDebugString() const
{
    TStringBuilder builder;
    builder.AppendString("{");
    bool isFirst = true;
    for (const auto& stripe : StripeList_->Stripes) {
        for (const auto& dataSlice : stripe->DataSlices) {
            if (isFirst) {
                isFirst = false;
            } else {
                builder.AppendString(", ");
            }
            std::vector<TChunkId> chunkIds;
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                chunkIds.emplace_back(chunkSlice->GetInputChunk()->ChunkId());
            }
            builder.AppendFormat("{DataWeight: %v, LowerLimit: %v, UpperLimit: %v, InputStreamIndex: %v, ChunkIds: %v}",
                dataSlice->GetDataWeight(),
                dataSlice->LowerLimit(),
                dataSlice->UpperLimit(),
                dataSlice->InputStreamIndex,
                chunkIds);
        }
    }
    builder.AppendString("}");

    return builder.Flush();
}

void TJobStub::SetUnsplittable()
{
    StripeList_->IsSplittable = false;
}

const TChunkStripePtr& TJobStub::GetStripe(int streamIndex, int rangeIndex, bool isStripePrimary)
{
    auto& stripe = StripeMap_[std::make_pair(streamIndex, rangeIndex)];
    if (!stripe) {
        stripe = New<TChunkStripe>(!isStripePrimary /* foreign */);
    }
    return stripe;
}

////////////////////////////////////////////////////////////////////////////////

//! An internal representation of a finalized job.
TJobManager::TJob::TJob()
{ }

TJobManager::TJob::TJob(TJobManager* owner, std::unique_ptr<TJobStub> jobStub, IChunkPoolOutput::TCookie cookie)
    : IsBarrier_(jobStub->GetIsBarrier())
    , DataWeight_(jobStub->GetDataWeight())
    , RowCount_(jobStub->GetRowCount())
    , LowerLimit_(jobStub->LowerPrimaryKey())
    , UpperLimit_(jobStub->UpperPrimaryKey())
    , StripeList_(std::move(jobStub->StripeList_))
    , InputCookies_(std::move(jobStub->InputCookies_))
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
    YT_VERIFY(SuspendedStripeCount_ >= 0);
    UpdateSelf();
}

void TJobManager::TJob::Invalidate()
{
    YT_VERIFY(!Invalidated_);
    Invalidated_ = true;
    StripeList_.Reset();
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
    YT_VERIFY(CookiePoolIterator_ != Owner_->CookiePool_->end());
    Owner_->CookiePool_->erase(CookiePoolIterator_);
    CookiePoolIterator_ = Owner_->CookiePool_->end();
    InPool_ = false;
}

void TJobManager::TJob::AddSelf()
{
    YT_VERIFY(CookiePoolIterator_ == Owner_->CookiePool_->end());
    CookiePoolIterator_ = Owner_->CookiePool_->insert(Owner_->CookiePool_->end(), Cookie_);
    InPool_ = true;
}

void TJobManager::TJob::SuspendSelf()
{
    YT_VERIFY(!Suspended_);
    Suspended_ = true;
    YT_VERIFY(++Owner_->SuspendedJobCount_ > 0);
}

void TJobManager::TJob::ResumeSelf()
{
    YT_VERIFY(Suspended_);
    YT_VERIFY(--Owner_->SuspendedJobCount_ >= 0);
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
    if (jobStubs.empty()) {
        return;
    }
    YT_LOG_DEBUG("Adding jobs to job manager (JobCount: %v)",
        jobStubs.size());
    for (auto& jobStub : jobStubs) {
        AddJob(std::move(jobStub));
    }
}

//! Add a job that is built from the given stub.
IChunkPoolOutput::TCookie TJobManager::AddJob(std::unique_ptr<TJobStub> jobStub)
{
    YT_VERIFY(jobStub);
    IChunkPoolOutput::TCookie outputCookie = Jobs_.size();

    if (jobStub->GetIsBarrier()) {
        YT_LOG_DEBUG("Adding barrier to job manager (Index: %v)", outputCookie);
        Jobs_.emplace_back(this, std::move(jobStub), outputCookie);
        Jobs_.back().SetState(EJobState::Completed);
        // TODO(max42): do not assign cookie to barriers.
        return outputCookie;
    }

    YT_LOG_DEBUG("Job added to job manager (Index: %v, PrimaryDataWeight: %v, PrimaryRowCount: %v, "
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
        if (SuspendedInputCookies_.contains(inputCookie)) {
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
    if (CookiePool_->empty()) {
        return IChunkPoolInput::NullCookie;
    }

    auto cookie = *CookiePool_->begin();

    Jobs_[cookie].UpdateCounters(&TProgressCounter::Start);
    Jobs_[cookie].SetState(EJobState::Running);

    YT_VERIFY(!Jobs_[cookie].GetIsBarrier());

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
    YT_VERIFY(SuspendedInputCookies_.insert(inputCookie).second);

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
    YT_VERIFY(SuspendedInputCookies_.erase(inputCookie) == 1);

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
    YT_VERIFY(0 <= inputCookie && inputCookie < Jobs_.size());
    Jobs_[inputCookie].Invalidate();
}

std::vector<TInputDataSlicePtr> TJobManager::ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(0 <= inputCookie && inputCookie < Jobs_.size());
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
    YT_VERIFY(cookie < Jobs_.size());
    YT_VERIFY(Jobs_[cookie].GetState() == EJobState::Running);
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

void TJobManager::Enlarge(i64 dataWeightPerJob, i64 primaryDataWeightPerJob)
{
    // TODO(max42): keep the order of jobs in a singly linked list that allows us to use this
    // procedure not only during the initial creation of jobs or right after the whole pool invalidation,
    // but at the arbitrary moment of job manager lifetime. After that implement YT-9019.

    YT_LOG_DEBUG("Enlarging jobs (DataWeightPerJob: %v, PrimaryDataWeightPerJob: %v)",
        dataWeightPerJob,
        primaryDataWeightPerJob);

    std::unique_ptr<TJobStub> currentJobStub;
    std::vector<IChunkPoolOutput::TCookie> joinedJobCookies;
    // Join `job` into current job stub and return true if
    // ((their joint data weight does not exceed provided limits) or (force is true)),
    // return false and leave the current job stub as is otherwise.
    // Force is used to initialize the job stub with a first job, sometimes it may already exceed the
    // given limits.
    auto tryJoinJob = [&] (int jobIndex, bool force) -> bool {
        const auto& job = Jobs_[jobIndex];
        i64 primaryDataWeight = currentJobStub->GetPrimaryDataWeight();
        i64 foreignDataWeight = currentJobStub->GetForeignDataWeight();
        for (const auto& stripe : job.StripeList()->Stripes) {
            for (const auto& dataSlice : stripe->DataSlices) {
                (stripe->Foreign ? foreignDataWeight : primaryDataWeight) += dataSlice->GetDataWeight();
            }
        }
        if ((primaryDataWeight <= primaryDataWeightPerJob && foreignDataWeight + primaryDataWeight <= dataWeightPerJob) || force) {
            for (const auto& stripe : job.StripeList()->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    currentJobStub->AddDataSlice(dataSlice, IChunkPoolInput::NullCookie, !stripe->Foreign);
                }
            }
            currentJobStub->InputCookies_.insert(currentJobStub->InputCookies_.end(), job.InputCookies().begin(), job.InputCookies().end());
            joinedJobCookies.emplace_back(jobIndex);

            return true;
        }
        YT_LOG_DEBUG("Stopping enlargement due to data weight constraints "
            "(NewDataWeight: %v, DataWeightPerJob: %v, NewPrimaryDataWeight: %v, PrimaryDataWeightPerJob: %v)",
            foreignDataWeight + primaryDataWeight,
            dataWeightPerJob,
            primaryDataWeight,
            primaryDataWeightPerJob);
        return false;
    };

    std::vector<std::unique_ptr<TJobStub>> newJobs;
    for (int startIndex = FirstValidJobIndex_, finishIndex = FirstValidJobIndex_; startIndex < Jobs_.size(); startIndex = finishIndex) {
        if (Jobs_[startIndex].GetIsBarrier()) {
            // NB: One may think that we should carefully bring this barrier between newly formed jobs but we
            // currently never enlarge jobs after building them from scratch, so barriers have no use after enlarging.
            // But when we store jobs in a singly linked list, we should deal with barriers carefully!
            finishIndex = startIndex + 1;
            continue;
        }

        currentJobStub = std::make_unique<TJobStub>();
        while (true) {
            if (finishIndex == Jobs_.size()) {
                YT_LOG_DEBUG("Stopping enlargement due to end of job list (StartIndex: %v, FinishIndex: %v)", startIndex, finishIndex);
                break;
            }

            // TODO(max42): we can not meet invalidated job as we enlarge jobs only when we build them from scratch.
            // In future we will iterate over a list of non-invalidated jobs, so it won't happen too.
            YT_VERIFY(!Jobs_[finishIndex].IsInvalidated());

            if (Jobs_[finishIndex].GetIsBarrier()) {
                YT_LOG_DEBUG("Stopping enlargement due to barrier (StartIndex: %v, FinishIndex: %v)", startIndex, finishIndex);
                break;
            }

            if (!tryJoinJob(finishIndex, finishIndex == startIndex /* force */)) {
                // This case is logged in tryJoinJob.
                break;
            }
            ++finishIndex;
        }

        if (joinedJobCookies.size() > 1) {
            std::vector<IChunkPoolOutput::TCookie> outputCookies;
            YT_LOG_DEBUG("Joining together jobs (JoinedJobCookies: %v, DataWeight: %v, PrimaryDataWeight: %v)",
                joinedJobCookies,
                currentJobStub->GetDataWeight(),
                currentJobStub->GetPrimaryDataWeight());
            for (int index : joinedJobCookies) {
                Jobs_[index].Invalidate();
                Jobs_[index].UpdateCounters(&TProgressCounter::Decrement);
            }
            currentJobStub->Finalize(false /* sortByPosition */);
            newJobs.emplace_back(std::move(currentJobStub));
        } else {
            YT_LOG_DEBUG("Leaving job as is (Cookie: %v)", startIndex);
        }
        joinedJobCookies.clear();
    }

    AddJobs(std::move(newJobs));
}

std::pair<TUnversionedRow, TUnversionedRow> TJobManager::GetLimits(IChunkPoolOutput::TCookie cookie) const
{
    YT_VERIFY(cookie >= 0);
    YT_VERIFY(cookie < static_cast<int>(Jobs_.size()));
    const auto& job = Jobs_[cookie];
    return {job.GetLowerLimit(), job.GetUpperLimit()};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
