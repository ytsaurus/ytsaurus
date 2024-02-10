#include "legacy_job_manager.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Used only for YT_LOG_ALERT.
static const TLogger Logger("LegacyJobManager");

////////////////////////////////////////////////////////////////////////////////

void TLegacyJobStub::AddDataSlice(const TLegacyDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary)
{
    YT_VERIFY(dataSlice->IsLegacy);

    if (dataSlice->IsEmpty()) {
        return;
    }

    int streamIndex = dataSlice->GetInputStreamIndex();
    int rangeIndex = dataSlice->GetRangeIndex();
    auto& stripe = GetStripe(streamIndex, rangeIndex, isPrimary);
    stripe->DataSlices.emplace_back(dataSlice);
    if (cookie != IChunkPoolInput::NullCookie) {
        InputCookies_.emplace_back(cookie);
    }

    if (isPrimary) {
        if (LowerPrimaryKey_ > dataSlice->LegacyLowerLimit().Key) {
            LowerPrimaryKey_ = dataSlice->LegacyLowerLimit().Key;
        }
        if (UpperPrimaryKey_ < dataSlice->LegacyUpperLimit().Key) {
            UpperPrimaryKey_ = dataSlice->LegacyUpperLimit().Key;
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

void TLegacyJobStub::AddPreliminaryForeignDataSlice(const TLegacyDataSlicePtr& dataSlice)
{
    PreliminaryForeignDataWeight_ += dataSlice->GetDataWeight();
    PreliminaryForeignRowCount_ += dataSlice->GetRowCount();
    ++PreliminaryForeignSliceCount_;
}

void TLegacyJobStub::Finalize(bool sortByPosition)
{
    for (const auto& [_, stripe] : StripeMap_) {
        const auto& statistics = stripe->GetStatistics();
        StripeList_->TotalDataWeight += statistics.DataWeight;
        StripeList_->TotalRowCount += statistics.RowCount;
        StripeList_->TotalChunkCount += statistics.ChunkCount;
        if (sortByPosition) {
            // This is done to ensure that all the data slices inside a stripe
            // are not only sorted by key, but additionally by their position
            // in the original table.

            auto lessThan = [] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
                if (lhs->LegacyUpperLimit().Key <= rhs->LegacyLowerLimit().Key) {
                    return true;
                }

                if (lhs->LegacyUpperLimit().RowIndex &&
                    rhs->LegacyLowerLimit().RowIndex &&
                    *lhs->LegacyUpperLimit().RowIndex <= *rhs->LegacyLowerLimit().RowIndex)
                {
                    return true;
                }

                return false;
            };

            auto dataSliceComparator = [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
                // Compare slice with itself.
                if (lhs.Get() == rhs.Get()) {
                    return false;
                }

                if (lhs->Type == EDataSourceType::UnversionedTable) {
                    auto lhsChunk = lhs->GetSingleUnversionedChunk();
                    auto rhsChunk = rhs->GetSingleUnversionedChunk();
                    if (lhsChunk != rhsChunk) {
                        return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
                    }
                } else if (lhs->Type == EDataSourceType::VersionedTable) {
                    // Tags should contain input cookies of data slices. Input cookies correspond to the order
                    // in which data slices are created in CombineVersionedDataSlices() which is OK for
                    // checking if one data slice should appear before another.
                    YT_VERIFY(rhs->Type == EDataSourceType::VersionedTable);
                    YT_VERIFY(lhs->Tag);
                    YT_VERIFY(rhs->Tag);
                    if (*lhs->Tag != *rhs->Tag) {
                        return *lhs->Tag < *rhs->Tag;
                    }
                }

                if (lessThan(lhs, rhs)) {
                    return true;
                }

                // Since slices of the single table must be disjoint, if lhs is not less than rhs,
                // then rhs must be less then lhs.
                YT_VERIFY(lessThan(rhs, lhs));

                return false;
            };

            bool sortNeeded = false;
            for (size_t index = 0; index + 1 < stripe->DataSlices.size(); ++index) {
                const auto& currentDataSlice = stripe->DataSlices[index];
                const auto& nextDataSlice = stripe->DataSlices[index + 1];
                if (!dataSliceComparator(currentDataSlice, nextDataSlice)) {
                    sortNeeded = true;
                    break;
                }
            }

            if (sortNeeded) {
                std::sort(
                    stripe->DataSlices.begin(),
                    stripe->DataSlices.end(),
                    dataSliceComparator);
            }
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

i64 TLegacyJobStub::GetDataWeight() const
{
    return PrimaryDataWeight_ + ForeignDataWeight_;
}

i64 TLegacyJobStub::GetRowCount() const
{
    return PrimaryRowCount_ + ForeignRowCount_;
}

int TLegacyJobStub::GetSliceCount() const
{
    return PrimarySliceCount_ + ForeignSliceCount_;
}

i64 TLegacyJobStub::GetPreliminaryDataWeight() const
{
    return PrimaryDataWeight_ + PreliminaryForeignDataWeight_;
}

i64 TLegacyJobStub::GetPreliminaryRowCount() const
{
    return PrimaryRowCount_ + PreliminaryForeignRowCount_;
}

int TLegacyJobStub::GetPreliminarySliceCount() const
{
    return PrimarySliceCount_ + PreliminaryForeignSliceCount_;
}

TString TLegacyJobStub::GetDebugString() const
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
                chunkIds.emplace_back(chunkSlice->GetInputChunk()->GetChunkId());
            }
            builder.AppendFormat("{DataWeight: %v, LowerLimit: %v, UpperLimit: %v, InputStreamIndex: %v, ChunkIds: %v}",
                dataSlice->GetDataWeight(),
                dataSlice->LegacyLowerLimit(),
                dataSlice->LegacyUpperLimit(),
                dataSlice->GetInputStreamIndex(),
                chunkIds);
        }
    }
    builder.AppendString("}");

    return builder.Flush();
}

const TChunkStripePtr& TLegacyJobStub::GetStripe(int streamIndex, int rangeIndex, bool isStripePrimary)
{
    auto& stripe = StripeMap_[std::pair(streamIndex, rangeIndex)];
    if (!stripe) {
        stripe = New<TChunkStripe>(!isStripePrimary /*foreign*/);
    }
    return stripe;
}

////////////////////////////////////////////////////////////////////////////////

TLegacyJobManager::TJob::TJob(TLegacyJobManager* owner, std::unique_ptr<TLegacyJobStub> jobStub, IChunkPoolOutput::TCookie cookie)
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
    , DataWeightProgressCounterGuard_(owner->DataWeightCounter(), jobStub->GetDataWeight())
    , RowProgressCounterGuard_(owner->RowCounter(), jobStub->GetRowCount())
    , JobProgressCounterGuard_(owner->JobCounter())
{ }

void TLegacyJobManager::TJob::SetState(EJobState state)
{
    State_ = state;
    UpdateSelf();
}

void TLegacyJobManager::TJob::SetInterruptReason(NScheduler::EInterruptReason reason)
{
    InterruptReason_ = reason;
}

void TLegacyJobManager::TJob::ChangeSuspendedStripeCountBy(int delta)
{
    SuspendedStripeCount_ += delta;
    YT_VERIFY(SuspendedStripeCount_ >= 0);
    UpdateSelf();
}

void TLegacyJobManager::TJob::Invalidate()
{
    YT_VERIFY(!Invalidated_);
    Invalidated_ = true;
    StripeList_.Reset();
    UpdateSelf();
}

void TLegacyJobManager::TJob::Remove()
{
    YT_VERIFY(!Removed_);
    Removed_ = true;
    StripeList_.Reset();
    UpdateSelf();
}

bool TLegacyJobManager::TJob::IsInvalidated() const
{
    return Invalidated_;
}

void TLegacyJobManager::TJob::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, State_);
    Persist(context, IsBarrier_);
    Persist(context, DataWeight_);
    Persist(context, RowCount_);
    Persist(context, LowerLimit_);
    Persist(context, UpperLimit_);
    Persist(context, StripeList_);
    Persist(context, InputCookies_);
    Persist(context, Owner_);
    Persist(context, SuspendedStripeCount_);
    Persist(context, Cookie_);
    Persist(context, Invalidated_);
    Persist(context, Removed_);
    Persist(context, DataWeightProgressCounterGuard_);
    Persist(context, RowProgressCounterGuard_);
    Persist(context, JobProgressCounterGuard_);
    Persist(context, InterruptReason_);

    if (context.IsLoad()) {
        // We must add ourselves to the job pool.
        CookiePoolIterator_ = Owner_->CookiePool_->end();
        UpdateSelf();
    }
}

void TLegacyJobManager::TJob::UpdateSelf()
{
    EProgressCategory newProgressCategory;
    if (IsBarrier_ || Removed_) {
        newProgressCategory = EProgressCategory::None;
    } else if (Invalidated_) {
        newProgressCategory = EProgressCategory::Invalidated;
    } else if (State_ == EJobState::Pending) {
        if (SuspendedStripeCount_ == 0) {
            newProgressCategory = EProgressCategory::Pending;
        } else {
            newProgressCategory = EProgressCategory::Suspended;
        }
    } else if (State_ == EJobState::Running) {
        newProgressCategory = EProgressCategory::Running;
    } else if (State_ == EJobState::Completed) {
        newProgressCategory = EProgressCategory::Completed;
    } else {
        YT_ABORT();
    }

    bool inPoolDesired = (newProgressCategory == EProgressCategory::Pending);
    if (InPool_ && !inPoolDesired) {
        RemoveSelf();
    } else if (!InPool_ && inPoolDesired) {
        AddSelf();
    }

    bool suspendedDesired = (newProgressCategory == EProgressCategory::Suspended);
    if (Suspended_ && !suspendedDesired) {
        ResumeSelf();
    } else if (!Suspended_ && suspendedDesired) {
        SuspendSelf();
    }

    if (newProgressCategory == EProgressCategory::Completed) {
        CallProgressCounterGuards(&TProgressCounterGuard::SetCompletedCategory, InterruptReason_);
    } else {
        CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, newProgressCategory);
    }
}

void TLegacyJobManager::TJob::RemoveSelf()
{
    YT_VERIFY(CookiePoolIterator_ != Owner_->CookiePool_->end());
    Owner_->CookiePool_->erase(CookiePoolIterator_);
    CookiePoolIterator_ = Owner_->CookiePool_->end();
    InPool_ = false;
}

void TLegacyJobManager::TJob::AddSelf()
{
    YT_VERIFY(CookiePoolIterator_ == Owner_->CookiePool_->end());
    CookiePoolIterator_ = Owner_->CookiePool_->insert(Owner_->CookiePool_->end(), Cookie_);
    InPool_ = true;
}

void TLegacyJobManager::TJob::SuspendSelf()
{
    YT_VERIFY(!Suspended_);
    Suspended_ = true;
}

void TLegacyJobManager::TJob::ResumeSelf()
{
    YT_VERIFY(Suspended_);
    Suspended_ = false;
}

template <class... TArgs>
void TLegacyJobManager::TJob::CallProgressCounterGuards(void (TProgressCounterGuard::*Method)(TArgs...), TArgs... args)
{
    (DataWeightProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
    (RowProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
    (JobProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

TLegacyJobManager::TStripeListComparator::TStripeListComparator(TLegacyJobManager* owner)
    : Owner_(owner)
{ }

bool TLegacyJobManager::TStripeListComparator::operator()(IChunkPoolOutput::TCookie lhs, IChunkPoolOutput::TCookie rhs) const
{
    const auto& lhsJob = Owner_->Jobs_[lhs];
    const auto& rhsJob = Owner_->Jobs_[rhs];
    if (lhsJob.GetDataWeight() != rhsJob.GetDataWeight()) {
        return lhsJob.GetDataWeight() > rhsJob.GetDataWeight();
    }
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

TLegacyJobManager::TLegacyJobManager()
    : CookiePool_(std::make_unique<TCookiePool>(TLegacyJobManager::TStripeListComparator(this /*owner*/)))
{ }

TLegacyJobManager::TLegacyJobManager(const TLogger& logger)
    : CookiePool_(std::make_unique<TCookiePool>(TLegacyJobManager::TStripeListComparator(this /*owner*/)))
    , Logger(logger)
{ }

std::vector<IChunkPoolOutput::TCookie> TLegacyJobManager::AddJobs(std::vector<std::unique_ptr<TLegacyJobStub>> jobStubs)
{
    if (jobStubs.empty()) {
        return {};
    }
    YT_LOG_DEBUG("Adding jobs to job manager (JobCount: %v)", jobStubs.size());
    std::vector<IChunkPoolOutput::TCookie> outputCookies;
    outputCookies.reserve(jobStubs.size());
    for (auto& jobStub : jobStubs) {
        auto outputCookie = AddJob(std::move(jobStub));
        outputCookies.push_back(outputCookie);
    }
    return outputCookies;
}

//! Add a job that is built from the given stub.
IChunkPoolOutput::TCookie TLegacyJobManager::AddJob(std::unique_ptr<TLegacyJobStub> jobStub)
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
        if (std::ssize(InputCookieToAffectedOutputCookies_) <= inputCookie) {
            InputCookieToAffectedOutputCookies_.resize(inputCookie + 1);
        }
        InputCookieToAffectedOutputCookies_[inputCookie].emplace_back(outputCookie);
        if (SuspendedInputCookies_.contains(inputCookie)) {
            ++initialSuspendedStripeCount;
        }
    }

    Jobs_.emplace_back(this /*owner*/, std::move(jobStub), outputCookie);
    Jobs_.back().SetState(EJobState::Pending);
    Jobs_.back().ChangeSuspendedStripeCountBy(initialSuspendedStripeCount);

    return outputCookie;
}

void TLegacyJobManager::Completed(IChunkPoolOutput::TCookie cookie, EInterruptReason reason)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.SetInterruptReason(reason);
    job.SetState(EJobState::Completed);
}

IChunkPoolOutput::TCookie TLegacyJobManager::ExtractCookie()
{
    if (CookiePool_->empty()) {
        return IChunkPoolInput::NullCookie;
    }

    auto cookie = *CookiePool_->begin();
    auto& job = Jobs_[cookie];
    YT_VERIFY(!job.GetIsBarrier());
    YT_VERIFY(job.GetState() == EJobState::Pending);

    job.SetState(EJobState::Running);

    return cookie;
}

void TLegacyJobManager::Failed(IChunkPoolOutput::TCookie cookie)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnFailed);
    job.SetState(EJobState::Pending);
}

void TLegacyJobManager::Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnAborted, reason);
    job.SetState(EJobState::Pending);
}

void TLegacyJobManager::Lost(IChunkPoolOutput::TCookie cookie)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Completed);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnLost);
    job.SetState(EJobState::Pending);
}

void TLegacyJobManager::Suspend(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(SuspendedInputCookies_.insert(inputCookie).second);

    if (std::ssize(InputCookieToAffectedOutputCookies_) <= inputCookie) {
        // This may happen if jobs that use this input were not added yet
        // (note that suspend may happen in Finish() before DoFinish()).
        return;
    }

    for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
        auto& job = Jobs_[outputCookie];
        job.ChangeSuspendedStripeCountBy(+1);
    }
}

void TLegacyJobManager::Resume(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(SuspendedInputCookies_.erase(inputCookie) == 1);

    if (std::ssize(InputCookieToAffectedOutputCookies_) <= inputCookie) {
        // This may happen if jobs that use this input were not added yet
        // (note that suspend may happen in Finish() before DoFinish()).
        return;
    }

    for (auto outputCookie : InputCookieToAffectedOutputCookies_[inputCookie]) {
        auto& job = Jobs_[outputCookie];
        job.ChangeSuspendedStripeCountBy(-1);
    }
}

void TLegacyJobManager::Invalidate(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(0 <= inputCookie && inputCookie < std::ssize(Jobs_));
    auto& job = Jobs_[inputCookie];
    job.Invalidate();
}

std::vector<TLegacyDataSlicePtr> TLegacyJobManager::ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(0 <= inputCookie && inputCookie < std::ssize(Jobs_));
    std::vector<TLegacyDataSlicePtr> foreignSlices;
    for (const auto& stripe : Jobs_[inputCookie].StripeList()->Stripes) {
        if (stripe->Foreign) {
            std::move(stripe->DataSlices.begin(), stripe->DataSlices.end(), std::back_inserter(foreignSlices));
            stripe->DataSlices.clear();
        }
    }
    return foreignSlices;
}

void TLegacyJobManager::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    if (context.IsLoad()) {
        CookiePool_ = std::make_unique<TCookiePool>(TStripeListComparator(this /*owner*/));
    }

    Persist(context, DataWeightCounter_);
    Persist(context, RowCounter_);
    Persist(context, JobCounter_);
    Persist(context, DataSliceCounter_);
    Persist(context, InputCookieToAffectedOutputCookies_);
    Persist(context, FirstValidJobIndex_);
    Persist(context, SuspendedInputCookies_);
    Persist(context, Jobs_);
    Persist(context, Logger);
}

TChunkStripeStatisticsVector TLegacyJobManager::GetApproximateStripeStatistics() const
{
    if (CookiePool_->size() == 0) {
        return TChunkStripeStatisticsVector();
    }
    auto cookie = *(CookiePool_->begin());
    const auto& job = Jobs_[cookie];
    return job.StripeList()->GetStatistics();
}

const TChunkStripeListPtr& TLegacyJobManager::GetStripeList(IChunkPoolOutput::TCookie cookie)
{
    YT_VERIFY(cookie < std::ssize(Jobs_));
    const auto& job = Jobs_[cookie];
    return job.StripeList();
}

void TLegacyJobManager::InvalidateAllJobs()
{
    while (FirstValidJobIndex_ < std::ssize(Jobs_)) {
        auto& job = Jobs_[FirstValidJobIndex_];
        if (!job.IsInvalidated()) {
            job.Invalidate();
        }
        FirstValidJobIndex_++;
    }
}

void TLegacyJobManager::Enlarge(i64 dataWeightPerJob, i64 primaryDataWeightPerJob)
{
    // TODO(max42): keep the order of jobs in a singly linked list that allows us to use this
    // procedure not only during the initial creation of jobs or right after the whole pool invalidation,
    // but at the arbitrary moment of job manager lifetime. After that implement YT-9019.

    YT_LOG_DEBUG("Enlarging jobs (DataWeightPerJob: %v, PrimaryDataWeightPerJob: %v)",
        dataWeightPerJob,
        primaryDataWeightPerJob);

    std::unique_ptr<TLegacyJobStub> currentJobStub;
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

    std::vector<std::unique_ptr<TLegacyJobStub>> newJobs;
    for (int startIndex = FirstValidJobIndex_, finishIndex = FirstValidJobIndex_; startIndex < std::ssize(Jobs_); startIndex = finishIndex) {
        if (Jobs_[startIndex].GetIsBarrier()) {
            // NB: One may think that we should carefully bring this barrier between newly formed jobs but we
            // currently never enlarge jobs after building them from scratch, so barriers have no use after enlarging.
            // But when we store jobs in a singly linked list, we should deal with barriers carefully!
            finishIndex = startIndex + 1;
            continue;
        }

        currentJobStub = std::make_unique<TLegacyJobStub>();
        while (true) {
            if (finishIndex == std::ssize(Jobs_)) {
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

            if (!tryJoinJob(finishIndex, finishIndex == startIndex /*force*/)) {
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
                auto& job = Jobs_[index];
                job.Remove();
            }
            currentJobStub->Finalize(false /*sortByPosition*/);
            newJobs.emplace_back(std::move(currentJobStub));
        } else {
            YT_LOG_DEBUG("Leaving job as is (Cookie: %v)", startIndex);
        }
        joinedJobCookies.clear();
    }

    AddJobs(std::move(newJobs));
}

std::pair<TKeyBound, TKeyBound> TLegacyJobManager::GetBounds(IChunkPoolOutput::TCookie /*cookie*/) const
{
    // We drop support for this method in legacy pool as it is used only in CHYT which already uses new job manager.
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
