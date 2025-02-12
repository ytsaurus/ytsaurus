#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>
#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkPools {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NLogging;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TNewJobStub::AddDataSlice(const TLegacyDataSlicePtr& dataSlice, IChunkPoolInput::TCookie cookie, bool isPrimary)
{
    YT_VERIFY(!dataSlice->IsLegacy);

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
        ++PrimarySliceCount_;
        PrimaryDataWeight_ += dataSlice->GetDataWeight();
        PrimaryRowCount_ += dataSlice->GetRowCount();
    } else {
        ++ForeignSliceCount_;
        ForeignDataWeight_ += dataSlice->GetDataWeight();
        ForeignRowCount_ += dataSlice->GetRowCount();
    }
}

void TNewJobStub::AddPreliminaryForeignDataSlice(const TLegacyDataSlicePtr& dataSlice)
{
    PreliminaryForeignDataWeight_ += dataSlice->GetDataWeight();
    PreliminaryForeignRowCount_ += dataSlice->GetRowCount();
    ++PreliminaryForeignSliceCount_;
}

void TNewJobStub::Finalize()
{
    for (const auto& [tableAndRangeIndex, stripe] : StripeMap_) {
        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);
        }
        const auto& statistics = stripe->GetStatistics();
        StripeList_->TotalDataWeight += statistics.DataWeight;
        StripeList_->TotalRowCount += statistics.RowCount;
        StripeList_->TotalChunkCount += statistics.ChunkCount;
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

    //    Ideally, there should be no need in such sorting.
    //
    //    for (const auto& stripe : StripeList_->Stripes) {
    //        std::stable_sort(stripe->DataSlices.begin(), stripe->DataSlices.end(), [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
    //            if (lhs->Tag && rhs->Tag && *lhs->Tag != *rhs->Tag) {
    //                return *lhs->Tag < *rhs->Tag;
    //            }
    //            return lhs->GetSliceIndex() < rhs->GetSliceIndex();
    //        });
//    }
}

i64 TNewJobStub::GetDataWeight() const
{
    return PrimaryDataWeight_ + ForeignDataWeight_;
}

i64 TNewJobStub::GetRowCount() const
{
    return PrimaryRowCount_ + ForeignRowCount_;
}

int TNewJobStub::GetSliceCount() const
{
    return PrimarySliceCount_ + ForeignSliceCount_;
}

i64 TNewJobStub::GetPreliminaryDataWeight() const
{
    return PrimaryDataWeight_ + PreliminaryForeignDataWeight_;
}

i64 TNewJobStub::GetPreliminaryRowCount() const
{
    return PrimaryRowCount_ + PreliminaryForeignRowCount_;
}

int TNewJobStub::GetPreliminarySliceCount() const
{
    return PrimarySliceCount_ + PreliminaryForeignSliceCount_;
}

TString TNewJobStub::GetDebugString() const
{
    TStringBuilder builder;
    builder.AppendString("{");
    bool isFirst = true;
    for (const auto& [key, stripe] : StripeMap_) {
        builder.AppendFormat("(%v, %v): ", key.first, key.second);
        for (const auto& dataSlice : stripe->DataSlices) {
            if (isFirst) {
                isFirst = false;
            } else {
                builder.AppendString(", ");
            }
            std::vector<TChunkId> chunkIds;
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                chunkIds.push_back(chunkSlice->GetInputChunk()->GetChunkId());
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

const TChunkStripePtr& TNewJobStub::GetStripe(int streamIndex, int rangeIndex, bool isStripePrimary)
{
    auto& stripe = StripeMap_[std::pair(streamIndex, rangeIndex)];
    if (!stripe) {
        stripe = New<TChunkStripe>(!isStripePrimary /*foreign*/);
    }
    return stripe;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TNewJobStub& jobStub, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("primary_lower_bound").Value(jobStub.GetPrimaryLowerBound())
            .Item("primary_upper_bound").Value(jobStub.GetPrimaryUpperBound())
            .Item("primary_slice_count").Value(jobStub.GetPrimarySliceCount())
            .Item("foreign_slice_count").Value(jobStub.GetForeignSliceCount())
            .Item("primary_row_count").Value(jobStub.GetPrimaryRowCount())
            .Item("foreign_row_count").Value(jobStub.GetForeignRowCount())
            .Item("primary_data_weight").Value(jobStub.GetPrimaryDataWeight())
            .Item("foreign_data_weight").Value(jobStub.GetForeignDataWeight())
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TNewJobManager::TJob::TJob(TNewJobManager* owner, std::unique_ptr<TNewJobStub> jobStub, IChunkPoolOutput::TCookie cookie)
    : IsBarrier_(jobStub->GetIsBarrier())
    , DataWeight_(jobStub->GetDataWeight())
    , RowCount_(jobStub->GetRowCount())
    , LowerBound_(jobStub->GetPrimaryLowerBound())
    , UpperBound_(jobStub->GetPrimaryUpperBound())
    , StripeList_(std::move(jobStub->StripeList_))
    , InputCookies_(std::move(jobStub->InputCookies_))
    , Owner_(owner)
    , CookiePoolIterator_(Owner_->CookiePool_->end())
    , Cookie_(cookie)
    , DataWeightProgressCounterGuard_(owner->DataWeightCounter(), jobStub->GetDataWeight())
    , RowProgressCounterGuard_(owner->RowCounter(), jobStub->GetRowCount())
    , JobProgressCounterGuard_(owner->JobCounter())
{ }

void TNewJobManager::TJob::SetState(EJobState state)
{
    State_ = state;
    UpdateSelf();
}

void TNewJobManager::TJob::SetInterruptionReason(NScheduler::EInterruptionReason reason)
{
    InterruptionReason_ = reason;
}

void TNewJobManager::TJob::ChangeSuspendedStripeCountBy(int delta)
{
    SuspendedStripeCount_ += delta;
    YT_VERIFY(SuspendedStripeCount_ >= 0);
    UpdateSelf();
}

void TNewJobManager::TJob::Invalidate()
{
    YT_VERIFY(!Invalidated_);
    Invalidated_ = true;
    StripeList_.Reset();
    UpdateSelf();
}

void TNewJobManager::TJob::Remove()
{
    YT_VERIFY(!Removed_);
    Removed_ = true;
    StripeList_.Reset();
    UpdateSelf();
}

bool TNewJobManager::TJob::IsInvalidated() const
{
    return Invalidated_;
}

void TNewJobManager::TJob::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, State_);
    PHOENIX_REGISTER_FIELD(2, IsBarrier_);
    PHOENIX_REGISTER_FIELD(3, DataWeight_);
    PHOENIX_REGISTER_FIELD(4, RowCount_);
    PHOENIX_REGISTER_FIELD(5, LowerBound_);
    PHOENIX_REGISTER_FIELD(6, UpperBound_);
    PHOENIX_REGISTER_FIELD(7, StripeList_);
    PHOENIX_REGISTER_FIELD(8, InputCookies_);
    PHOENIX_REGISTER_FIELD(9, Owner_);
    PHOENIX_REGISTER_FIELD(10, SuspendedStripeCount_);
    PHOENIX_REGISTER_FIELD(11, Cookie_);
    PHOENIX_REGISTER_FIELD(12, Invalidated_);
    PHOENIX_REGISTER_FIELD(13, Removed_);
    PHOENIX_REGISTER_FIELD(14, DataWeightProgressCounterGuard_);
    PHOENIX_REGISTER_FIELD(15, RowProgressCounterGuard_);
    PHOENIX_REGISTER_FIELD(16, JobProgressCounterGuard_);
    PHOENIX_REGISTER_FIELD(17, InterruptionReason_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        // We must add ourselves to the job pool.
        this_->CookiePoolIterator_ = this_->Owner_->CookiePool_->end();
        this_->UpdateSelf();
    });
}

void TNewJobManager::TJob::UpdateSelf()
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
        CallProgressCounterGuards(&TProgressCounterGuard::SetCompletedCategory, InterruptionReason_);
    } else {
        CallProgressCounterGuards(&TProgressCounterGuard::SetCategory, newProgressCategory);
    }
}

void TNewJobManager::TJob::RemoveSelf()
{
    YT_VERIFY(CookiePoolIterator_ != Owner_->CookiePool_->end());
    Owner_->CookiePool_->erase(CookiePoolIterator_);
    CookiePoolIterator_ = Owner_->CookiePool_->end();
    InPool_ = false;
}

void TNewJobManager::TJob::AddSelf()
{
    YT_VERIFY(CookiePoolIterator_ == Owner_->CookiePool_->end());
    CookiePoolIterator_ = Owner_->CookiePool_->insert(Owner_->CookiePool_->end(), Cookie_);
    InPool_ = true;
}

void TNewJobManager::TJob::SuspendSelf()
{
    YT_VERIFY(!Suspended_);
    Suspended_ = true;
}

void TNewJobManager::TJob::ResumeSelf()
{
    YT_VERIFY(Suspended_);
    Suspended_ = false;
}

template <class... TArgs>
void TNewJobManager::TJob::CallProgressCounterGuards(void (TProgressCounterGuard::*Method)(TArgs...), TArgs... args)
{
    (DataWeightProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
    (RowProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
    (JobProgressCounterGuard_.*Method)(std::forward<TArgs>(args)...);
}

PHOENIX_DEFINE_TYPE(TNewJobManager::TJob);

////////////////////////////////////////////////////////////////////////////////

TNewJobManager::TStripeListComparator::TStripeListComparator(TNewJobManager* owner)
    : Owner_(owner)
{ }

bool TNewJobManager::TStripeListComparator::operator()(IChunkPoolOutput::TCookie lhs, IChunkPoolOutput::TCookie rhs) const
{
    const auto& lhsJob = Owner_->Jobs_[lhs];
    const auto& rhsJob = Owner_->Jobs_[rhs];
    if (lhsJob.GetDataWeight() != rhsJob.GetDataWeight()) {
        return lhsJob.GetDataWeight() > rhsJob.GetDataWeight();
    }
    return lhs < rhs;
}

////////////////////////////////////////////////////////////////////////////////

TNewJobManager::TNewJobManager()
    : CookiePool_(std::make_unique<TCookiePool>(TNewJobManager::TStripeListComparator(this /*owner*/)))
{ }

TNewJobManager::TNewJobManager(const NLogging::TLogger& logger)
    : CookiePool_(std::make_unique<TCookiePool>(TNewJobManager::TStripeListComparator(this /*owner*/)))
    , Logger(logger)
{ }

std::vector<IChunkPoolOutput::TCookie> TNewJobManager::AddJobs(std::vector<std::unique_ptr<TNewJobStub>> jobStubs)
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
IChunkPoolOutput::TCookie TNewJobManager::AddJob(std::unique_ptr<TNewJobStub> jobStub)
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
        jobStub->GetPrimaryLowerBound(),
        jobStub->GetPrimaryUpperBound());

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

void TNewJobManager::Completed(IChunkPoolOutput::TCookie cookie, EInterruptionReason reason)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.SetInterruptionReason(reason);
    job.SetState(EJobState::Completed);
}

IChunkPoolOutput::TCookie TNewJobManager::ExtractCookie()
{
    if (CookiePool_->empty()) {
        return IChunkPoolInput::NullCookie;
    }

    auto cookie = *CookiePool_->begin();

    DoExtractCookie(cookie);

    return cookie;
}

void TNewJobManager::ExtractCookie(IChunkPoolOutput::TCookie cookie)
{
    YT_VERIFY(CookiePool_->contains(cookie));
    DoExtractCookie(cookie);
}

void TNewJobManager::DoExtractCookie(IChunkPoolOutput::TCookie cookie)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(!job.GetIsBarrier());
    YT_VERIFY(job.GetState() == EJobState::Pending);

    job.SetState(EJobState::Running);
}

void TNewJobManager::Failed(IChunkPoolOutput::TCookie cookie)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnFailed);
    job.SetState(EJobState::Pending);
}

void TNewJobManager::Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason)
{
    auto& job = Jobs_[cookie];
    YT_VERIFY(job.GetState() == EJobState::Running);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnAborted, reason);
    job.SetState(EJobState::Pending);
}

void TNewJobManager::Lost(IChunkPoolOutput::TCookie cookie, bool force)
{
    auto& job = Jobs_[cookie];

    if (!force && job.GetState() != EJobState::Completed) {
        return;
    }

    YT_VERIFY(job.GetState() == EJobState::Completed);
    job.CallProgressCounterGuards(&TProgressCounterGuard::OnLost);
    job.SetState(EJobState::Pending);
}

void TNewJobManager::Suspend(IChunkPoolInput::TCookie inputCookie)
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

void TNewJobManager::Resume(IChunkPoolInput::TCookie inputCookie)
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

void TNewJobManager::Invalidate(IChunkPoolInput::TCookie inputCookie)
{
    YT_VERIFY(0 <= inputCookie && inputCookie < std::ssize(Jobs_));
    auto& job = Jobs_[inputCookie];
    job.Invalidate();
}

std::vector<TLegacyDataSlicePtr> TNewJobManager::ReleaseForeignSlices(IChunkPoolInput::TCookie inputCookie)
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

void TNewJobManager::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, DataWeightCounter_);
    PHOENIX_REGISTER_FIELD(2, RowCounter_);
    PHOENIX_REGISTER_FIELD(3, JobCounter_);
    PHOENIX_REGISTER_FIELD(4, DataSliceCounter_);
    PHOENIX_REGISTER_FIELD(5, InputCookieToAffectedOutputCookies_);
    PHOENIX_REGISTER_FIELD(6, FirstValidJobIndex_);
    PHOENIX_REGISTER_FIELD(7, SuspendedInputCookies_);
    PHOENIX_REGISTER_FIELD(8, Jobs_);
    PHOENIX_REGISTER_FIELD(9, Logger);
}

TChunkStripeStatisticsVector TNewJobManager::GetApproximateStripeStatistics() const
{
    if (CookiePool_->size() == 0) {
        return TChunkStripeStatisticsVector();
    }
    auto cookie = *(CookiePool_->begin());
    const auto& job = Jobs_[cookie];
    return job.StripeList()->GetStatistics();
}

const TChunkStripeListPtr& TNewJobManager::GetStripeList(IChunkPoolOutput::TCookie cookie)
{
    YT_VERIFY(cookie < std::ssize(Jobs_));
    const auto& job = Jobs_[cookie];
    return job.StripeList();
}

void TNewJobManager::InvalidateAllJobs()
{
    while (FirstValidJobIndex_ < std::ssize(Jobs_)) {
        auto& job = Jobs_[FirstValidJobIndex_];
        if (!job.IsInvalidated()) {
            job.Invalidate();
        }
        FirstValidJobIndex_++;
    }
}

void TNewJobManager::Enlarge(i64 dataWeightPerJob, i64 primaryDataWeightPerJob)
{
    // TODO(max42): keep the order of jobs in a singly linked list that allows us to use this
    // procedure not only during the initial creation of jobs or right after the whole pool invalidation,
    // but at the arbitrary moment of job manager lifetime. After that implement YT-9019.

    YT_LOG_DEBUG("Enlarging jobs (DataWeightPerJob: %v, PrimaryDataWeightPerJob: %v)",
        dataWeightPerJob,
        primaryDataWeightPerJob);

    std::unique_ptr<TNewJobStub> currentJobStub;
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

    std::vector<std::unique_ptr<TNewJobStub>> newJobs;
    for (int startIndex = FirstValidJobIndex_, finishIndex = FirstValidJobIndex_; startIndex < std::ssize(Jobs_); startIndex = finishIndex) {
        if (Jobs_[startIndex].GetIsBarrier()) {
            // NB: One may think that we should carefully bring this barrier between newly formed jobs but we
            // currently never enlarge jobs after building them from scratch, so barriers have no use after enlarging.
            // But when we store jobs in a singly linked list, we should deal with barriers carefully!
            finishIndex = startIndex + 1;
            continue;
        }

        currentJobStub = std::make_unique<TNewJobStub>();
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
            currentJobStub->Finalize();
            newJobs.emplace_back(std::move(currentJobStub));
        } else {
            YT_LOG_DEBUG("Leaving job as is (Cookie: %v)", startIndex);
        }
        joinedJobCookies.clear();
    }

    AddJobs(std::move(newJobs));
}

std::pair<TKeyBound, TKeyBound> TNewJobManager::GetBounds(IChunkPoolOutput::TCookie cookie) const
{
    YT_VERIFY(cookie >= 0);
    YT_VERIFY(cookie < std::ssize(Jobs_));
    const auto& job = Jobs_[cookie];
    return {job.GetLowerBound(), job.GetUpperBound()};
}

PHOENIX_DEFINE_TYPE(TNewJobManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
