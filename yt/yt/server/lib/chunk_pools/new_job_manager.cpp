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
    stripe->DataSlices.push_back(dataSlice);
    if (cookie != IChunkPoolInput::NullCookie) {
        InputCookies_.emplace_back(cookie);
    }

    if (isPrimary) {
        ++PrimarySliceCount_;
        PrimaryDataWeight_ += dataSlice->GetDataWeight();
        PrimaryRowCount_ += dataSlice->GetRowCount();
        PrimaryCompressedDataSize_ += dataSlice->GetCompressedDataSize();
    } else {
        ++ForeignSliceCount_;
        ForeignDataWeight_ += dataSlice->GetDataWeight();
        ForeignRowCount_ += dataSlice->GetRowCount();
        ForeignCompressedDataSize_ += dataSlice->GetCompressedDataSize();
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
        StripeList_->TotalCompressedDataSize += statistics.CompressedDataSize;
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

i64 TNewJobStub::GetCompressedDataSize() const
{
    return PrimaryCompressedDataSize_ + ForeignCompressedDataSize_;
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
            .Item("primary_compressed_data_size").Value(jobStub.GetPrimaryCompressedDataSize())
            .Item("foreign_compressed_data_size").Value(jobStub.GetForeignCompressedDataSize())
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

void TNewJobManager::TJob::Revalidate()
{
    YT_VERIFY(Invalidated_);
    // NB: Only for vanilla jobs.
    YT_VERIFY(!StripeList_);
    StripeList_ = New<TChunkStripeList>();
    Invalidated_ = false;
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
    if (InPool() && !inPoolDesired) {
        RemoveSelf();
    } else if (!InPool() && inPoolDesired) {
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
    YT_VERIFY(InPool());
    Owner_->CookiePool_->erase(CookiePoolIterator_);
    CookiePoolIterator_ = Owner_->CookiePool_->end();
}

void TNewJobManager::TJob::AddSelf()
{
    YT_VERIFY(!InPool());
    CookiePoolIterator_ = Owner_->CookiePool_->insert(Owner_->CookiePool_->end(), Cookie_);
}

bool TNewJobManager::TJob::InPool() const
{
    return CookiePoolIterator_ != Owner_->CookiePool_->end();
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

class TNewJobManager::TJobOrder
{
public:
    //! Position of first cookie.
    DEFINE_BYVAL_RO_PROPERTY(std::optional<int>, FirstCookie);

    //! Subsequent pushes will insert new jobs after |cookie|.
    void Seek(TOutputCookie cookie)
    {
        YT_VERIFY(0 <= cookie && cookie < std::ssize(Neighbors_));
        Current_ = cookie;
    }

    //! Insert job with index |cookie| after current.
    void Push(TOutputCookie cookie)
    {
        EnsureVectorIndex(Neighbors_, cookie);

        auto next = Current_ ? Neighbors_[*Current_].Next : std::nullopt;
        auto prev = Current_;

        if (next) {
            Neighbors_[*next].Prev = cookie;
        }
        Neighbors_[cookie].Next = next;

        if (prev) {
            Neighbors_[*prev].Next = cookie;
        } else {
            YT_VERIFY(!FirstCookie_);
            FirstCookie_ = cookie;
        }
        Neighbors_[cookie].Prev = prev;

        Current_ = cookie;
    }

    //! Removes current job and returns its cookie.
    //! The next job will become current.
    TOutputCookie Remove()
    {
        YT_VERIFY(Current_);
        auto next = Neighbors_[*Current_].Next;
        auto prev = Neighbors_[*Current_].Prev;

        if (prev) {
            Neighbors_[*prev].Next = next;
        }
        if (next) {
            Neighbors_[*next].Prev = prev;
        }

        // Just for sanity.
        Neighbors_[*Current_].Next = std::nullopt;
        Neighbors_[*Current_].Prev = std::nullopt;

        YT_VERIFY(FirstCookie_);
        if (*Current_ == *FirstCookie_) {
            FirstCookie_ = next;
        }

        auto oldCurrent = *Current_;
        Current_ = next;
        return oldCurrent;
    }

    //! Forgets all jobs. Useful for |InvalidateAllJobs|.
    void Reset()
    {
        Neighbors_.clear();
        Current_ = std::nullopt;
        FirstCookie_ = std::nullopt;
    }

    std::optional<TOutputCookie> Next(TOutputCookie cookie) const
    {
        YT_VERIFY(0 <= cookie && cookie < std::ssize(Neighbors_));
        return Neighbors_[cookie].Next;
    }

    std::optional<TOutputCookie> Prev(TOutputCookie cookie) const
    {
        YT_VERIFY(0 <= cookie && cookie < std::ssize(Neighbors_));
        return Neighbors_[cookie].Prev;
    }

    // COMPAT(coteeq)
    void InitializeCompat(int jobCount)
    {
        // Form a bunch of disjoint cycles among jobs.
        // This is fine as long as we do not enlarge jobs at runtime
        // (which we shouldn't do for compatted operations).
        EnsureVectorSize(Neighbors_, jobCount);
    }

private:
    struct TNeighbors
    {
        std::optional<TOutputCookie> Next;
        std::optional<TOutputCookie> Prev;

        PHOENIX_DECLARE_TYPE(TNeighbors, 0xe3fecf76);
    };

    // Maps cookie to its neighbors.
    std::vector<TNeighbors> Neighbors_;

    std::optional<int> Current_;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TJobOrder, 0xe7c3d856);
};

void TNewJobManager::TJobOrder::TNeighbors::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Next);
    PHOENIX_REGISTER_FIELD(2, Prev);
}

PHOENIX_DEFINE_TYPE(TNewJobManager::TJobOrder::TNeighbors);

void TNewJobManager::TJobOrder::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, FirstCookie_);
    PHOENIX_REGISTER_FIELD(2, Neighbors_);
    PHOENIX_REGISTER_FIELD(3, Current_);
}

PHOENIX_DEFINE_TYPE(TNewJobManager::TJobOrder);

////////////////////////////////////////////////////////////////////////////////

TNewJobManager::TNewJobManager()
    : CookiePool_(std::make_unique<TCookiePool>(TNewJobManager::TStripeListComparator(this /*owner*/)))
    , JobOrder_(std::make_unique<TJobOrder>())
{ }

TNewJobManager::TNewJobManager(const NLogging::TLogger& logger)
    : CookiePool_(std::make_unique<TCookiePool>(TNewJobManager::TStripeListComparator(this /*owner*/)))
    , JobOrder_(std::make_unique<TJobOrder>())
    , Logger(logger)
{ }

TNewJobManager::~TNewJobManager() = default;

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
    YT_VERIFY(ValidJobCount_ == std::ssize(Jobs_));
    ++ValidJobCount_;

    JobOrder_->Push(outputCookie);

    if (jobStub->GetIsBarrier()) {
        YT_LOG_DEBUG("Adding barrier to job manager (Index: %v)", outputCookie);
        Jobs_.emplace_back(this, std::move(jobStub), outputCookie);
        Jobs_.back().SetState(EJobState::Completed);
        // TODO(max42): do not assign cookie to barriers.
        return outputCookie;
    }

    YT_LOG_DEBUG(
        "Job added to job manager (Index: %v, PrimaryDataWeight: %v, PrimaryCompressedDataSize: %v, "
        "PrimaryRowCount: %v, PrimarySliceCount: %v, ForeignDataWeight: %v, ForeignCompressedDataSize: %v, "
        "ForeignRowCount: %v, ForeignSliceCount: %v, LowerPrimaryKey: %v, UpperPrimaryKey: %v)",
        outputCookie,
        jobStub->GetPrimaryDataWeight(),
        jobStub->GetPrimaryCompressedDataSize(),
        jobStub->GetPrimaryRowCount(),
        jobStub->GetPrimarySliceCount(),
        jobStub->GetForeignDataWeight(),
        jobStub->GetForeignCompressedDataSize(),
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

std::vector<IChunkPoolOutput::TCookie> TNewJobManager::SetJobCount(int desiredJobCount)
{
    if (desiredJobCount < ValidJobCount_) {
        return DecreaseJobCount(ValidJobCount_ - desiredJobCount);
    }
    if (desiredJobCount > ValidJobCount_) {
        IncreaseJobCount(desiredJobCount - ValidJobCount_);
        return {};
    }
    return {};
}

void TNewJobManager::IncreaseJobCount(int delta)
{
    YT_VERIFY(delta > 0);
    int changedJobCount = 0;
    for (int index = 0; index < std::ssize(Jobs_) && changedJobCount < delta; ++index) {
        if (Jobs_[index].IsInvalidated()) {
            Jobs_[index].Revalidate();
            ++changedJobCount;
            ++ValidJobCount_;
        }
    }
    while (changedJobCount < delta) {
        AddJob(std::make_unique<TNewJobStub>());
        ++changedJobCount;
    }
}

std::vector<IChunkPoolOutput::TCookie> TNewJobManager::DecreaseJobCount(int delta)
{
    YT_VERIFY(delta > 0);
    std::vector<IChunkPoolOutput::TCookie> cookies;
    int changedJobCount = 0;
    cookies.reserve(delta);

    // NB: Invalidate jobs from the back to:
    // 1. Try to keep cookies in [0, job_count) range.
    // 2. Try to abort youngest jobs if possible.
    for (int index = std::ssize(Jobs_) - 1; index >= 0 && changedJobCount < delta; --index) {
        auto& job = Jobs_[index];
        if (!job.IsInvalidated() && job.GetState() != EJobState::Completed) {
            if (job.GetState() == EJobState::Running) {
                cookies.push_back(index);
            }
            job.Invalidate();
            --ValidJobCount_;
            ++changedJobCount;
        }
    }

    // NB: It is okay if cookies.size() < delta. We will abort everything we can and
    // operation will be completed automatically.

    return cookies;
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
    PHOENIX_REGISTER_FIELD(10, ValidJobCount_,
        .SinceVersion(ESnapshotVersion::DynamicVanillaJobCount)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->ValidJobCount_ = std::ssize(this_->Jobs_);
        }));

    PHOENIX_REGISTER_FIELD(11, JobOrder_,
        .SinceVersion(ESnapshotVersion::OrderedAndSortedJobSizeAdjuster)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->JobOrder_ = std::make_unique<TJobOrder>();
            this_->JobOrder_->InitializeCompat(this_->Jobs_.size());
        }));
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
    JobOrder_->Reset();
}

void TNewJobManager::Enlarge(
    i64 dataWeightPerJob,
    i64 primaryDataWeightPerJob,
    const IJobSizeConstraintsPtr& jobSizeConstraints)
{
    YT_LOG_DEBUG("Enlarging jobs (DataWeightPerJob: %v, PrimaryDataWeightPerJob: %v)",
        dataWeightPerJob,
        primaryDataWeightPerJob);

    auto shouldJoinJob = [&] (const TNewJobStub* currentJobStub, const TJob& job, bool force) -> bool {
        i64 primaryDataWeight = currentJobStub->GetPrimaryDataWeight();
        i64 foreignDataWeight = currentJobStub->GetForeignDataWeight();
        i64 compressedDataSize = currentJobStub->GetCompressedDataSize();
        i64 sliceCount = currentJobStub->GetSliceCount();
        for (const auto& stripe : job.StripeList()->Stripes) {
            if (!force && stripe->Foreign) {
                YT_LOG_DEBUG("Stopping enlargement due to the foreign data stripe");
                return false;
            }
            for (const auto& dataSlice : stripe->DataSlices) {
                (stripe->Foreign ? foreignDataWeight : primaryDataWeight) += dataSlice->GetDataWeight();
                compressedDataSize += dataSlice->GetCompressedDataSize();
                ++sliceCount;
            }
        }

        bool primaryDataWeightFits = primaryDataWeight <= primaryDataWeightPerJob;
        bool totalDataWeightFits = foreignDataWeight + primaryDataWeight <= dataWeightPerJob &&
            foreignDataWeight + primaryDataWeight <= jobSizeConstraints->GetMaxDataWeightPerJob();
        bool compressedDataSizeFits = compressedDataSize <= jobSizeConstraints->GetMaxCompressedDataSizePerJob();
        bool sliceCountFits = sliceCount <= jobSizeConstraints->GetMaxDataSlicesPerJob();
        bool fits = primaryDataWeightFits && totalDataWeightFits && compressedDataSizeFits && sliceCountFits;

        if (fits || force) {
            return true;
        }
        YT_LOG_DEBUG("Stopping enlargement due to size constraints "
            "(NewSize: {DataWeight: %v, PrimaryDataWeight: %v, CompressedDataSize: %v, SliceCount: %v}, "
            "NewConstraints: {DataWeightPerJob: %v, PrimaryDataWeightPerJob: %v, MaxDataWeightPerJob: %v, "
            "MaxCompressedDataSizePerJob: %v, MaxDataSlicesPerJob: %v})",
            foreignDataWeight + primaryDataWeight,
            primaryDataWeight,
            compressedDataSize,
            sliceCount,
            dataWeightPerJob,
            primaryDataWeightPerJob,
            jobSizeConstraints->GetMaxDataWeightPerJob(),
            jobSizeConstraints->GetMaxCompressedDataSizePerJob(),
            jobSizeConstraints->GetMaxDataSlicesPerJob());
        return false;
    };

    auto isJobJoinable = [&] (int index) {
        const auto& job = Jobs_[index];
        // NB(coteeq): We do not want to join running or completed jobs.
        return job.GetIsBarrier() || job.GetState() != EJobState::Pending;
    };

    for (
        auto startIndex = JobOrder_->GetFirstCookie(), finishIndex = JobOrder_->GetFirstCookie();
        startIndex.has_value();
        startIndex = finishIndex)
    {
        if (isJobJoinable(*startIndex)) {
            // There is no job to flush, so just skip non-joinable job.
            finishIndex = JobOrder_->Next(*startIndex);
            continue;
        }

        std::unique_ptr<TNewJobStub> currentJobStub = std::make_unique<TNewJobStub>();
        std::vector<IChunkPoolOutput::TCookie> joinedJobCookies;

        while (true) {
            if (!finishIndex) {
                YT_LOG_DEBUG(
                    "Stopping enlargement due to end of job list (StartIndex: %v, FinishIndex: %v)",
                    startIndex,
                    finishIndex);
                break;
            }

            YT_VERIFY(!Jobs_[*finishIndex].IsInvalidated());

            if (isJobJoinable(*finishIndex)) {
                YT_LOG_DEBUG(
                    "Stopping enlargement due to non-joinable job (StartIndex: %v, FinishIndex: %v)",
                    startIndex,
                    finishIndex);
                break;
            }

            const auto& job = Jobs_[*finishIndex];
            if (shouldJoinJob(currentJobStub.get(), job, finishIndex == startIndex /*force*/)) {
                for (const auto& stripe : job.StripeList()->Stripes) {
                    for (const auto& dataSlice : stripe->DataSlices) {
                        // TODO(coteeq): Do not duplicate rows in foreign slices here.
                        // For primary slices everything is simple - we just throw more slices to
                        // the currentJobStub and it works because everything is ordered.
                        // For foreign slices that does not work in the following case:
                        // 1. enable_key_guarantee=%false
                        // 2. We have two separate jobs, both containing primary rows with key="asdf"
                        // 3. We have a foreign table containing row with key="asdf"
                        // Then both jobs have to contain foreign row with key="asdf" and it's
                        // exactly where "join all slices" logic fails, because we are going to duplicate
                        // foreign row with key="asdf".
                        // It is probably possible to carefully examine foreign slices and deduplicate
                        // rows, but does not seem to be easy :/
                        //
                        // See also YT-25074.
                        currentJobStub->AddDataSlice(dataSlice, IChunkPoolInput::NullCookie, !stripe->Foreign);
                    }
                }
                currentJobStub->InputCookies_.insert(currentJobStub->InputCookies_.end(), job.InputCookies().begin(), job.InputCookies().end());
                joinedJobCookies.emplace_back(*finishIndex);

            } else {
                // This case is logged in shouldJoinJob.
                break;
            }

            finishIndex = JobOrder_->Next(*finishIndex);
        }

        if (joinedJobCookies.size() > 1) {
            std::vector<IChunkPoolOutput::TCookie> outputCookies;
            YT_LOG_DEBUG("Joining together jobs (JoinedJobCookies: %v, DataWeight: %v, PrimaryDataWeight: %v)",
                MakeCompactIntervalView(joinedJobCookies),
                currentJobStub->GetDataWeight(),
                currentJobStub->GetPrimaryDataWeight());

            currentJobStub->Finalize();
            JobOrder_->Seek(joinedJobCookies.back());
            AddJob(std::move(currentJobStub));

            JobOrder_->Seek(*startIndex);
            for (int index : joinedJobCookies) {
                auto& job = Jobs_[index];
                job.Remove();
                YT_VERIFY(JobOrder_->Remove() == index);
            }
        } else {
            YT_LOG_DEBUG("Leaving job as is (Cookie: %v)", startIndex);
        }
    }
}

void TNewJobManager::SeekOrder(TOutputCookie cookie)
{
    JobOrder_->Seek(cookie);
}

std::vector<int> TNewJobManager::GetCookieToPosition() const
{
    int position = 0;
    std::vector<int> cookieToPosition;
    cookieToPosition.resize(Jobs_.size(), -1);
    for (auto cookie = JobOrder_->GetFirstCookie(); cookie; cookie = JobOrder_->Next(*cookie)) {
        YT_VERIFY(cookieToPosition[*cookie] == -1);
        cookieToPosition[*cookie] = position++;
    }

    return cookieToPosition;
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
