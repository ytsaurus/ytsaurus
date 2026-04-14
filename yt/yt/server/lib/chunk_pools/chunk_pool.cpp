#include "chunk_pool.h"

#include "legacy_job_manager.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/statistics.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_TYPE(IPersistentChunkPoolInput);

////////////////////////////////////////////////////////////////////////////////

void TChunkPoolInputBase::Finish()
{
    Finished = true;
}

bool TChunkPoolInputBase::IsFinished() const
{
    return Finished;
}

IChunkPoolInput::TCookie TChunkPoolInputBase::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    // `key` argument should be set to something non-trivial only for sink chunk pool inputs,
    // so for all classes that are inherited from this `key` should never be set.
    YT_VERIFY(!key);
    // Stripes may either contain several data slices or consist only of a single chunk tree id.
    // All classes that are inherited from this base are dealing with explicit chunk representations,
    // so they are not ready to work with stripes that do not contain data slices.
    YT_VERIFY(!stripe->DataSlices().empty());

    return Add(stripe);
}

void TChunkPoolInputBase::Reset(TCookie /*cookie*/, TChunkStripePtr /*stripe*/, TInputChunkMappingPtr /*mapping*/)
{
    YT_ABORT();
}

void TChunkPoolInputBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<IPersistentChunkPoolInput>();
    PHOENIX_REGISTER_FIELD(1, Finished);
}

PHOENIX_DEFINE_TYPE(TChunkPoolInputBase);

////////////////////////////////////////////////////////////////////////////////

i64 TChunkPoolOutputBase::GetLocality(NNodeTrackerClient::TNodeId /*nodeId*/) const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

TChunkPoolOutputWithCountersBase::TChunkPoolOutputWithCountersBase()
    : DataWeightCounter_(New<TProgressCounter>())
    , RowCounter_(New<TProgressCounter>())
    , JobCounter_(New<TProgressCounter>())
    , DataSliceCounter_(New<TProgressCounter>())
{ }

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetJobCounter() const
{
    return JobCounter_;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetDataWeightCounter() const
{
    return DataWeightCounter_;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetRowCounter() const
{
    return RowCounter_;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetDataSliceCounter() const
{
    return DataSliceCounter_;
}

void TChunkPoolOutputWithCountersBase::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobCounter_);
    PHOENIX_REGISTER_FIELD(2, DataWeightCounter_);
    PHOENIX_REGISTER_FIELD(3, RowCounter_);
    PHOENIX_REGISTER_FIELD(4, DataSliceCounter_);
}

PHOENIX_DEFINE_TYPE(TChunkPoolOutputWithCountersBase);

////////////////////////////////////////////////////////////////////////////////

// IPersistentChunkPoolOutput implementation.

template <class TJobManager>
TChunkPoolOutputWithJobManagerBase<TJobManager>::TChunkPoolOutputWithJobManagerBase(const NLogging::TLogger& logger)
    : JobManager_(New<TJobManager>(logger))
{ }

template <class TJobManager>
NTableClient::TChunkStripeStatisticsVector TChunkPoolOutputWithJobManagerBase<TJobManager>::GetApproximateStripeStatistics() const
{
    return JobManager_->GetApproximateStripeStatistics();
}

template <class TJobManager>
IChunkPoolOutput::TCookie TChunkPoolOutputWithJobManagerBase<TJobManager>::Extract(TNodeId /*nodeId*/)
{
    return JobManager_->ExtractCookie();
}

template <class TJobManager>
TChunkStripeListPtr TChunkPoolOutputWithJobManagerBase<TJobManager>::GetStripeList(IChunkPoolOutput::TCookie cookie)
{
    return JobManager_->GetStripeList(cookie);
}

template <class TJobManager>
int TChunkPoolOutputWithJobManagerBase<TJobManager>::GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const
{
    return JobManager_->GetStripeList(cookie)->GetAggregateStatistics().ChunkCount;
}

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary)
{
    JobManager_->Completed(cookie, jobSummary.InterruptionReason);
}

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Failed(IChunkPoolOutput::TCookie cookie)
{
    JobManager_->Failed(cookie);
}

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason)
{
    JobManager_->Aborted(cookie, reason);
}

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Lost(IChunkPoolOutput::TCookie cookie)
{
    JobManager_->Lost(cookie);
}

template <class TJobManager>
const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase<TJobManager>::GetJobCounter() const
{
    return JobManager_->JobCounter();
}

template <class TJobManager>
const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase<TJobManager>::GetDataWeightCounter() const
{
    return JobManager_->DataWeightCounter();
}

template <class TJobManager>
const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase<TJobManager>::GetRowCounter() const
{
    return JobManager_->RowCounter();
}

template <class TJobManager>
const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase<TJobManager>::GetDataSliceCounter() const
{
    return JobManager_->DataSliceCounter();
}

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_TEMPLATE_TYPE(TChunkPoolOutputWithJobManagerBase, (NPhoenix::_));

// Explicit instantiations.
template class TChunkPoolOutputWithJobManagerBase<TLegacyJobManager>;
template class TChunkPoolOutputWithJobManagerBase<TNewJobManager>;

////////////////////////////////////////////////////////////////////////////////

void TJobSplittingBase::Completed(TCookie cookie, const TCompletedJobSummary& jobSummary)
{
    // By default we assume that the job is not empty until proven otherwise.
    if (!jobSummary.Statistics) {
        return;
    }

    if (jobSummary.SplitJobCount > 1) {
        // Indicate that we expect this cookie to be split into at least two jobs.
        AssignVectorAt(CookieShouldBeSplitProperly_, cookie, /*defaultValue*/ true);
    }

    auto inputRowCount = FindNumericValue(*jobSummary.Statistics, InputRowCountPath);
    // If we have at least one processed row, we are a legal job, do nothing.
    // If we cannot find input row count field in statistics, something is wrong,
    // so do not affect splittability of anybody.
    YT_LOG_DEBUG(
        "Input row count extracted from job summary (JobId: %v, OutputCookie: %v, InputRowCount: %v)",
        jobSummary.Id,
        cookie,
        inputRowCount);
    if (!inputRowCount || *inputRowCount != 0) {
        return;
    }
    // By this moment inputRowCount == 0.

    // This is an important property as we should not consider job as an empty job since it may
    // not actually be empty. We can rely on fact that a job cannot be interrupted unless it read at
    // least one row, but let's write something more robust.
    if (jobSummary.InterruptionReason != EInterruptionReason::None) {
        return;
    }

    auto parentCookie = VectorAtOr(CookieToParentCookie_, cookie, /*defaultValue*/ IChunkPoolOutput::NullCookie);
    // If we are a root job, we affect splittability of nobody.
    if (parentCookie == IChunkPoolOutput::NullCookie) {
        YT_LOG_DEBUG("Job is a root job, doing nothing (JobId: %v, OutputCookie: %v)", jobSummary.Id, cookie);
        return;
    }

    // If our parent is unsplittable, we are also unsplittable due to unsplittability propagation,
    // no need to do anything any more.
    if (!IsSplittable(parentCookie)) {
        YT_VERIFY(!VectorAtOr(CookieIsSplittable_, cookie, /*defaultValue*/ true));
        YT_LOG_DEBUG(
            "Parent is already unsplittable, doing nothing (JobId: %v, OutputCookie: %v, ParentOutputCookie: %v)",
            jobSummary.Id,
            cookie,
            parentCookie);
        return;
    }

    EnsureVectorIndex(CookieToEmptyChildCount_, parentCookie);
    auto parentEmptyChildCount = ++CookieToEmptyChildCount_[parentCookie];
    YT_LOG_DEBUG(
        "Empty split child detected (JobId: %v, OutputCookie: %v, ParentOutputCookie: %v, ParentEmptyChildCount: %v)",
        jobSummary.Id,
        cookie,
        parentCookie,
        parentEmptyChildCount);

    YT_VERIFY(parentCookie < static_cast<TCookie>(CookieToChildCookies_.size()));
    if (parentEmptyChildCount + 1 >= std::ssize(CookieToChildCookies_[parentCookie])) {
        YT_LOG_DEBUG(
            "All but one sibling of a cookie are empty, marking descendants of a "
            "parent as unsplittable (JobId: %v, OutputCookie: %v, ParentOutputCookie: %v)",
            jobSummary.Id,
            cookie,
            parentCookie);
        MarkDescendantsUnsplittable(jobSummary.Id, parentCookie);
    }
}

bool TJobSplittingBase::IsSplittable(TCookie cookie) const
{
    return VectorAtOr(CookieIsSplittable_, cookie, /*defaultValue*/ true);
}

void TJobSplittingBase::RegisterChildCookies(TJobId jobId, TCookie cookie, std::vector<TCookie> childCookies)
{
    // I am not really sure if this is possible, but let us handle that trivially.
    if (childCookies.empty()) {
        return;
    }

    YT_LOG_DEBUG(
        "Job was split (JobId: %v, OutputCookie: %v, ChildOutputCookies: %v",
        jobId,
        cookie,
        childCookies);

    // Job splitter must call IsSplittable twice: before deciding to interrupt job and
    // before setting SplitJobCount (note that we may decide that job is unsplittable
    // before these two events by all-siblings-were-empty condition).
    YT_LOG_ALERT_UNLESS(
        childCookies.size() == 1 || IsSplittable(cookie),
        "Unexpected call to RegisterChildCookies (JobId: %v, IsSplittable: %v, ChildCookies: %v)",
        jobId,
        IsSplittable(cookie),
        childCookies);

    // If there is a single child while we expected child count to be at least 2,
    // we were not successful in splitting this job.
    // Also, if we were already marked unsplittable, mark child unsplittable
    // to ensure unsplittability propagation.
    bool markChildrenUnsplittable = false;
    if (childCookies.size() == 1 && VectorAtOr(CookieShouldBeSplitProperly_, cookie)) {
        YT_LOG_DEBUG(
            "Splitting resulted in a single child, marking him as unsplittable (JobId: %v, OutputCookie: %v, ChildOutputCookie: %v)",
            jobId,
            cookie,
            childCookies.back());
        markChildrenUnsplittable = true;
    }
    if (!IsSplittable(cookie)) {
        YT_LOG_DEBUG(
            "Propagating unsplittability to children (JobId: %v, OutputCookie: %v, ChildOutputCookies: %v)",
            jobId,
            cookie,
            childCookies);
        markChildrenUnsplittable = true;
    }
    if (markChildrenUnsplittable) {
        for (auto childCookie : childCookies) {
            AssignVectorAt(CookieIsSplittable_, childCookie, /*value*/ false, /*defaultValue*/ true);
        }
    }

    for (auto childCookie : childCookies) {
        AssignVectorAt(CookieToParentCookie_, childCookie, cookie, /*defaultValue*/ IChunkPoolOutput::NullCookie);
    }
    AssignVectorAt(CookieToChildCookies_, cookie, std::move(childCookies));
}

void TJobSplittingBase::MarkDescendantsUnsplittable(TJobId jobId, TCookie cookie)
{
    if (!IsSplittable(cookie)) {
        return;
    }

    std::vector<TCookie> visitedCookies;

    // Try to avoid recursion by using a self-made stack.
    std::vector<TCookie> cookiesToVisit = {cookie};
    while (!cookiesToVisit.empty()) {
        auto cookieToVisit = cookiesToVisit.back();
        cookiesToVisit.pop_back();
        YT_VERIFY(IsSplittable(cookieToVisit));

        visitedCookies.push_back(cookieToVisit);
        AssignVectorAt(CookieIsSplittable_, cookieToVisit, /*value*/ false, /*defaultValue*/ true);

        auto childCookies = VectorAtOr(CookieToChildCookies_, cookieToVisit, /*defaultValue*/ {});

        for (auto childCookie : childCookies) {
            if (!IsSplittable(childCookie)) {
                continue;
            }
            cookiesToVisit.push_back(childCookie);
        }
    }

    YT_LOG_DEBUG(
        "Descendants of a cookie marked as unsplittable (JobId: %v, OutputCookie: %v, NewUnsplittableDescendants: %v)",
        jobId,
        cookie,
        visitedCookies);
}

size_t TJobSplittingBase::GetMaxVectorSize() const
{
    return std::max({
        CookieToChildCookies_.size(),
        CookieToEmptyChildCount_.size(),
        CookieToParentCookie_.size(),
        CookieIsSplittable_.size(),
        CookieShouldBeSplitProperly_.size(),
    });
}

void TJobSplittingBase::ValidateChildJobSizes(
    TOutputCookie parentCookie,
    const std::vector<TOutputCookie>& childCookies,
    const std::function<TChunkStripeListPtr(TOutputCookie)>& getStripeList) const
{
    auto getPrimaryAndForeignStatistics = [&] (TOutputCookie cookie) -> std::pair<TChunkStripeStatistics, TChunkStripeStatistics> {
        TChunkStripeStatistics primaryStatistics;
        TChunkStripeStatistics foreignStatistics;

        for (auto stripeList = getStripeList(cookie); const auto& stripe : stripeList->Stripes()) {
            if (!stripe->IsForeign()) {
                primaryStatistics += stripe->GetStatistics();
            } else {
                foreignStatistics += stripe->GetStatistics();
            }
        }

        return {primaryStatistics, foreignStatistics};
    };

    auto [parentPrimaryStatistics, parentForeignStatistics] = getPrimaryAndForeignStatistics(parentCookie);

    TChunkStripeStatistics totalChildPrimaryStatistics;
    TChunkStripeStatistics totalChildForeignStatistics;

    auto hasGreaterComponent = [] (const TChunkStripeStatistics& childStatistics, const TChunkStripeStatistics& parentStatistics) {
        return childStatistics.DataWeight > parentStatistics.DataWeight ||
            childStatistics.CompressedDataSize > parentStatistics.CompressedDataSize ||
            childStatistics.RowCount > parentStatistics.RowCount ||
            childStatistics.ChunkCount > parentStatistics.ChunkCount;
    };

    for (auto childCookie : childCookies) {
        auto [childPrimaryStatistics, childForeignStatistics] = getPrimaryAndForeignStatistics(childCookie);

        YT_LOG_ALERT_IF(
            hasGreaterComponent(childPrimaryStatistics, parentPrimaryStatistics) ||
            hasGreaterComponent(childForeignStatistics, parentForeignStatistics),
            "Child job is greater than parent job "
            "(ParentCookie: %v, ChildCookie: %v, ParentPrimaryStatistics: %v, ChildPrimaryStatistics: %v, "
            "ParentForeignStatistics: %v, ChildForeignStatistics: %v)",
            parentCookie,
            childCookie,
            parentPrimaryStatistics,
            childPrimaryStatistics,
            parentForeignStatistics,
            childForeignStatistics);

        totalChildPrimaryStatistics += childPrimaryStatistics;
        totalChildForeignStatistics += childForeignStatistics;
    }

    YT_LOG_ALERT_IF(
        totalChildPrimaryStatistics.DataWeight > parentPrimaryStatistics.DataWeight ||
        totalChildPrimaryStatistics.RowCount > parentPrimaryStatistics.RowCount,
        "Child jobs have greater primary data weight or primary row count than parent job "
        "(ParentCookie: %v, ChildCookies: %v, ParentPrimaryStatistics: %v, ParentForeignStatistics: %v, "
        "TotalChildPrimaryStatistics: %v, TotalChildForeignStatistics: %v)",
        parentCookie,
        childCookies,
        parentPrimaryStatistics,
        parentForeignStatistics,
        totalChildPrimaryStatistics,
        totalChildForeignStatistics);
}

void TJobSplittingBase::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TLoggerOwner>();

    PHOENIX_REGISTER_FIELD(1, CookieToChildCookies_);
    PHOENIX_REGISTER_FIELD(2, CookieToEmptyChildCount_);
    PHOENIX_REGISTER_FIELD(3, CookieToParentCookie_);
    PHOENIX_REGISTER_FIELD(4, CookieIsSplittable_);
    PHOENIX_REGISTER_FIELD(5, CookieShouldBeSplitProperly_);
}

PHOENIX_DEFINE_TYPE(TJobSplittingBase);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
