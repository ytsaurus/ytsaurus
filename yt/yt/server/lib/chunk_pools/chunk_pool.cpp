#include "chunk_pool.h"

#include "legacy_job_manager.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/statistics.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;

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
    YT_VERIFY(!stripe->DataSlices.empty());

    return Add(stripe);
}

void TChunkPoolInputBase::Reset(TCookie /*cookie*/, TChunkStripePtr /*stripe*/, TInputChunkMappingPtr /*mapping*/)
{
    YT_ABORT();
}

void TChunkPoolInputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Finished);
}

////////////////////////////////////////////////////////////////////////////////

// IPersistent implementation.

TOutputOrderPtr TChunkPoolOutputBase::GetOutputOrder() const
{
    return nullptr;
}

i64 TChunkPoolOutputBase::GetLocality(NNodeTrackerClient::TNodeId /*nodeId*/) const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

TChunkPoolOutputWithCountersBase::TChunkPoolOutputWithCountersBase()
    : DataWeightCounter(New<TProgressCounter>())
    , RowCounter(New<TProgressCounter>())
    , JobCounter(New<TProgressCounter>())
    , DataSliceCounter(New<TProgressCounter>())
{ }

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetJobCounter() const
{
    return JobCounter;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetDataWeightCounter() const
{
    return DataWeightCounter;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetRowCounter() const
{
    return RowCounter;
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetDataSliceCounter() const
{
    return DataSliceCounter;
}

void TChunkPoolOutputWithCountersBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobCounter);
    Persist(context, DataWeightCounter);
    Persist(context, RowCounter);
    Persist(context, DataSliceCounter);
}

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
    return JobManager_->GetStripeList(cookie)->TotalChunkCount;
}

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary)
{
    JobManager_->Completed(cookie, jobSummary.InterruptReason);
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

template <class TJobManager>
void TChunkPoolOutputWithJobManagerBase<TJobManager>::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobManager_);
}

////////////////////////////////////////////////////////////////////////////////

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
        "Input row count extracted from job summary (OutputCookie: %v, InputRowCount: %v)",
        cookie,
        inputRowCount);
    if (!inputRowCount || *inputRowCount != 0) {
        return;
    }
    // By this moment inputRowCount == 0.

    // This is an important property as we should not consider job as an empty job since it may
    // not actually be empty. We can rely on fact that a job cannot be interrupted unless it read at
    // least one row, but let's write something more robust.
    if (jobSummary.InterruptReason != EInterruptReason::None) {
        return;
    }

    auto parentCookie = VectorAtOr(CookieToParentCookie_, cookie, /*defaultValue*/ IChunkPoolOutput::NullCookie);
    // If we are a root job, we affect splittability of nobody.
    if (parentCookie == IChunkPoolOutput::NullCookie) {
        YT_LOG_DEBUG("Job is a root job, doing nothing (OutputCookie: %v)", cookie);
        return;
    }

    // If our parent is unsplittable, we are also unsplittable due to unsplittability propagation,
    // no need to do anything any more.
    if (!IsSplittable(parentCookie)) {
        YT_VERIFY(!VectorAtOr(CookieIsSplittable_, cookie, /*defaultValue*/ true));
        YT_LOG_DEBUG(
            "Parent is already unsplittable, doing nothing (OutputCookie: %v, ParentOutputCookie: %v)",
            cookie,
            parentCookie);
        return;
    }

    EnsureVectorIndex(CookieToEmptyChildCount_, parentCookie);
    auto parentEmptyChildCount = ++CookieToEmptyChildCount_[parentCookie];
    YT_LOG_DEBUG(
        "Empty split child detected (OutputCookie: %v, ParentOutputCookie: %v, ParentEmptyChildCount: %v)",
        cookie,
        parentCookie,
        parentEmptyChildCount);

    YT_VERIFY(parentCookie < static_cast<TCookie>(CookieToChildCookies_.size()));
    if (parentEmptyChildCount + 1 >= static_cast<int>(CookieToChildCookies_[parentCookie].size())) {
        YT_LOG_DEBUG(
            "All but one sibling of a cookie are empty, marking descendants of a "
            "parent as unsplittable (OutputCookie: %v, ParentOutputCookie: %v)",
            cookie,
            parentCookie);
        MarkDescendantsUnsplittable(parentCookie);
    }
}

bool TJobSplittingBase::IsSplittable(TCookie cookie) const
{
    return VectorAtOr(CookieIsSplittable_, cookie, /*defaultValue*/ true);
}

void TJobSplittingBase::RegisterChildCookies(TCookie cookie, std::vector<TCookie> childCookies)
{
    // I am not really sure if this is possible, but let us handle that trivially.
    if (childCookies.empty()) {
        return;
    }

    YT_LOG_DEBUG(
        "Job was split (OutputCookie: %v, ChildOutputCookies: %v",
        cookie,
        childCookies);

    // Job splitter must call IsSplittable twice: before deciding to interrupt job and
    // before setting SplitJobCount (note that we may decide that job is unsplittable
    // before these two events by all-siblings-were-empty condition).
    YT_LOG_ALERT_UNLESS(
        childCookies.size() == 1 || IsSplittable(cookie),
        "Unexpected call to RegisterChildCookies (IsSplittable: %v, ChildCookies: %v)",
        IsSplittable(cookie),
        childCookies);

    // If there is a single child while we expected child count to be at least 2,
    // we were not successful in splitting this job.
    // Also, if we were already marked unsplittable, mark child unsplittable
    // to ensure unsplittability propagation.
    bool markChildrenUnsplittable = false;
    if (childCookies.size() == 1 && VectorAtOr(CookieShouldBeSplitProperly_, cookie)) {
        YT_LOG_DEBUG(
            "Splitting resulted in a single child, marking him as unsplittable (OutputCookie: %v, ChildOutputCookie: %v)",
            cookie,
            childCookies.back());
        markChildrenUnsplittable = true;
    }
    if (!IsSplittable(cookie)) {
        YT_LOG_DEBUG(
            "Propagating unsplittability to children (OutputCookie: %v, ChildOutputCookies: %v)",
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

void TJobSplittingBase::MarkDescendantsUnsplittable(TCookie cookie)
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
        "Descendants of a cookie marked as unsplittable (OutputCookie: %v, NewUnsplittableDescendants: %v)",
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

void TJobSplittingBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    TLoggerOwner::Persist(context);

    Persist(context, CookieToChildCookies_);
    Persist(context, CookieToEmptyChildCount_);
    Persist(context, CookieToParentCookie_);
    Persist(context, CookieIsSplittable_);
    Persist(context, CookieShouldBeSplitProperly_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
