#include "chunk_pool.h"

#include "job_manager.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NScheduler;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

void TChunkPoolInputBase::Finish()
{
    Finished = true;
}

IChunkPoolInput::TCookie TChunkPoolInputBase::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    // `key` argument should be set to something non-trivial only for sink chunk pool inputs,
    // so for all classes that are inherited from this `key` should never be set.
    YCHECK(!key);
    // Stripes may either contain several data slices or consist only of a single chunk tree id.
    // All classes that are inherited from this base are dealing with explicit chunk representations,
    // so they are not ready to work with stripes that do not contain data slices.
    YCHECK(!stripe->DataSlices.empty());

    return Add(stripe);
}

void TChunkPoolInputBase::Reset(TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping)
{
    Y_UNREACHABLE();
}

void TChunkPoolInputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Finished);
}

////////////////////////////////////////////////////////////////////////////////

// IPersistent implementation.

const std::vector<TInputChunkPtr>& TChunkPoolOutputBase::GetTeleportChunks() const
{
    return TeleportChunks_;
}

TOutputOrderPtr TChunkPoolOutputBase::GetOutputOrder() const
{
    return nullptr;
}

i64 TChunkPoolOutputBase::GetLocality(NNodeTrackerClient::TNodeId nodeId) const
{
    return 0;
}

void TChunkPoolOutputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TeleportChunks_);
}

////////////////////////////////////////////////////////////////////////////////

TChunkPoolOutputWithCountersBase::TChunkPoolOutputWithCountersBase()
    : DataWeightCounter(New<TProgressCounter>(0))
    , RowCounter(New<TProgressCounter>(0))
    , JobCounter(New<TProgressCounter>())
{ }

i64 TChunkPoolOutputWithCountersBase::GetTotalDataWeight() const
{
    return DataWeightCounter->GetTotal();
}

i64 TChunkPoolOutputWithCountersBase::GetRunningDataWeight() const
{
    return DataWeightCounter->GetRunning();
}

i64 TChunkPoolOutputWithCountersBase::GetCompletedDataWeight() const
{
    return DataWeightCounter->GetCompletedTotal();
}

i64 TChunkPoolOutputWithCountersBase::GetPendingDataWeight() const
{
    return DataWeightCounter->GetPending();
}

i64 TChunkPoolOutputWithCountersBase::GetTotalRowCount() const
{
    return RowCounter->GetTotal();
}

const TProgressCounterPtr& TChunkPoolOutputWithCountersBase::GetJobCounter() const
{
    return JobCounter;
}

void TChunkPoolOutputWithCountersBase::Persist(const TPersistenceContext& context)
{
    TChunkPoolOutputBase::Persist(context);

    using NYT::Persist;
    Persist(context, DataWeightCounter);
    Persist(context, RowCounter);
    Persist(context, JobCounter);
}

////////////////////////////////////////////////////////////////////////////////

// IChunkPoolOutput implementation.

TChunkPoolOutputWithJobManagerBase::TChunkPoolOutputWithJobManagerBase()
    : JobManager_(New<TJobManager>())
{ }

TChunkStripeStatisticsVector TChunkPoolOutputWithJobManagerBase::GetApproximateStripeStatistics() const
{
    return JobManager_->GetApproximateStripeStatistics();
}

int TChunkPoolOutputWithJobManagerBase::GetTotalJobCount() const
{
    return JobManager_->JobCounter()->GetTotal();
}

int TChunkPoolOutputWithJobManagerBase::GetPendingJobCount() const
{
    return JobManager_->GetPendingJobCount();
}

IChunkPoolOutput::TCookie TChunkPoolOutputWithJobManagerBase::Extract(TNodeId /* nodeId */)
{
    return JobManager_->ExtractCookie();
}

TChunkStripeListPtr TChunkPoolOutputWithJobManagerBase::GetStripeList(IChunkPoolOutput::TCookie cookie)
{
    return JobManager_->GetStripeList(cookie);
}

int TChunkPoolOutputWithJobManagerBase::GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const
{
    return JobManager_->GetStripeList(cookie)->TotalChunkCount;
}

void TChunkPoolOutputWithJobManagerBase::Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary)
{
    JobManager_->Completed(cookie, jobSummary.InterruptReason);
}

void TChunkPoolOutputWithJobManagerBase::Failed(IChunkPoolOutput::TCookie cookie)
{
    JobManager_->Failed(cookie);
}

void TChunkPoolOutputWithJobManagerBase::Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason)
{
    JobManager_->Aborted(cookie, reason);
}

void TChunkPoolOutputWithJobManagerBase::Lost(IChunkPoolOutput::TCookie cookie)
{
    JobManager_->Lost(cookie);
}

i64 TChunkPoolOutputWithJobManagerBase::GetTotalDataWeight() const
{
    return JobManager_->DataWeightCounter()->GetTotal();
}

i64 TChunkPoolOutputWithJobManagerBase::GetRunningDataWeight() const
{
    return JobManager_->DataWeightCounter()->GetRunning();
}

i64 TChunkPoolOutputWithJobManagerBase::GetCompletedDataWeight() const
{
    return JobManager_->DataWeightCounter()->GetCompletedTotal();
}

i64 TChunkPoolOutputWithJobManagerBase::GetPendingDataWeight() const
{
    return JobManager_->DataWeightCounter()->GetPending();
}

i64 TChunkPoolOutputWithJobManagerBase::GetTotalRowCount() const
{
    return JobManager_->RowCounter()->GetTotal();
}

const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase::GetJobCounter() const
{
    return JobManager_->JobCounter();
}

void TChunkPoolOutputWithJobManagerBase::Persist(const TPersistenceContext& context)
{
    TChunkPoolOutputBase::Persist(context);

    using NYT::Persist;
    Persist(context, JobManager_);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
