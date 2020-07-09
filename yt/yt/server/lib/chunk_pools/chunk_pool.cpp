#include "chunk_pool.h"

#include "job_manager.h"

#include <yt/server/lib/controller_agent/structs.h>

#include <yt/ytlib/chunk_client/input_chunk.h>

#include <yt/core/misc/numeric_helpers.h>

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

void TChunkPoolInputBase::Reset(TCookie /* cookie */, TChunkStripePtr /* stripe */, TInputChunkMappingPtr /* mapping */)
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

i64 TChunkPoolOutputBase::GetLocality(NNodeTrackerClient::TNodeId /* nodeId */) const
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

// IChunkPoolOutput implementation.

TChunkPoolOutputWithJobManagerBase::TChunkPoolOutputWithJobManagerBase()
    : JobManager_(New<TJobManager>())
{ }

TChunkStripeStatisticsVector TChunkPoolOutputWithJobManagerBase::GetApproximateStripeStatistics() const
{
    return JobManager_->GetApproximateStripeStatistics();
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

const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase::GetJobCounter() const
{
    return JobManager_->JobCounter();
}

const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase::GetDataWeightCounter() const
{
    return JobManager_->DataWeightCounter();
}

const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase::GetRowCounter() const
{
    return JobManager_->RowCounter();
}

const TProgressCounterPtr& TChunkPoolOutputWithJobManagerBase::GetDataSliceCounter() const
{
    return JobManager_->DataSliceCounter();
}

void TChunkPoolOutputWithJobManagerBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobManager_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
