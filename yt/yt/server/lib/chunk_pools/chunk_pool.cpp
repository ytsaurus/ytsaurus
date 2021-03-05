#include "chunk_pool.h"

#include "legacy_job_manager.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/core/misc/numeric_helpers.h>

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

template <class TJobManager>
TChunkPoolOutputWithJobManagerBase<TJobManager>::TChunkPoolOutputWithJobManagerBase()
    : JobManager_(New<TJobManager>())
{ }

template <class TJobManager>
TChunkStripeStatisticsVector TChunkPoolOutputWithJobManagerBase<TJobManager>::GetApproximateStripeStatistics() const
{
    return JobManager_->GetApproximateStripeStatistics();
}

template <class TJobManager>
IChunkPoolOutput::TCookie TChunkPoolOutputWithJobManagerBase<TJobManager>::Extract(TNodeId /* nodeId */)
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

} // namespace NYT::NChunkPools
