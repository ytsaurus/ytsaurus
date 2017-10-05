#include "chunk_pool.h"

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NChunkPools {

using namespace NChunkClient;
using namespace NControllerAgent;

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

void TChunkPoolInputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Finished);
}

////////////////////////////////////////////////////////////////////////////////

TChunkPoolOutputBase::TChunkPoolOutputBase()
    : DataWeightCounter(New<TProgressCounter>(0))
    , RowCounter(New<TProgressCounter>(0))
    , JobCounter(New<TProgressCounter>())
{ }

// IChunkPoolOutput implementation.

i64 TChunkPoolOutputBase::GetTotalDataWeight() const
{
    return DataWeightCounter->GetTotal();
}

i64 TChunkPoolOutputBase::GetRunningDataWeight() const
{
    return DataWeightCounter->GetRunning();
}

i64 TChunkPoolOutputBase::GetCompletedDataWeight() const
{
    return DataWeightCounter->GetCompletedTotal();
}

i64 TChunkPoolOutputBase::GetPendingDataWeight() const
{
    return DataWeightCounter->GetPending();
}

i64 TChunkPoolOutputBase::GetTotalRowCount() const
{
    return RowCounter->GetTotal();
}

const TProgressCounterPtr& TChunkPoolOutputBase::GetJobCounter() const
{
    return JobCounter;
}

// IPersistent implementation.

void TChunkPoolOutputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataWeightCounter);
    Persist(context, RowCounter);
    Persist(context, JobCounter);
}

const std::vector<TInputChunkPtr>& TChunkPoolOutputBase::GetTeleportChunks() const
{
    return TeleportChunks_;
}

TOutputOrderPtr TChunkPoolOutputBase::GetOutputOrder() const
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
