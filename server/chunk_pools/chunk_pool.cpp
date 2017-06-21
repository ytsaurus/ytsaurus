#include "chunk_pool.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/job_size_adjuster.h>
#include <yt/server/controller_agent/progress_counter.h>
#include <yt/server/controller_agent/private.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NChunkPools {

using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler;

using NTableClient::NProto::TPartitionsExt;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

void TChunkStripeStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, DataSize);
    Persist(context, RowCount);
    Persist(context, MaxBlockSize);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe(bool foreign, bool solid)
    : Foreign(foreign)
    , Solid(solid)
{ }

TChunkStripe::TChunkStripe(TInputDataSlicePtr dataSlice, bool foreign)
    : Foreign(foreign)
{
    DataSlices.emplace_back(std::move(dataSlice));
}

TChunkStripeStatistics TChunkStripe::GetStatistics() const
{
    TChunkStripeStatistics result;

    for (const auto& dataSlice : DataSlices) {
        result.DataSize += dataSlice->GetDataSize();
        result.RowCount += dataSlice->GetRowCount();
        ++result.ChunkCount;
        result.MaxBlockSize = std::max(result.MaxBlockSize, dataSlice->GetMaxBlockSize());
    }

    return result;
}

int TChunkStripe::GetChunkCount() const
{
    int result = 0;
    for (const auto& dataSlice : DataSlices) {
        result += dataSlice->GetChunkCount();
    }
    return result;
}

int TChunkStripe::GetTableIndex() const
{
    YCHECK(!DataSlices.empty());
    YCHECK(!DataSlices.front()->ChunkSlices.empty());
    return DataSlices.front()->ChunkSlices.front()->GetInputChunk()->GetTableIndex();
}

int TChunkStripe::GetInputStreamIndex() const
{
    YCHECK(!DataSlices.empty());
    return DataSlices.front()->InputStreamIndex;
}

void TChunkStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataSlices);
    Persist(context, WaitingChunkCount);
    Persist(context, Foreign);
    Persist(context, Solid);
}

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    TChunkStripeStatistics result;
    result.ChunkCount = lhs.ChunkCount + rhs.ChunkCount;
    result.DataSize = lhs.DataSize + rhs.DataSize;
    result.RowCount = lhs.RowCount + rhs.RowCount;
    result.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return result;
}

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.DataSize += rhs.DataSize;
    lhs.RowCount += rhs.RowCount;
    lhs.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    return lhs;
}

TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics)
{
    TChunkStripeStatistics sum;
    for (const auto& stat : statistics) {
        sum += stat;
    }
    return TChunkStripeStatisticsVector(1, sum);
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeList::TChunkStripeList(int stripeCount)
    : Stripes(stripeCount)
{ }

TChunkStripeStatisticsVector TChunkStripeList::GetStatistics() const
{
    TChunkStripeStatisticsVector result;
    result.reserve(Stripes.size());
    for (const auto& stripe : Stripes) {
        result.push_back(stripe->GetStatistics());
    }
    return result;
}

TChunkStripeStatistics TChunkStripeList::GetAggregateStatistics() const
{
    TChunkStripeStatistics result;
    result.ChunkCount = TotalChunkCount;
    if (IsApproximate) {
        result.RowCount = TotalRowCount * ApproximateSizesBoostFactor;
        result.DataSize = TotalDataSize * ApproximateSizesBoostFactor;
    } else {
        result.RowCount = TotalRowCount;
        result.DataSize = TotalDataSize;
    }
    return result;
}

void TChunkStripeList::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripes);
    Persist(context, PartitionTag);
    Persist(context, IsApproximate);
    Persist(context, TotalDataSize);
    Persist(context, LocalDataSize);
    Persist(context, TotalRowCount);
    Persist(context, TotalChunkCount);
    Persist(context, LocalChunkCount);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkPoolInputBase::Finish()
{
    Finished = true;
}

void TChunkPoolInputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Finished);
}

////////////////////////////////////////////////////////////////////////////////

TSuspendableStripe::TSuspendableStripe()
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
{ }

TSuspendableStripe::TSuspendableStripe(TChunkStripePtr stripe)
    : ExtractedCookie_(IChunkPoolOutput::NullCookie)
    , Stripe_(std::move(stripe))
    , OriginalStripe_(Stripe_)
    , Statistics_(Stripe_->GetStatistics())
{ }

const TChunkStripePtr& TSuspendableStripe::GetStripe() const
{
    return Stripe_;
}

const TChunkStripeStatistics& TSuspendableStripe::GetStatistics() const
{
    return Statistics_;
}

void TSuspendableStripe::Suspend()
{
    YCHECK(Stripe_);
    YCHECK(!Suspended_);

    Suspended_ = true;
}

bool TSuspendableStripe::IsSuspended() const
{
    return Suspended_;
}

void TSuspendableStripe::Resume(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    // NB: do not update statistics on resume to preserve counters.
    Suspended_ = false;
    Stripe_ = stripe;
}

yhash<TInputChunkPtr, TInputChunkPtr> TSuspendableStripe::ResumeAndBuildChunkMapping(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    yhash<TInputChunkPtr, TInputChunkPtr> mapping;

    // Our goal is to restore the correspondence between the old data slices and new data slices
    // in order to be able to substitute old references to input chunks in newly created jobs with current
    // ones.

    auto addToMapping = [&mapping] (const TInputDataSlicePtr& originalDataSlice, const TInputDataSlicePtr& newDataSlice) {
        YCHECK(!newDataSlice || originalDataSlice->ChunkSlices.size() == newDataSlice->ChunkSlices.size());
        for (int index = 0; index < originalDataSlice->ChunkSlices.size(); ++index) {
            mapping[originalDataSlice->ChunkSlices[index]->GetInputChunk()] = newDataSlice
                ? newDataSlice->ChunkSlices[index]->GetInputChunk()
                : nullptr;
        }
    };

    yhash<i64, TInputDataSlicePtr> tagToDataSlice;

    for (const auto& dataSlice : stripe->DataSlices) {
        YCHECK(dataSlice->Tag);
        YCHECK(tagToDataSlice.insert(std::make_pair(*dataSlice->Tag, dataSlice)).second);
    }

    for (int index = 0; index < OriginalStripe_->DataSlices.size(); ++index) {
        const auto& originalSlice = OriginalStripe_->DataSlices[index];
        auto it = tagToDataSlice.find(*originalSlice->Tag);
        if (it != tagToDataSlice.end()) {
            const auto& newSlice = it->second;
            if (originalSlice->Type == EDataSourceType::UnversionedTable) {
                const auto& originalChunk = originalSlice->GetSingleUnversionedChunkOrThrow();
                const auto& newChunk = newSlice->GetSingleUnversionedChunkOrThrow();
                if (*originalChunk->BoundaryKeys() != *newChunk->BoundaryKeys()) {
                    THROW_ERROR_EXCEPTION("Corresponding chunks in original and new stripes have different boundary keys")
                        << TErrorAttribute("data_slice_tag", *originalSlice->Tag)
                        << TErrorAttribute("original_chunk_id", originalChunk->ChunkId())
                        << TErrorAttribute("original_boundary_keys", *originalChunk->BoundaryKeys())
                        << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
                        << TErrorAttribute("new_boundary_keys", *newChunk->BoundaryKeys());
                    break;
                }
                if (originalChunk->GetRowCount() != newChunk->GetRowCount()) {
                    THROW_ERROR_EXCEPTION("Corresponding chunks in original and new stripes have different row counts")
                        << TErrorAttribute("data_slice_tag", *originalSlice->Tag)
                        << TErrorAttribute("original_chunk_id", originalChunk->ChunkId())
                        << TErrorAttribute("original_row_count", originalChunk->GetRowCount())
                        << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
                        << TErrorAttribute("new_row_count", newChunk->GetRowCount());
                }
            }
            addToMapping(originalSlice, newSlice);
            tagToDataSlice.erase(it);
        } else {
            addToMapping(originalSlice, nullptr);
        }
    }

    if (!tagToDataSlice.empty()) {
        THROW_ERROR_EXCEPTION("New stripe has extra data slices")
            << TErrorAttribute("extra_data_slice_tag", tagToDataSlice.begin()->first);
    }

    // NB: do not update statistics on resume to preserve counters.
    Suspended_ = false;
    Stripe_ = stripe;

    return mapping;
}

void TSuspendableStripe::ReplaceOriginalStripe()
{
    OriginalStripe_ = Stripe_;
}

void TSuspendableStripe::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ExtractedCookie_);
    Persist(context, Stripe_);
    Persist(context, OriginalStripe_);
    Persist(context, Teleport_);
    Persist(context, Suspended_);
    Persist(context, Statistics_);
}

////////////////////////////////////////////////////////////////////////////////


TChunkPoolOutputBase::TChunkPoolOutputBase()
    : DataSizeCounter(0)
    , RowCounter(0)
{ }

// IChunkPoolOutput implementation.

i64 TChunkPoolOutputBase::GetTotalDataSize() const
{
    return DataSizeCounter.GetTotal();
}

i64 TChunkPoolOutputBase::GetRunningDataSize() const
{
    return DataSizeCounter.GetRunning();
}

i64 TChunkPoolOutputBase::GetCompletedDataSize() const
{
    return DataSizeCounter.GetCompletedTotal();
}

i64 TChunkPoolOutputBase::GetPendingDataSize() const
{
    return DataSizeCounter.GetPending();
}

i64 TChunkPoolOutputBase::GetTotalRowCount() const
{
    return RowCounter.GetTotal();
}

const TProgressCounter& TChunkPoolOutputBase::GetJobCounter() const
{
    return JobCounter;
}

// IPersistent implementation.

void TChunkPoolOutputBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DataSizeCounter);
    Persist(context, RowCounter);
    Persist(context, JobCounter);
}

const std::vector<TInputChunkPtr>& TChunkPoolOutputBase::GetTeleportChunks() const
{
    return TeleportChunks_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
