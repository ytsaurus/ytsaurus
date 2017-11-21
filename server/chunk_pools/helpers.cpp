#include "helpers.h"

#include "chunk_pool.h"

namespace NYT {
namespace NChunkPools {

using namespace NNodeTrackerClient;
using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void AddStripeToList(
    const TChunkStripePtr& stripe,
    i64 stripeDataWeight,
    i64 stripeRowCount,
    const TChunkStripeListPtr& list,
    TNodeId nodeId)
{
    list->Stripes.push_back(stripe);
    list->TotalDataWeight += stripeDataWeight;
    list->TotalRowCount += stripeRowCount;
    list->TotalChunkCount += stripe->GetChunkCount();
    if (nodeId == InvalidNodeId) {
        return;
    }
    for (const auto& dataSlice : stripe->DataSlices) {
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            bool isLocal = false;
            for (auto replica : chunkSlice->GetInputChunk()->GetReplicaList()) {
                if (replica.GetNodeId() == nodeId) {
                    i64 locality = chunkSlice->GetLocality(replica.GetReplicaIndex());
                    if (locality > 0) {
                        list->LocalDataWeight += locality;
                        isLocal = true;
                    }
                }
            }

            if (isLocal) {
                ++list->LocalChunkCount;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkStripeListPtr ApplyChunkMappingToStripe(
    const TChunkStripeListPtr& stripeList,
    const THashMap<TInputChunkPtr, TInputChunkPtr>& inputChunkMapping)
{
    auto mappedStripeList = New<TChunkStripeList>(stripeList->Stripes.size());
    for (int stripeIndex = 0; stripeIndex < stripeList->Stripes.size(); ++stripeIndex) {
        const auto& stripe = stripeList->Stripes[stripeIndex];
        YCHECK(stripe);
        const auto& mappedStripe = (mappedStripeList->Stripes[stripeIndex] = New<TChunkStripe>(stripe->Foreign));
        for (const auto& dataSlice : stripe->DataSlices) {
            TInputDataSlice::TChunkSliceList mappedChunkSlices;
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                auto iterator = inputChunkMapping.find(chunkSlice->GetInputChunk());
                YCHECK(iterator != inputChunkMapping.end());
                mappedChunkSlices.emplace_back(New<TInputChunkSlice>(*chunkSlice));
                mappedChunkSlices.back()->SetInputChunk(iterator->second);
            }

            mappedStripe->DataSlices.emplace_back(New<TInputDataSlice>(
                    dataSlice->Type,
                    std::move(mappedChunkSlices),
                    dataSlice->LowerLimit(),
                    dataSlice->UpperLimit()));
            mappedStripe->DataSlices.back()->Tag = dataSlice->Tag;
            mappedStripe->DataSlices.back()->InputStreamIndex = dataSlice->InputStreamIndex;
        }
    }

    mappedStripeList->IsApproximate = stripeList->IsApproximate;
    mappedStripeList->TotalDataWeight = stripeList->TotalDataWeight;
    mappedStripeList->LocalDataWeight = stripeList->LocalDataWeight;
    mappedStripeList->TotalRowCount = stripeList->TotalRowCount;
    mappedStripeList->TotalChunkCount = stripeList->TotalChunkCount;
    mappedStripeList->LocalChunkCount = stripeList->LocalChunkCount;

    return mappedStripeList;
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

THashMap<TInputChunkPtr, TInputChunkPtr> TSuspendableStripe::ResumeAndBuildChunkMapping(TChunkStripePtr stripe)
{
    YCHECK(Stripe_);
    YCHECK(Suspended_);

    THashMap<TInputChunkPtr, TInputChunkPtr> mapping;

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

    THashMap<i64, TInputDataSlicePtr> tagToDataSlice;

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
                TNullable<TOwningBoundaryKeys> originalBoundaryKeys =
                    originalChunk->BoundaryKeys() ? MakeNullable(*originalChunk->BoundaryKeys()) : Null;
                TNullable<TOwningBoundaryKeys> newBoundaryKeys =
                    newChunk->BoundaryKeys() ? MakeNullable(*newChunk->BoundaryKeys()) : Null;
                if (originalBoundaryKeys != newBoundaryKeys) {
                    THROW_ERROR_EXCEPTION("Corresponding chunks in original and new stripes have different boundary keys")
                            << TErrorAttribute("data_slice_tag", *originalSlice->Tag)
                            << TErrorAttribute("original_chunk_id", originalChunk->ChunkId())
                            << TErrorAttribute("original_boundary_keys", originalBoundaryKeys)
                            << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
                            << TErrorAttribute("new_boundary_keys", newBoundaryKeys);
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

} // namespace NChunkPools
} // namespace NYT

