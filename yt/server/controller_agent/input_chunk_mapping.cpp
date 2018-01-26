#include "input_chunk_mapping.h"

#include <yt/server/chunk_pools/chunk_stripe.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NControllerAgent{

using namespace NChunkPools;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TInputChunkMapping::TInputChunkMapping(EChunkMappingMode mode)
    : Mode_(mode)
{ }

TChunkStripePtr TInputChunkMapping::GetMappedStripe(const TChunkStripePtr& stripe) const
{
    YCHECK(stripe);
    auto mappedStripe = New<TChunkStripe>();
    for (const auto& dataSlice : stripe->DataSlices) {
        TInputDataSlice::TChunkSliceList mappedChunkSlices;
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            auto iterator = Substitutes_.find(chunkSlice->GetInputChunk());
            if (iterator == Substitutes_.end()) {
                mappedChunkSlices.emplace_back(chunkSlice);
            } else {
                YCHECK(!dataSlice->HasLimits());
                const auto& substitutes = iterator->second;
                for (const auto& substituteChunk : substitutes) {
                    mappedChunkSlices.emplace_back(New<TInputChunkSlice>(substituteChunk));
                }
            }
        }

        mappedStripe->DataSlices.emplace_back(New<TInputDataSlice>(
             dataSlice->Type,
             std::move(mappedChunkSlices),
             dataSlice->LowerLimit(),
             dataSlice->UpperLimit()));
        mappedStripe->DataSlices.back()->Tag = dataSlice->Tag;
        mappedStripe->DataSlices.back()->InputStreamIndex = dataSlice->InputStreamIndex;
    }

    return mappedStripe;
}

void TInputChunkMapping::OnStripeRegenerated(
    IChunkPoolInput::TCookie cookie,
    const NChunkPools::TChunkStripePtr& newStripe)
{
    const auto& oldStripe = OriginalStripes_[cookie];
    YCHECK(oldStripe);

    if (Mode_ == EChunkMappingMode::Sorted) {
        if (oldStripe->DataSlices.size() != newStripe->DataSlices.size()) {
            THROW_ERROR_EXCEPTION("New stripe has different number of data slices")
                << TErrorAttribute("old_data_slice_count", oldStripe->DataSlices.size())
                << TErrorAttribute("new_data_slice_count", newStripe->DataSlices.size());
        }
    }

    for (int index = 0; index < oldStripe->DataSlices.size(); ++index) {
        const auto& oldSlice = oldStripe->DataSlices[index];
        // Versioned slices may not be lost and regenerated.
        YCHECK(oldSlice->Type == EDataSourceType::UnversionedTable);
        const auto& oldChunk = oldSlice->GetSingleUnversionedChunkOrThrow();

        // In case of unordered mode we distribtue the substitutes uniformly
        // among the original chunks.
        int begin = (index * newStripe->DataSlices.size()) / oldStripe->DataSlices.size();
        int end = ((index + 1) * newStripe->DataSlices.size()) / oldStripe->DataSlices.size();

        auto& substitutes = Substitutes_[oldChunk];
        substitutes.clear();
        substitutes.reserve(end - begin);

        for (int newIndex = begin; newIndex < end; ++newIndex) {
            const auto& newSlice = newStripe->DataSlices[index];
            YCHECK(newSlice->Type == EDataSourceType::UnversionedTable);
            const auto& newChunk = newSlice->GetSingleUnversionedChunkOrThrow();

            if (Mode_ == EChunkMappingMode::Sorted) {
                ValidateSortedChunkConsistency(oldChunk, newChunk);
            }

            substitutes.emplace_back(newChunk);
        }
    }
}

void TInputChunkMapping::ValidateSortedChunkConsistency(
    const TInputChunkPtr& oldChunk,
    const TInputChunkPtr& newChunk) const
{
    TNullable<TOwningBoundaryKeys> oldBoundaryKeys =
        oldChunk->BoundaryKeys() ? MakeNullable(*oldChunk->BoundaryKeys()) : Null;
    TNullable<TOwningBoundaryKeys> newBoundaryKeys =
        newChunk->BoundaryKeys() ? MakeNullable(*newChunk->BoundaryKeys()) : Null;
    if (oldBoundaryKeys != newBoundaryKeys) {
        // Due to some weird linkage error, I cannot use bare oldBoundaryKeys/newBoundaryKeys
        // as arguments in TErrorAttribute.

        TYsonString oldBoundaryKeysYson;
        if (oldBoundaryKeys) {
            oldBoundaryKeysYson = TYsonString(BuildYsonStringFluently()
                .Value(*oldBoundaryKeys)
                .GetData());
        } else {
            oldBoundaryKeysYson = TYsonString("#");
        }
        TYsonString newBoundaryKeysYson;
        if (newBoundaryKeys) {
            newBoundaryKeysYson = TYsonString(BuildYsonStringFluently()
                .Value(*newBoundaryKeys)
                .GetData());
        } else {
            newBoundaryKeysYson = TYsonString("#");
        }
        THROW_ERROR_EXCEPTION("Corresponding chunks in old and new stripes have different boundary keys")
            << TErrorAttribute("old_chunk_id", oldChunk->ChunkId())
            << TErrorAttribute("old_boundary_keys", oldBoundaryKeysYson)
            << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
            << TErrorAttribute("new_boundary_keys", newBoundaryKeysYson);
    }
    if (oldChunk->GetRowCount() != newChunk->GetRowCount()) {
        THROW_ERROR_EXCEPTION("Corresponding chunks in old and new stripes have different row counts")
            << TErrorAttribute("old_chunk_id", oldChunk->ChunkId())
            << TErrorAttribute("old_row_count", oldChunk->GetRowCount())
            << TErrorAttribute("new_chunk_id", newChunk->ChunkId())
            << TErrorAttribute("new_row_count", newChunk->GetRowCount());
    }
}

void TInputChunkMapping::OnChunkDisappeared(const TInputChunkPtr& chunk)
{
    Substitutes_[chunk].clear();
}

void TInputChunkMapping::Reset(IChunkPoolInput::TCookie resetCookie, const TChunkStripePtr& resetStripe)
{
    for (auto& pair : OriginalStripes_) {
        auto cookie = pair.first;
        auto& stripe = pair.second;
        if (cookie == resetCookie) {
            stripe = resetStripe;
        } else {
            stripe = GetMappedStripe(stripe);
        }
    }

    Substitutes_.clear();
}

void TInputChunkMapping::Add(IChunkPoolInput::TCookie cookie, const TChunkStripePtr& stripe)
{
    YCHECK(OriginalStripes_.insert(std::make_pair(cookie, stripe)).second);
}

void TInputChunkMapping::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, Substitutes_);
    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, OriginalStripes_);
    Persist(context, Mode_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

