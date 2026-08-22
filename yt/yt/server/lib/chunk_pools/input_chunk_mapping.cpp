#include "input_chunk_mapping.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NTableClient;
using namespace NThreading;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger Logger("IdentityChunkMapping");
TInputChunkMappingPtr IdentityChunkMapping = New<TInputChunkMapping>(EChunkMappingMode::Unordered, Logger);

////////////////////////////////////////////////////////////////////////////////

TInputChunkMapping::TInputChunkMapping(EChunkMappingMode mode, NLogging::TLogger logger)
    : Mode_(mode)
    , Logger(logger)
{ }

TChunkStripePtr TInputChunkMapping::GetMappedStripe(const TChunkStripePtr& stripe) const
{
    auto guard = ReaderGuard(SpinLock_);
    return GetMappedStripeGuarded(stripe);
}

TChunkStripePtr TInputChunkMapping::GetMappedStripeGuarded(const TChunkStripePtr& stripe) const
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

    YT_VERIFY(stripe);

    if (Substitutes_.empty()) {
        return stripe;
    }

    auto mappedStripe = New<TChunkStripe>();
    for (const auto& dataSlice : stripe->DataSlices()) {
        if (dataSlice->Type == EDataSourceType::UnversionedTable) {
            const auto& chunk = dataSlice->GetSingleUnversionedChunk();
            auto iterator = Substitutes_.find(chunk);
            if (iterator == Substitutes_.end()) {
                // The chunk was never substituted, so it remains as is.
                mappedStripe->DataSlices().push_back(dataSlice);
            } else {
                const auto& substitutes = iterator->second;
                if (substitutes.empty()) {
                    continue;
                }

                if (dataSlice->IsLegacy) {
                    // COMPAT(max42): keeping old code as is to ensure old behavior.
                    if (dataSlice->HasLimits()) {
                        YT_VERIFY(substitutes.size() == 1);
                        auto substituteChunk = substitutes.front();
                        YT_TLOG_DEBUG("Input chunk mapping has mapped a chunk")
                            .With("From", chunk->GetChunkId())
                            .With("To", substituteChunk->GetChunkId())
                            .With("Legacy", true)
                            .With("Single", true);
                        auto chunkSlice = CreateInputChunkSlice(substituteChunk);
                        chunkSlice->LegacyLowerLimit() = dataSlice->ChunkSlices[0]->LegacyLowerLimit();
                        chunkSlice->LegacyUpperLimit() = dataSlice->ChunkSlices[0]->LegacyUpperLimit();
                        mappedStripe->DataSlices().push_back(New<TLegacyDataSlice>(
                            dataSlice->Type,
                            TLegacyDataSlice::TChunkSliceList{std::move(chunkSlice)},
                            dataSlice->LegacyLowerLimit(),
                            dataSlice->LegacyUpperLimit()));
                        mappedStripe->DataSlices().back()->SetInputStreamIndex(dataSlice->GetInputStreamIndex());
                    } else {
                        for (const auto& substituteChunk : substitutes) {
                            YT_TLOG_DEBUG("Input chunk mapping has mapped a chunk")
                                .With("From", chunk->GetChunkId())
                                .With("To", substituteChunk->GetChunkId())
                                .With("Legacy", true)
                                .With("Single", false);
                            mappedStripe->DataSlices().push_back(New<TLegacyDataSlice>(
                                dataSlice->Type,
                                TLegacyDataSlice::TChunkSliceList{CreateInputChunkSlice(substituteChunk)}));
                            mappedStripe->DataSlices().back()->SetInputStreamIndex(dataSlice->GetInputStreamIndex());
                        }
                    }
                } else {
                    if (dataSlice->HasLimits()) {
                        YT_VERIFY(substitutes.size() == 1);
                        auto substituteChunk = substitutes.front();
                        YT_TLOG_DEBUG("Input chunk mapping has mapped a chunk")
                            .With("From", chunk->GetChunkId())
                            .With("To", substituteChunk->GetChunkId())
                            .With("Legacy", false)
                            .With("Single", true);

                        auto mappedDataSlice = CreateInputDataSlice(dataSlice);
                        mappedDataSlice->ChunkSlices[0]->SetInputChunk(substituteChunk);
                        mappedDataSlice->CopyPayloadFrom(*dataSlice);
                        mappedStripe->DataSlices().push_back(std::move(mappedDataSlice));
                    } else {
                        for (const auto& substituteChunk : substitutes) {
                            YT_TLOG_DEBUG("Input chunk mapping has mapped a chunk")
                                .With("From", chunk->GetChunkId())
                                .With("To", substituteChunk->GetChunkId())
                                .With("Legacy", false)
                                .With("Single", false);
                            auto mappedDataSlice = CreateInputDataSlice(dataSlice);
                            mappedDataSlice->ChunkSlices[0]->SetInputChunk(substituteChunk);
                            mappedDataSlice->CopyPayloadFrom(*dataSlice);
                            mappedStripe->DataSlices().push_back(std::move(mappedDataSlice));
                        }
                    }
                }
            }
        } else {
            // Let's hope versioned chunks are never lost nor regenerated.
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                YT_VERIFY(!Substitutes_.contains(chunkSlice->GetInputChunk()));
            }
            mappedStripe->DataSlices().push_back(dataSlice);
        }
    }

    return mappedStripe;
}

void TInputChunkMapping::OnStripeRegenerated(
    IChunkPoolInput::TCookie cookie,
    const NChunkPools::TChunkStripePtr& newStripe)
{
    auto guard = WriterGuard(SpinLock_);

    YT_TLOG_DEBUG("Input chunk mapping has regenerated the stripe")
        .With("Cookie", cookie);

    YT_VERIFY(cookie != IChunkPoolInput::NullCookie);
    const auto& oldStripe = OriginalStripes_[cookie];
    YT_VERIFY(oldStripe);

    // NB(gritukan, max42): YT-14252.
    if (Mode_ == EChunkMappingMode::SortedWithoutKeyGuarantree) {
        THROW_ERROR_EXCEPTION("Chunk mapping invalidation cannot be reliably checked when key guarantee is disabled");
    }

    if (Mode_ == EChunkMappingMode::Sorted) {
        if (oldStripe->DataSlices().size() != newStripe->DataSlices().size()) {
            THROW_ERROR_EXCEPTION("New stripe has different number of data slices")
                .With("old_data_slice_count", oldStripe->DataSlices().size())
                .With("new_data_slice_count", newStripe->DataSlices().size());
        }

        for (int index = 0; index < std::ssize(oldStripe->DataSlices()); ++index) {
            const auto& oldChunk = oldStripe->DataSlices()[index]->GetSingleUnversionedChunk();
            const auto& newChunk = newStripe->DataSlices()[index]->GetSingleUnversionedChunk();
            ValidateSortedChunkConsistency(oldChunk, newChunk);
        }
    }

    for (int index = 0; index < std::ssize(oldStripe->DataSlices()); ++index) {
        const auto& oldSlice = oldStripe->DataSlices()[index];
        // Versioned slices may not be lost and regenerated.
        YT_VERIFY(oldSlice->Type == EDataSourceType::UnversionedTable);
        const auto& oldChunk = oldSlice->GetSingleUnversionedChunk();

        // In case of unordered mode we distribute the substitutes uniformly
        // among the original chunks.
        int begin = (index * newStripe->DataSlices().size()) / oldStripe->DataSlices().size();
        int end = ((index + 1) * newStripe->DataSlices().size()) / oldStripe->DataSlices().size();

        auto& substitutes = Substitutes_[oldChunk];
        substitutes.clear();
        substitutes.reserve(end - begin);

        for (int newIndex = begin; newIndex < end; ++newIndex) {
            const auto& newChunk = newStripe->DataSlices()[newIndex]->GetSingleUnversionedChunk();
            YT_TLOG_DEBUG("Input chunk mapping has added a substitute")
                .With("Cookie", cookie)
                .With("From", oldChunk->GetChunkId())
                .With("To", newChunk->GetChunkId());
            substitutes.emplace_back(newChunk);
        }
    }
}

void TInputChunkMapping::ValidateSortedChunkConsistency(
    const TInputChunkPtr& oldChunk,
    const TInputChunkPtr& newChunk)
{
    std::optional<TOwningBoundaryKeys> oldBoundaryKeys =
        oldChunk->BoundaryKeys() ? std::make_optional(*oldChunk->BoundaryKeys()) : std::nullopt;
    std::optional<TOwningBoundaryKeys> newBoundaryKeys =
        newChunk->BoundaryKeys() ? std::make_optional(*newChunk->BoundaryKeys()) : std::nullopt;
    if (oldBoundaryKeys != newBoundaryKeys) {
        // Due to some weird linkage error, I cannot use bare oldBoundaryKeys/newBoundaryKeys
        // as arguments in TErrorAttribute.

        TYsonString oldBoundaryKeysYson;
        if (oldBoundaryKeys) {
            oldBoundaryKeysYson = BuildYsonStringFluently()
                .Value(*oldBoundaryKeys);
        } else {
            oldBoundaryKeysYson = TYsonString(TStringBuf("#"));
        }
        TYsonString newBoundaryKeysYson;
        if (newBoundaryKeys) {
            newBoundaryKeysYson = BuildYsonStringFluently()
                .Value(*newBoundaryKeys);
        } else {
            newBoundaryKeysYson = TYsonString(TStringBuf("#"));
        }
        THROW_ERROR_EXCEPTION("Corresponding chunks in old and new stripes have different boundary keys")
            .With("old_chunk_id", oldChunk->GetChunkId())
            .With("old_boundary_keys", oldBoundaryKeysYson)
            .With("new_chunk_id", newChunk->GetChunkId())
            .With("new_boundary_keys", newBoundaryKeysYson);
    }
    if (oldChunk->GetRowCount() != newChunk->GetRowCount()) {
        THROW_ERROR_EXCEPTION("Corresponding chunks in old and new stripes have different row counts")
            .With("old_chunk_id", oldChunk->GetChunkId())
            .With("old_row_count", oldChunk->GetRowCount())
            .With("new_chunk_id", newChunk->GetChunkId())
            .With("new_row_count", newChunk->GetRowCount());
    }
}

void TInputChunkMapping::OnChunkDisappeared(const TInputChunkPtr& chunk)
{
    auto guard = WriterGuard(SpinLock_);

    YT_TLOG_DEBUG("Input chunk mapping has registered a chunk disappearance")
        .With("Chunk", chunk->GetChunkId());
    Substitutes_[chunk].clear();
}

void TInputChunkMapping::Reset(IChunkPoolInput::TCookie resetCookie, const TChunkStripePtr& resetStripe)
{
    auto guard = WriterGuard(SpinLock_);

    YT_TLOG_DEBUG("Input chunk mapping has been reset")
        .With("Cookie", resetCookie);
    for (auto& [cookie, stripe] : OriginalStripes_) {
        if (cookie == resetCookie) {
            stripe = resetStripe;
        } else {
            stripe = GetMappedStripeGuarded(stripe);
        }
    }

    Substitutes_.clear();
}

void TInputChunkMapping::Add(IChunkPoolInput::TCookie cookie, const TChunkStripePtr& stripe)
{
    auto guard = WriterGuard(SpinLock_);

    YT_TLOG_DEBUG("Input chunk mapping has added a cookie")
        .With("Cookie", cookie);
    YT_VERIFY(OriginalStripes_.emplace(cookie, stripe).second);
}

void TInputChunkMapping::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Substitutes_,
        .template Serializer<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>());
    PHOENIX_REGISTER_FIELD(2, OriginalStripes_,
        .template Serializer<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>());
    PHOENIX_REGISTER_FIELD(3, Mode_);
    PHOENIX_REGISTER_FIELD(4, Logger);
}

PHOENIX_DEFINE_TYPE(TInputChunkMapping);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

