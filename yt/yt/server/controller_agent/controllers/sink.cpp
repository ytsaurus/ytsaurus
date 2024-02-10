#include "sink.h"

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

TSink::TSink(TOperationControllerBase* controller, int outputTableIndex)
    : Controller_(controller)
    , OutputTableIndex_(outputTableIndex)
{ }

IChunkPoolInput::TCookie TSink::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    YT_VERIFY(stripe->ChunkListId);
    auto& table = Controller_->OutputTables_[OutputTableIndex_];
    auto chunkListId = stripe->ChunkListId;

    if (table->TableUploadOptions.TableSchema->IsSorted() && Controller_->ShouldVerifySortedOutput()) {
        // We override the key suggested by the task with the one formed by the stripe boundary keys.
        YT_VERIFY(stripe->BoundaryKeys);
        key = stripe->BoundaryKeys;
    }

    if (Controller_->IsLegacyOutputLivePreviewSupported()) {
        Controller_->AttachToLivePreview(chunkListId, table->LivePreviewTableId);
    }

    Controller_->AttachToLivePreview(table->LivePreviewTableName, stripe);

    table->OutputChunkTreeIds.emplace_back(key, chunkListId);
    table->ChunkCount += stripe->GetStatistics().ChunkCount;

    const auto& Logger = Controller_->Logger;
    YT_LOG_DEBUG("Output stripe registered (Table: %v, ChunkListId: %v, Key: %v, ChunkCount: %v)",
        OutputTableIndex_,
        chunkListId,
        key,
        stripe->GetStatistics().ChunkCount);

    if (table->Dynamic) {
        for (auto& slice : stripe->DataSlices) {
            YT_VERIFY(slice->ChunkSlices.size() == 1);
            table->OutputChunks.push_back(slice->ChunkSlices[0]->GetInputChunk());
        }
    }

    return IChunkPoolInput::NullCookie;
}

IChunkPoolInput::TCookie TSink::Add(TChunkStripePtr stripe)
{
    return AddWithKey(stripe, TChunkStripeKey());
}

void TSink::Suspend(TCookie /*cookie*/)
{
    YT_ABORT();
}

void TSink::Resume(TCookie /*cookie*/)
{
    YT_ABORT();
}

void TSink::Reset(
    TCookie /*cookie*/,
    TChunkStripePtr /*stripe*/,
    TInputChunkMappingPtr /*mapping*/)
{
    YT_ABORT();
}

void TSink::Finish()
{
    // Mmkay. Don't know what to do here though :)
}

bool TSink::IsFinished() const
{
    return false;
}

void TSink::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Controller_);
    Persist(context, OutputTableIndex_);
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
