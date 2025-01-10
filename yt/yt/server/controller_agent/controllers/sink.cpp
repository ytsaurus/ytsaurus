#include "sink.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

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

    if (Controller_->GetOutputLivePreviewVertexDescriptor() == TDataFlowGraph::SinkDescriptor) {
        Controller_->AttachToLivePreview(table->LivePreviewTableName, stripe);
    }

    table->ChunkCount += stripe->GetStatistics().ChunkCount;

    const auto& Logger = Controller_->Logger;
    YT_LOG_DEBUG("Output stripe registered (Table: %v, ChunkListId: %v, Key: %v, ChunkCount: %v)",
        OutputTableIndex_,
        chunkListId,
        key,
        stripe->GetStatistics().ChunkCount);

    bool isHunk = false;
    if (table->Dynamic) {
        for (auto& slice : stripe->DataSlices) {
            YT_VERIFY(slice->ChunkSlices.size() == 1);
            const auto& chunk = slice->ChunkSlices[0]->GetInputChunk();
            isHunk |= chunk->IsHunk();

            if (isHunk) {
                YT_VERIFY(table->Schema->HasHunkColumns());
                table->OutputHunkChunks.push_back(chunk);
            } else {
                table->OutputChunks.push_back(chunk);
            }
        }
    }

    if (isHunk) {
        table->OutputHunkChunkTreeIds.emplace_back(key, chunkListId);
    } else {
        table->OutputChunkTreeIds.emplace_back(key, chunkListId);
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

void TSink::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Controller_)();
    PHOENIX_REGISTER_FIELD(2, OutputTableIndex_)();
}

PHOENIX_DEFINE_TYPE(TSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
