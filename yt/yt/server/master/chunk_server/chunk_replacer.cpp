#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_replacer.h"
#include "chunk_tree.h"
#include "chunk.h"

#include <stack>

namespace NYT::NChunkServer {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TChunkReplacer::TChunkReplacer(
    IChunkReplacerCallbacksPtr chunkReplacerCallbacks,
    NLogging::TLogger logger)
    : ChunkReplacerCallbacks_(std::move(chunkReplacerCallbacks))
    , Logger(std::move(logger))
{ }

bool TChunkReplacer::Replace(
    TChunkList* oldChunkList,
    TChunkList* newChunkList,
    TChunk* newChunk,
    const std::vector<TChunkId>& oldChunkIds)
{
    std::stack<TTraversalStateEntry> stack;
    stack.push({oldChunkList, 0});

    auto oldChunkIndex = 0;

    enum class EProcessChunkResult
    {
        Advance,
        Skip,
        Unknown
    };

    auto processChunk = [&] (TChunk* chunk) {
        if (oldChunkIndex < oldChunkIds.size() && oldChunkIds[oldChunkIndex] == chunk->GetId()) {
            ++oldChunkIndex;
            return EProcessChunkResult::Advance;
        }

        if (oldChunkIndex == oldChunkIds.size() || oldChunkIndex == 0) {
            return EProcessChunkResult::Skip;
        }

        return EProcessChunkResult::Unknown;
    };

    auto isDynamicTableChunkTree = [&] (TChunkTree* chunkTree) {
        if (chunkTree->GetType() == EObjectType::ChunkView ||
            chunkTree->GetType() == EObjectType::SortedDynamicTabletStore || 
            chunkTree->GetType() == EObjectType::OrderedDynamicTabletStore)
        {
            YT_LOG_ALERT_IF(ChunkReplacerCallbacks_->IsMutationLoggingEnabled(), "Unexpected chunk tree type (Type: %v, Id: %v)",
                chunkTree->GetType(),
                chunkTree->GetId());
            YT_VERIFY(oldChunkIndex == oldChunkIds.size() || oldChunkIndex == 0);
            return true;
        }
        return false;
    };
    
    while (!stack.empty()) {
        auto& entry = stack.top();
        auto* chunkTree = entry.ChunkTree;

        if (chunkTree->GetType() == EObjectType::ChunkList) {
            auto* chunkList = chunkTree->AsChunkList();

            // Chunks are already replaced, just skip.
            if (oldChunkIndex == oldChunkIds.size() && entry.Index == 0) {
                ChunkReplacerCallbacks_->AttachToChunkList(newChunkList, chunkList);
                stack.pop();
                continue;
            }

            if (chunkList->Statistics().Rank > 1) {
                if (entry.Index < static_cast<int>(chunkList->Children().size())) {
                    stack.push({chunkList->Children()[entry.Index], 0});
                    ++entry.Index;
                    continue;
                }
            } else {
                YT_VERIFY(entry.Index == 0 && oldChunkIndex < oldChunkIds.size());

                auto firstChunkToReplace = -1;
                auto lastChunkToReplace = -1;
                for (auto i = 0; i < static_cast<int>(chunkList->Children().size()); ++i) {
                    const auto& child = chunkList->Children()[i];
                    if (isDynamicTableChunkTree(child)) {
                        return false;
                    }

                    YT_VERIFY(child->GetType() == EObjectType::Chunk);
                    auto childChunk = child->AsChunk();
                    
                    auto result = processChunk(childChunk);
                    if (result == EProcessChunkResult::Advance) {
                        if (firstChunkToReplace == -1) {
                            firstChunkToReplace = i;
                        }
                        lastChunkToReplace = i;
                    } else if (result == EProcessChunkResult::Unknown) {
                        YT_LOG_ALERT_IF(ChunkReplacerCallbacks_->IsMutationLoggingEnabled(), "Could not replace chunks: unexpected chunk (ChunkId: %v, ChunkListId: %v, Index: %v)",
                            childChunk->GetId(),
                            oldChunkList->GetId(),
                            oldChunkIndex);
                        return false;
                    }
                }

                if (firstChunkToReplace == -1) {
                    // There were no chunks to replace in this chunk list.
                    ChunkReplacerCallbacks_->AttachToChunkList(newChunkList, chunkList);
                } else {
                    // ...[
                    ChunkReplacerCallbacks_->AttachToChunkList(
                        newChunkList,
                        chunkList->Children().begin(),
                        chunkList->Children().begin() + firstChunkToReplace);

                    // ...[...]
                    if (oldChunkIndex == oldChunkIds.size()) {
                        ChunkReplacerCallbacks_->AttachToChunkList(newChunkList, newChunk);
                    }
                    if (lastChunkToReplace + 1 < static_cast<int>(chunkList->Children().size())) {
                        // ...[...]...
                        ChunkReplacerCallbacks_->AttachToChunkList(
                            newChunkList,
                            chunkList->Children().begin() + lastChunkToReplace + 1,
                            chunkList->Children().end());
                    }
                }
            }
            stack.pop();
        } else {
            if (isDynamicTableChunkTree(chunkTree)) {
                return false;
            }

            YT_VERIFY(chunkTree->GetType() == EObjectType::Chunk);
            auto chunk = chunkTree->AsChunk();
            
            auto result = processChunk(chunk);
            if (result == EProcessChunkResult::Advance) {
                if (oldChunkIndex == oldChunkIds.size()) {
                    ChunkReplacerCallbacks_->AttachToChunkList(newChunkList, newChunk);
                }
            } else if (result == EProcessChunkResult::Unknown) {
                YT_LOG_ALERT_IF(ChunkReplacerCallbacks_->IsMutationLoggingEnabled(), "Could not replace chunks: unexpected chunk (ChunkId: %v, ChunkListId: %v, Index: %v)",
                    chunk->GetId(),
                    oldChunkList->GetId(),
                    oldChunkIndex);
                return false;
            } else {
                ChunkReplacerCallbacks_->AttachToChunkList(newChunkList, chunk);
            }
            stack.pop();
        }
    }

    return oldChunkIndex == oldChunkIds.size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
