#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_replacer.h"
#include "chunk_tree.h"
#include "chunk.h"

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TChunkReplacer::TChunkReplacer(
    IChunkReplacerCallbacksPtr chunkReplacerCallbacks,
    NLogging::TLogger logger)
    : ChunkReplacerCallbacks_(std::move(chunkReplacerCallbacks))
    , Logger(std::move(logger))
    , NewRootChunkList_(ChunkReplacerCallbacks_->CreateChunkList(EChunkListKind::Static))
{
    // TODO(aleksandra-zh): optimize.
    ChunkReplacerCallbacks_->RefObject(NewRootChunkList_);
}

TChunkReplacer::~TChunkReplacer()
{
    ChunkReplacerCallbacks_->UnrefObject(NewRootChunkList_);
}

bool TChunkReplacer::FindChunkList(
    TChunkList* rootChunkList,
    TChunkListId desiredChunkListId)
{
    Stack_.push({rootChunkList, 0});

    while (!Stack_.empty()) {
        auto& entry = Stack_.top();
        auto* chunkTree = entry.ChunkTree;

        if (chunkTree->GetType() != EObjectType::ChunkList) {
            if (chunkTree->GetType() != EObjectType::Chunk) {
                return false;
            }

            ChunkReplacerCallbacks_->AttachToChunkList(NewRootChunkList_, chunkTree);
            Stack_.pop();
            continue;
        }

        auto* chunkList = chunkTree->AsChunkList();
        if (chunkList->GetId() == desiredChunkListId) {
            PrevParentChunkList_ = chunkList;
            NewParentChunkList_ = ChunkReplacerCallbacks_->CreateChunkList(EChunkListKind::Static);
            ChunkReplacerCallbacks_->AttachToChunkList(NewRootChunkList_, NewParentChunkList_);
        } else if (chunkList->Statistics().Rank > 1) {
            if (entry.Index < std::ssize(chunkList->Children())) {
                Stack_.push({chunkList->Children()[entry.Index], 0});
                ++entry.Index;
                continue;
            }
        } else {
            ChunkReplacerCallbacks_->AttachToChunkList(NewRootChunkList_, chunkList);
        }

        Stack_.pop();
    }
    Initialized_ = PrevParentChunkList_ != nullptr;
    return Initialized_;
}

bool TChunkReplacer::ReplaceChunkSequence(
    TChunk* newChunk,
    const std::vector<TChunkId>& chunksToReplaceIds)
{
    if (!Initialized_) {
        return false;
    }

    YT_VERIFY(!chunksToReplaceIds.empty());

    int chunkToReplaceIndex = 0;
    std::vector<TChunkTree*> chunkStash;
    auto flush = [&] {
        for (auto* chunk : chunkStash) {
            ChunkReplacerCallbacks_->AttachToChunkList(NewParentChunkList_, chunk);
        }
    };
    while (ChunkListIndex_ < std::ssize(PrevParentChunkList_->Children())) {
        const auto& child = PrevParentChunkList_->Children()[ChunkListIndex_++];
        if (child->GetId() == chunksToReplaceIds[chunkToReplaceIndex]) {
            chunkStash.push_back(child);
            ++chunkToReplaceIndex;
        } else {
            if (chunkToReplaceIndex > 0) {
                flush();
                ChunkReplacerCallbacks_->AttachToChunkList(NewParentChunkList_, child);
                return false;
            }
            ChunkReplacerCallbacks_->AttachToChunkList(NewParentChunkList_, child);
        }

        if (chunkToReplaceIndex == std::ssize(chunksToReplaceIds)) {
            ChunkReplacerCallbacks_->AttachToChunkList(NewParentChunkList_, newChunk);
            return true;
        }
    }

    flush();
    return false;
}

TChunkList* TChunkReplacer::Finish()
{
    if (!Initialized_) {
        return nullptr;
    }

    // Flush the remaining chunks in PrevParentChunkList_.
    YT_VERIFY(!ReplaceChunkSequence(nullptr, {NullChunkId}));
    YT_VERIFY(ChunkListIndex_ == std::ssize(PrevParentChunkList_->Children()));

    return NewRootChunkList_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
