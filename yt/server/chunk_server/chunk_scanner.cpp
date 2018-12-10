#include "chunk_scanner.h"
#include "chunk.h"
#include "private.h"

#include <yt/server/object_server/object_manager.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkScanner::TChunkScanner(
    TObjectManagerPtr objectManager,
    EChunkScanKind kind)
    : ObjectManager_(std::move(objectManager))
    , Kind_(kind)
    , Logger(NLogging::TLogger(ChunkServerLogger)
        .AddTag("Kind: %", Kind_))
{ }

void TChunkScanner::Start(TChunk* frontChunk, int chunkCount)
{
    YCHECK(!GlobalIterator_);
    GlobalIterator_ = frontChunk;

    YCHECK(GlobalCount_ < 0);
    GlobalCount_ = chunkCount;

    LOG_INFO("Global chunk scan started (ChunkCount: %v)",
        GlobalCount_);
}

void TChunkScanner::OnChunkDestroyed(TChunk* chunk)
{
    if (chunk == GlobalIterator_) {
        AdvanceGlobalIterator();
    }
}

bool TChunkScanner::EnqueueChunk(TChunk* chunk)
{
    auto epoch = ObjectManager_->GetCurrentEpoch();
    if (chunk->GetScanFlag(Kind_, epoch)) {
        return false;
    }
    chunk->SetScanFlag(Kind_, epoch);
    ObjectManager_->EphemeralRefObject(chunk);
    Queue_.push({chunk, NProfiling::GetCpuInstant()});
    return true;
}

TChunk* TChunkScanner::DequeueChunk()
{
    if (GlobalIterator_) {
        auto* chunk = GlobalIterator_;
        AdvanceGlobalIterator();
        return IsObjectAlive(chunk) ? chunk : nullptr;
    }

    if (Queue_.empty()) {
        return nullptr;
    }

    auto* chunk = Queue_.front().Chunk;
    Queue_.pop();

    auto epoch = ObjectManager_->GetCurrentEpoch();

    bool alive = IsObjectAlive(chunk);
    if (alive) {
        Y_ASSERT(chunk->GetScanFlag(Kind_, epoch));
        chunk->ClearScanFlag(Kind_, epoch);
    }
    ObjectManager_->EphemeralUnrefObject(chunk);
    return alive ? chunk : nullptr;
}

bool TChunkScanner::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    if (GlobalIterator_) {
        return true;
    }

    if (!Queue_.empty() && Queue_.front().Instant < deadline) {
        return true;
    }

    return false;
}

int TChunkScanner::GetQueueSize() const
{
    return GlobalCount_ + static_cast<int>(Queue_.size());
}

void TChunkScanner::AdvanceGlobalIterator()
{
    YCHECK(GlobalCount_ > 0);
    --GlobalCount_;

    GlobalIterator_ = GlobalIterator_->GetNextScannedChunk(Kind_);
    if (!GlobalIterator_) {
        // NB: Some chunks could vanish during the scan so this is not
        // necessary zero.
        YCHECK(GlobalCount_ >= 0);
        LOG_INFO("Global chunk scan finished (VanishedChunkCount: %v)",
            GlobalCount_);
        GlobalCount_ = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
