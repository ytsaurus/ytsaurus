#include "chunk_scanner.h"
#include "chunk.h"
#include "private.h"

#include <yt/yt/server/master/object_server/object_manager.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkScanner::TChunkScanner(
    IObjectManagerPtr objectManager,
    EChunkScanKind kind,
    bool journal)
    : ObjectManager_(std::move(objectManager))
    , Kind_(kind)
    , Journal_(journal)
    , Logger(ChunkServerLogger.WithTag("Kind: %v, Journal: %v",
        Kind_,
        Journal_))
{ }

void TChunkScanner::Start(TGlobalChunkScanDescriptor descriptor)
{
    YT_VERIFY(!GlobalIterator_);
    YT_VERIFY(GlobalCount_ == 0);

    ScheduleGlobalScan(descriptor);

    YT_VERIFY(!std::exchange(Running_, true));
}

void TChunkScanner::ScheduleGlobalScan(TGlobalChunkScanDescriptor descriptor)
{
    GlobalIterator_ = descriptor.FrontChunk;
    GlobalCount_ = descriptor.ChunkCount;

    YT_VERIFY(!IsObjectAlive(GlobalIterator_) || GlobalIterator_->IsJournal() == Journal_);

    YT_LOG_INFO("Global chunk scan started (ChunkCount: %v)",
        GlobalCount_);
}

void TChunkScanner::Stop()
{
    if (!Running_) {
        return;
    }

    YT_LOG_INFO("Global chunk scan stopped (RemainingChunkCount: %v)",
        GetQueueSize());

    // Clear global chunk scan state.
    GlobalIterator_ = nullptr;
    GlobalCount_ = 0;

    // NB: Queue may be huge, so we offload its destruction into heavy thread.
    std::queue<TQueueEntry> queue;
    std::swap(Queue_, queue);
    NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
        BIND([queue = std::move(queue)] { Y_UNUSED(queue); }));

    YT_VERIFY(std::exchange(Running_, false));
}

void TChunkScanner::OnChunkDestroyed(TChunk* chunk)
{
    if (chunk == GlobalIterator_) {
        AdvanceGlobalIterator();
    }
}

bool TChunkScanner::EnqueueChunk(TChunk* chunk)
{
    if (!Running_) {
        return false;
    }

    if (chunk->GetScanFlag(Kind_)) {
        return false;
    }
    chunk->SetScanFlag(Kind_);
    Queue_.push({
        TEphemeralObjectPtr<TChunk>(chunk),
        NProfiling::GetCpuInstant()
    });
    return true;
}

TChunk* TChunkScanner::DequeueChunk()
{
    YT_VERIFY(Running_);

    if (GlobalIterator_) {
        auto* chunk = GlobalIterator_;
        AdvanceGlobalIterator();
        return IsObjectAlive(chunk) ? chunk : nullptr;
    }

    if (Queue_.empty()) {
        return nullptr;
    }

    auto* chunk = Queue_.front().Chunk.Get();
    bool alive = IsObjectAlive(chunk);
    if (alive) {
        YT_ASSERT(chunk->GetScanFlag(Kind_));
        chunk->ClearScanFlag(Kind_);
    }
    Queue_.pop();
    return alive ? chunk : nullptr;
}

bool TChunkScanner::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    YT_VERIFY(Running_);

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
    YT_VERIFY(GlobalCount_ > 0);
    --GlobalCount_;

    GlobalIterator_ = GlobalIterator_->GetNextScannedChunk();
    if (!GlobalIterator_) {
        // NB: Some chunks could vanish during the scan so this is not
        // necessarily zero.
        YT_VERIFY(GlobalCount_ >= 0);
        YT_LOG_INFO("Global chunk scan finished (VanishedChunkCount: %v)",
            GlobalCount_);
        GlobalCount_ = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
