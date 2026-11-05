#include "chunk_prolongator.h"

#include "chunk.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NLogging;
using namespace NYTree;

using TProlongedChunksSerializer = TMapSerializer<TDefaultSerializer, TObjectPtrSerializer<TRawNonversionedObjectPtrSerializer>>;

////////////////////////////////////////////////////////////////////////////////

constinit auto Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

int TChunkProlongator::ProlongChunk(
    TChunk* chunk,
    TChunkProlongationCookie prolongationCookie,
    TInstant prolongationInstant)
{
    if (!IsObjectAlive(chunk) || !Enable_) {
        return 0;
    }

    auto chunkId = chunk->GetId();

    // Gracefully trim over time.
    constexpr int MaxTrimmedElementCount = 16;
    int trimmedElementCount = 0;
    int displacedChunkCount = 0;

    // Trims at most one element unless the dynamic config changed recently.
    while (
        std::ssize(ProlongedChunksQueue_) >= MaxProlongedChunksQueueSize_ &&
        !ProlongedChunksQueue_.empty() &&
        ++trimmedElementCount <= MaxTrimmedElementCount)
    {
        auto [poppedChunkId, poppedCookie, _] = ProlongedChunksQueue_.front();
        ProlongedChunksQueue_.pop_front();
        displacedChunkCount += ProlongedChunks_.erase(std::pair(poppedChunkId, poppedCookie));
    }

    YT_TLOG_ALERT_IF(displacedChunkCount > 0,
        "Chunk prolongation queue is overflowing")
        .With("MaxQueueSize", MaxProlongedChunksQueueSize_);

    auto [it, inserted] = ProlongedChunks_.emplace(std::pair(chunkId, prolongationCookie), TChunkPtr(chunk));

    YT_TLOG_ALERT_UNLESS(inserted,
        "Chunk with the same prolongation cookie is already present in the prolongation map")
        .With("ChunkId", chunkId)
        .With("ProlongationCookie", prolongationCookie);

    if (inserted) {
        ProlongedChunksQueue_.emplace_back(chunkId, prolongationCookie, prolongationInstant);
    }

    return displacedChunkCount;
}

void TChunkProlongator::ExpireProlongedChunk(TChunkId chunkId, TChunkProlongationCookie prolongationCookie)
{
    // May be already displaced via queue overflow.
    ProlongedChunks_.erase(std::pair(chunkId, prolongationCookie));
}

bool TChunkProlongator::ShouldTrimQueue(TInstant expirationTime) const
{
    return !ProlongedChunksQueue_.empty() && std::get<TInstant>(ProlongedChunksQueue_.front()) <= expirationTime;
}

void TChunkProlongator::TrimQueue(TInstant expirationTime)
{
    auto it = std::find_if(
        ProlongedChunksQueue_.begin(),
        ProlongedChunksQueue_.end(),
        [&] (const TQueueItem& queueItem) {
            return std::get<TInstant>(queueItem) > expirationTime;
        });

    for (const auto& [chunkId, cookie, _] : std::ranges::subrange(ProlongedChunksQueue_.begin(), it)) {
        ProlongedChunks_.erase(std::pair(chunkId, cookie));
    }

    ProlongedChunksQueue_.erase(ProlongedChunksQueue_.begin(), it);
}

void TChunkProlongator::Reconfigure(const TDynamicChunkProlongatorConfigPtr& config)
{
    MaxProlongedChunksQueueSize_ = config->MaxQueueSize;
    Enable_ = config->Enable;

    // NB(sabdenovch): Even if the prolongator is disabled, the persistent state is not dropped.
}

void TChunkProlongator::Save(NCellMaster::TSaveContext& context) const
{
    TProlongedChunksSerializer::Save(context, ProlongedChunks_);
    TListSerializer<TTupleSerializer<TQueueItem>>::Save(context, ProlongedChunksQueue_);
}

void TChunkProlongator::Load(NCellMaster::TLoadContext& context)
{
    TProlongedChunksSerializer::Load(context, ProlongedChunks_);
    TListSerializer<TTupleSerializer<TQueueItem>>::Load(context, ProlongedChunksQueue_);
}

void TChunkProlongator::BuildOrchidYson(TFluentMap fluent) const
{
    fluent
        .Item("prolonged_chunk_map_size").Value(std::ssize(ProlongedChunks_))
        .Item("prolonged_chunk_queue_size").Value(std::ssize(ProlongedChunksQueue_));
}

void TChunkProlongator::Clear()
{
    ProlongedChunks_.clear();
    ProlongedChunksQueue_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
