#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkProlongator
{
public:
    void BuildOrchidYson(NYTree::TFluentMap fluent) const;

    // Returns a number of chunks displaced from the prolongation queue.
    int ProlongChunk(
        TChunk* chunk,
        TChunkProlongationCookie prolongationCookie,
        TInstant prolongationInstant);

    void ExpireProlongedChunk(TChunkId chunkId, TChunkProlongationCookie prolongationCookie);

    bool ShouldTrimQueue(TInstant expirationTime) const;
    void TrimQueue(TInstant expirationTime);

    void Reconfigure(const TDynamicChunkProlongatorConfigPtr& config);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Clear();

private:
    using TQueueItem = std::tuple<TChunkId, TChunkProlongationCookie, TInstant>;

    THashMap<std::pair<TChunkId, TChunkProlongationCookie>, TChunkPtr> ProlongedChunks_;
    // Contains a superset of ProlongedChunks_.
    // Prolongation instants within this FIFO queue are non-strictly increasing.
    std::deque<TQueueItem> ProlongedChunksQueue_;

    bool Enable_ = false;
    i64 MaxProlongedChunksQueueSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
