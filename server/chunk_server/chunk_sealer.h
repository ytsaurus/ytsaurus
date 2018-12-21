#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public TRefCounted
{
public:
    TChunkSealer(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TChunkSealer();

    void Start(TChunk* frontJournalChunk, int journalChunkCount);
    void Stop();

    void ScheduleSeal(TChunk* chunk);

    void OnChunkDestroyed(NChunkServer::TChunk* chunk);

    int GetQueueSize() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkSealer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
