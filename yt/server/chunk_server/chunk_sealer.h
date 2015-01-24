#pragma once

#include "public.h"

#include <server/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkSealer
    : public TRefCounted
{
public:
    TChunkSealer(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TChunkSealer();

    void Start();

    void MaybeScheduleSeal(TChunk* chunk);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkSealer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
