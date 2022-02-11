#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/staged_object.h>


namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base class for TChunkList, TChunk, TChunkView, and TDynamicStore.
class TChunkTree
    : public NObjectServer::TStagedObject
{
public:
    using TStagedObject::TStagedObject;

    TChunkList* AsChunkList();
    const TChunkList* AsChunkList() const;

    TChunk* AsChunk();
    const TChunk* AsChunk() const;

    TChunkView* AsChunkView();
    const TChunkView* AsChunkView() const;

    TDynamicStore* AsDynamicStore();
    const TDynamicStore* AsDynamicStore() const;

    bool IsSealed() const;
    bool GetOverlayed() const;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
