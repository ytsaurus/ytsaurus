#pragma once

#include "public.h"

#include <yt/server/master/object_server/staged_object.h>


namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base class for TChunkList, TChunk and TChunkView.
class TChunkTree
    : public NObjectServer::TStagedObject
{
public:
    explicit TChunkTree(TChunkTreeId id);

    TChunkList* AsChunkList();
    const TChunkList* AsChunkList() const;

    TChunk* AsChunk();
    const TChunk* AsChunk() const;

    TChunkView* AsChunkView();
    const TChunkView* AsChunkView() const;

    TDynamicStore* AsDynamicStore();
    const TDynamicStore* AsDynamicStore() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
