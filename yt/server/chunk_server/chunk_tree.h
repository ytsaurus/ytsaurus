#pragma once

#include "public.h"

#include <server/object_server/staged_object.h>


namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base class for both TChunkList and TChunk.
class TChunkTree
    : public NObjectServer::TStagedObject
{
public:
    explicit TChunkTree(const TChunkTreeId& id);

    TChunkList* AsChunkList();
    const TChunkList* AsChunkList() const;

    TChunk* AsChunk();
    const TChunk* AsChunk() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
