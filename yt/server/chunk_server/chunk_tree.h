#pragma once

#include "public.h"

#include <server/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Base class for both TChunkList and TChunk.
class TChunkTree
    : public NObjectServer::TUnversionedObjectBase
{
public:
    explicit TChunkTree(const TChunkTreeId& id);

    TChunkList* AsChunkList();
    const TChunkList* AsChunkList() const;

    TChunk* AsChunk();
    const TChunk* AsChunk() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
