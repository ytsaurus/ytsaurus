#pragma once

#include "id.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk;
class TChunkList;

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeRef
{
public:
    explicit TChunkTreeRef(TChunk* chunk);
    explicit TChunkTreeRef(TChunkList* chunkList);

    NObjectServer::EObjectType GetType() const;

    TChunk* AsChunk() const;
    TChunkList* AsChunkList() const;

    TChunkTreeId GetId() const;

private:
    uintptr_t Pointer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
