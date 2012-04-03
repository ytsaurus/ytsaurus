#pragma once

#include "public.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeRef
{
public:
    explicit TChunkTreeRef(TChunk* chunk);
    explicit TChunkTreeRef(TChunkList* chunkList);

    bool operator == (const TChunkTreeRef& other) const;
    NObjectServer::EObjectType GetType() const;

    TChunk* AsChunk() const;
    TChunkList* AsChunkList() const;

    TChunkTreeId GetId() const;

    friend struct ::hash<TChunkTreeRef>;

private:
    uintptr_t Pointer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

template <>
struct hash<NYT::NChunkServer::TChunkTreeRef>
{
    inline size_t operator()(const NYT::NChunkServer::TChunkTreeRef& chunkRef) const
    {
        return hash<uintptr_t>()(chunkRef.Pointer);
    }
};
