#pragma once

#include "public.h"

#include <server/object_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Acts as a compact type-safe handle that either points nowhere,
//! to a chunk, or to a chunk list.
//! The actual type is stored in the lowest bits.
class TChunkTreeRef
{
public:
    TChunkTreeRef();
    TChunkTreeRef(TChunk* chunk);
    TChunkTreeRef(TChunkList* chunkList);

    bool operator == (const TChunkTreeRef& other) const;
    bool operator != (const TChunkTreeRef& other) const;

    NObjectClient::EObjectType GetType() const;

    TChunk* AsChunk() const;
    TChunkList* AsChunkList() const;

    TChunkTreeId GetId() const;

private:
    friend struct ::hash<TChunkTreeRef>;

    uintptr_t Cookie;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

// Hashing helper.

template <>
struct hash<NYT::NChunkServer::TChunkTreeRef>
{
    inline size_t operator()(const NYT::NChunkServer::TChunkTreeRef& chunkRef) const
    {
        return hash<uintptr_t>()(chunkRef.Cookie);
    }
};

////////////////////////////////////////////////////////////////////////////////

// TObjectIdTraits and GetObjectId specializations.

namespace NYT {
namespace NObjectServer {

template <>
struct TObjectIdTraits<NChunkServer::TChunkTreeRef, void>
{
    typedef TObjectId TId;
};

inline TObjectId GetObjectId(const NChunkServer::TChunkTreeRef& object)
{
    return object.GetId();
}

} // namespace NObjectServer
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
