#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/small_vector.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/chunk_holder/chunk.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYREF_RW_PROPERTY(NChunkHolder::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RW_PROPERTY(NChunkHolder::NProto::TChunkInfo, ChunkInfo);

    // This is usually small, e.g. has the length of 3.
    // typedef TSmallVector<THolderId, 3> TStoredLocations;
    // TODO(babenko): switch to small vector when it's ready
    typedef yvector<THolderId> TStoredLocations;
    DEFINE_BYREF_RO_PROPERTY(TStoredLocations, StoredLocations);

    // This list is usually empty.
    // Keeping a holder is very space efficient (takes just 8 bytes).
    DEFINE_BYREF_RO_PROPERTY(::THolder< yhash_set<THolderId> >, CachedLocations);

public:
    static const i64 UnknownSize;

    TChunk(const TChunkId& id);

    ~TChunk();

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);

    void AddLocation(THolderId holderId, bool cached);
    void RemoveLocation(THolderId holderId, bool cached);
    yvector<THolderId> GetLocations() const;

    bool ValidateChunkInfo(const NChunkHolder::NProto::TChunkInfo& chunkInfo) const;
    bool IsConfirmed() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
