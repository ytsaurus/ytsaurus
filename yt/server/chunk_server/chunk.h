#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/small_vector.h>

#include <ytlib/chunk_client/chunk.pb.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <server/cell_master/public.h>

#include <server/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkMeta, ChunkMeta);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::NProto::TChunkInfo, ChunkInfo);
    DEFINE_BYVAL_RW_PROPERTY(i16, ReplicationFactor);
    DEFINE_BYVAL_RW_PROPERTY(bool, Movable);

    typedef TSmallVector<TChunkList*, 2> TParents;
    DEFINE_BYREF_RW_PROPERTY(TParents, Parents);

    // This is usually small, e.g. has the length of 3.
    typedef TSmallVector<TNodeId, 3> TStoredLocations;
    DEFINE_BYREF_RO_PROPERTY(TStoredLocations, StoredLocations);

    // This list is usually empty.
    // Keeping a holder is very space efficient (takes just 8 bytes).
    DEFINE_BYREF_RO_PROPERTY(::THolder< yhash_set<TNodeId> >, CachedLocations);

public:
    static const i64 UnknownSize;

    explicit TChunk(const TChunkId& id);
    ~TChunk();

    TChunkTreeStatistics GetStatistics() const;

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    void AddLocation(TNodeId nodeId, bool cached);
    void RemoveLocation(TNodeId nodeId, bool cached);
    std::vector<TNodeId> GetLocations() const;

    bool ValidateChunkInfo(const NChunkClient::NProto::TChunkInfo& chunkInfo) const;
    bool IsConfirmed() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
