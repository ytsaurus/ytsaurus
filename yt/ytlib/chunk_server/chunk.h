#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/small_vector.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/object_server/object_detail.h>

#include <ytlib/chunk_holder/chunk.pb.h>
#include <ytlib/file_client/file_chunk_meta.pb.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RW_PROPERTY(i32, ReplicationFactor);
    DEFINE_BYVAL_RW_PROPERTY(i64, Size);
    DEFINE_BYVAL_RW_PROPERTY(TSharedRef, Attributes);

    // This is usually small, e.g. has the length of 3.
    typedef TSmallVector<THolderId, 3> TStoredLocations;
    DEFINE_BYREF_RO_PROPERTY(TStoredLocations, StoredLocations);

    // This list is usually empty.
    // Keeping a holder is very space efficient (takes just 8 bytes).
    DEFINE_BYREF_RO_PROPERTY(::THolder< yhash_set<THolderId> >, CachedLocations);

public:
    static const i64 UnknownSize = -1;
    
    explicit TChunk(const TChunkId& id);

    TChunkTreeStatistics GetStatistics() const;

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);

    void AddLocation(THolderId holderId, bool cached);
    void RemoveLocation(THolderId holderId, bool cached);
    yvector<THolderId> GetLocations() const;

    bool IsConfirmed() const;

    NChunkHolder::NProto::TChunkAttributes DeserializeAttributes() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
