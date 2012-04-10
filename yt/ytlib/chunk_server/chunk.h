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
    DEFINE_BYVAL_RW_PROPERTY(i64, Size);
    DEFINE_BYVAL_RW_PROPERTY(TSharedRef, Attributes);
    // Usually small, e.g. 3 replicas.
    typedef TSmallVector<THolderId, 3> TStoredLocations;
    DEFINE_BYREF_RO_PROPERTY(TStoredLocations, StoredLocations);
    // Usually empty.
    DEFINE_BYREF_RO_PROPERTY(::THolder< yhash_set<THolderId> >, CachedLocations);

public:
    static const i64 UnknownSize = -1;
    
    TChunk(const TChunkId& id);

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
