#pragma once

#include "common.h"
#include "id.h"
#include "chunk.pb.h"

#include <yt/ytlib/misc/property.h>
#include <yt/ytlib/object_server/object_detail.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
    : public NObjectServer::TObjectWithIdBase
{
    DEFINE_BYVAL_RW_PROPERTY(i64, Size);
    DEFINE_BYVAL_RW_PROPERTY(TSharedRef, Attributes);
    // Usually small, e.g. 3 replicas.
    DEFINE_BYREF_RO_PROPERTY(yvector<THolderId>, StoredLocations);
    // Usually empty.
    DEFINE_BYREF_RO_PROPERTY(TAutoPtr< yhash_set<THolderId> >, CachedLocations);

public:
    static const i64 UnknownSize = -1;
    
    TChunk(const TChunkId& id);

    TAutoPtr<TChunk> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunk> Load(const TChunkId& id, TInputStream* input);

    void AddLocation(THolderId holderId, bool cached);
    void RemoveLocation(THolderId holderId, bool cached);
    yvector<THolderId> GetLocations() const;

    bool IsConfirmed() const;

    NChunkHolder::NProto::TChunkAttributes DeserializeAttributes() const;

private:
    TChunk(const TChunk& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
