#pragma once

#include "common.h"
#include "chunk.pb.h"

#include "../misc/property.h"
#include "../misc/serialize.h"
#include "../chunk_client/common.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunk
{
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TChunkId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TChunkListId, ChunkListId);
    DEFINE_BYVAL_RW_PROPERTY(i64, Size);
    DEFINE_BYVAL_RW_PROPERTY(TSharedRef, Attributes);
    // Usually small, e.g. 3 replicas.
    DEFINE_BYREF_RO_PROPERTY(yvector<THolderId>, StoredLocations);
    // Usually empty.
    DEFINE_BYREF_RO_PROPERTY(TAutoPtr< yhash_set<THolderId> >, CachedLocations);

public:
    static const i64 UnknownSize = -1;
    
    TChunk(const NChunkClient::TChunkId& id);

    TAutoPtr<TChunk> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TChunk> Load(const NChunkClient::TChunkId& id, TInputStream* input);

    void AddLocation(THolderId holderId, bool cached);
    void RemoveLocation(THolderId holderId, bool cached);
    yvector<THolderId> GetLocations() const;

    bool IsConfirmed() const;

    i32 Ref();
    i32 Unref();
    i32 GetRefCounter() const;

    NChunkHolder::NProto::TChunkAttributes DeserializeAttributes() const;

private:
    i32 RefCounter;

    TChunk(const TChunk& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
