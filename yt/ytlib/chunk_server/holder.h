#pragma once

#include "id.h"
#include "chunk_service.pb.h"

#include "chunk_service.pb.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EHolderState,
    // The holder had just registered but have not reported any heartbeats yet.
    (Inactive)
    // The holder is reporting heartbeats.
    // We have a proper knowledge of its chunk set.
    (Active)
);

class THolder
{
    DEFINE_BYVAL_RO_PROPERTY(THolderId, Id);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYVAL_RW_PROPERTY(EHolderState, State);
    DEFINE_BYREF_RW_PROPERTY(NProto::THolderStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, StoredChunkIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, CachedChunkIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, UnapprovedChunkIds);
    // TODO(babenko): consider using hashset
    DEFINE_BYREF_RO_PROPERTY(yvector<TJobId>, JobIds);

public:
    THolder(
        THolderId id,
        const Stroka& address,
        EHolderState state,
        const NProto::THolderStatistics& statistics);

    THolder(const THolder& other);

    TAutoPtr<THolder> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<THolder> Load(THolderId id, TInputStream* input);

    void AddChunk(const TChunkId& chunkId, bool cached);
    void RemoveChunk(const TChunkId& chunkId, bool cached);
    bool HasChunk(const TChunkId& chunkId, bool cached) const;

    void AddUnapprovedChunk(const TChunkId& chunkId);
    void RemoveUnapprovedChunk(const TChunkId& chunkId);
    bool HasUnapprovedChunk(const TChunkId& chunkId) const;
    void ApproveChunk(const TChunkId& chunkId);

    void AddJob(const TJobId& id);
    void RemoveJob(const TJobId& id);
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationSink
{
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobId>, JobIds);

public:
    explicit TReplicationSink(const Stroka &address)
        : Address_(address)
    { }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
