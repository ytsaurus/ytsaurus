#pragma once

#include "public.h"
#include "chunk_service.pb.h"

#include <ytlib/misc/property.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EHolderState,
    // Not registered.
    (Offline)
    // Registered but did not report the full heartbeat yet.
    (Registered)
    // Registered and reported the full heartbeat.
    (Online)
);

class THolder
{
    DEFINE_BYVAL_RO_PROPERTY(THolderId, Id);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYVAL_RO_PROPERTY(TIncarnationId, IncarnationId);
    DEFINE_BYVAL_RW_PROPERTY(EHolderState, State);
    DEFINE_BYREF_RW_PROPERTY(NProto::THolderStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, StoredChunkIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, CachedChunkIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, UnapprovedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yvector<TJobId>, JobIds);

public:
    THolder(
        THolderId id,
        const Stroka& address,
        const TIncarnationId& incarnationId,
        EHolderState state,
        const NProto::THolderStatistics& statistics);

    THolder(THolderId id);

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input, const NCellMaster::TLoadContext& context);

    void AddChunk(const TChunkId& chunkId, bool cached);
    void RemoveChunk(const TChunkId& chunkId, bool cached);
    bool HasChunk(const TChunkId& chunkId, bool cached) const;

    void MarkChunkUnapproved(const TChunkId& chunkId);
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
