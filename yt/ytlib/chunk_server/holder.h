#pragma once

#include "common.h"

#include "chunk_service.pb.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/serialize.h>

#include <ytlib/chunk_client/common.h>
#include <ytlib/chunk_holder/chunk_holder_service_proxy.h>

// TODO: remove this
#include <ytlib/chunk_holder/common.h>

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
    typedef NChunkClient::TChunkId TChunkId;
    typedef NChunkHolder::TJobId TJobId;

    DEFINE_BYVAL_RO_PROPERTY(THolderId, Id);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYVAL_RW_PROPERTY(EHolderState, State);
    DEFINE_BYREF_RW_PROPERTY(NChunkServer::NProto::THolderStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, StoredChunkIds);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TChunkId>, CachedChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yvector<TJobId>, JobIds);

public:
    THolder(
        THolderId id,
        const Stroka& address,
        EHolderState state,
        const NChunkServer::NProto::THolderStatistics& statistics);

    THolder(const THolder& other);

    TAutoPtr<THolder> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<THolder> Load(THolderId id, TInputStream* input);

    void AddChunk(const TChunkId& chunkId, bool cached);
    void RemoveChunk(const TChunkId& chunkId, bool cached);
    bool HasChunk(const TChunkId& chunkId, bool cached) const;

    void AddJob(const TJobId& id);
    void RemoveJob(const TJobId& id);
};

////////////////////////////////////////////////////////////////////////////////

// TODO: refactor & cleanup
struct TReplicationSink
{
    explicit TReplicationSink(const Stroka &address)
        : Address(address)
    { }

    Stroka Address;
    yhash_set<NChunkHolder::TJobId> JobIds;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
