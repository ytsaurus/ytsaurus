#pragma once

#include "common.h"

#include "../misc/property.h"
#include "../misc/serialize.h"

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EHolderState,
    // The holder had just registered but have not reported any heartbeats yet.
    (Registered)
    // The holder is reporting heartbeats.
    // We have a proper knowledge of its chunk set.
    (Active)
);

class THolder
{
    DEFINE_BYVAL_RO_PROPERTY(THolderId, Id);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYVAL_RW_PROPERTY(EHolderState, State);
    DEFINE_BYREF_RW_PROPERTY(NChunkHolder::THolderStatistics, Statistics);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NChunkClient::TChunkId>, ChunkIds);
    DEFINE_BYREF_RO_PROPERTY(yvector<NChunkHolder::TJobId>, JobIds);


public:
    THolder(
        THolderId id,
        const Stroka& address,
        EHolderState state,
        const NChunkHolder::THolderStatistics& statistics);

    THolder(const THolder& other);

    TAutoPtr<THolder> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<THolder> Load(THolderId id, TInputStream* input);

    void AddJob(const NChunkHolder::TJobId& id);
    void RemoveJob(const NChunkHolder::TJobId& id);
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
