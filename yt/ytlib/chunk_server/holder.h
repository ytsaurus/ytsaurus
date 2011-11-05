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

// TODO: move impl to cpp

class THolder
{
    DECLARE_BYVAL_RO_PROPERTY(Id, THolderId);
    DECLARE_BYVAL_RO_PROPERTY(Address, Stroka);
    DECLARE_BYVAL_RW_PROPERTY(State, EHolderState);
    DECLARE_BYREF_RW_PROPERTY(Statistics, THolderStatistics);
    DECLARE_BYREF_RW_PROPERTY(ChunkIds, yhash_set<TChunkId>);
    DECLARE_BYREF_RO_PROPERTY(JobIds, yvector<TJobId>);

public:
    THolder(
        THolderId id,
        const Stroka& address,
        EHolderState state,
        const THolderStatistics& statistics)
        : Id_(id)
        , Address_(address)
        , State_(state)
        , Statistics_(statistics)
    { }

    THolder(const THolder& other)
        : Id_(other.Id_)
        , Address_(other.Address_)
        , State_(other.State_)
        , Statistics_(other.Statistics_)
        , ChunkIds_(other.ChunkIds_)
        , JobIds_(other.JobIds_)
    { }

    TAutoPtr<THolder> Clone() const
    {
        return new THolder(*this);
    }

    void Save(TOutputStream* output) const
    {
        //::Save(output, Id_);
        ::Save(output, Address_);
        ::Save(output, State_);
        ::Save(output, Statistics_);
        SaveSet(output, ChunkIds_);
        ::Save(output, JobIds_);
    }

    static TAutoPtr<THolder> Load(THolderId id, TInputStream* input)
    {
        Stroka address;
        EHolderState state;
        THolderStatistics statistics;
        ::Load(input, id);
        ::Load(input, address);
        ::Load(input, state);
        ::Load(input, statistics);
        auto* holder = new THolder(id, address, state, statistics);
        ::Load(input, holder->ChunkIds_);
        ::Load(input, holder->JobIds_);
        return holder;
    }

    void AddJob(const TJobId& id)
    {
        JobIds_.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        auto it = Find(JobIds_.begin(), JobIds_.end(), id);
        if (it != JobIds_.end()) {
            JobIds_.erase(it);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO: refactor & cleanup
struct TReplicationSink
{
    explicit TReplicationSink(const Stroka &address)
        : Address(address)
    { }

    Stroka Address;
    yhash_set<TJobId> JobIds;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
