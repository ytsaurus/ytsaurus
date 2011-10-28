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
    DECLARE_BYREF_RW_PROPERTY(Chunks, yhash_set<TChunkId>);
    DECLARE_BYREF_RO_PROPERTY(Jobs, yvector<TJobId>);

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
        , Chunks_(other.Chunks_)
        , Jobs_(other.Jobs_)
    { }

    TAutoPtr<THolder> Clone() const
    {
        return new THolder(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id_);
        ::Save(output, Address_);
        ::Save(output, (i32) State_); // TODO: For some reason could not DECLARE_PODTYPE(EHolderState)
        ::Save(output, Statistics_);
        SaveSet(output, Chunks_);
        ::Save(output, Jobs_);
    }

    static TAutoPtr<THolder> Load(TInputStream* input)
    {
        THolderId id;
        Stroka address;
        i32 state; // TODO: For some reason could not DECLARE_PODTYPE(EHolderState)
        THolderStatistics statistics;
        ::Load(input, id);
        ::Load(input, address);
        ::Load(input, state);
        ::Load(input, statistics);
        auto* holder = new THolder(id, address, EHolderState(state), statistics);
        ::Load(input, holder->Chunks_);
        ::Load(input, holder->Jobs_);
        return holder;
    }

    void AddJob(const TJobId& id)
    {
        Jobs_.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        auto it = Find(Jobs_.begin(), Jobs_.end(), id);
        if (it != Jobs_.end()) {
            Jobs_.erase(it);
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
