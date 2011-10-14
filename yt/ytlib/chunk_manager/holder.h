#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

#include "../misc/serialize.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

typedef int THolderId;
const int InvalidHolderId = -1;

DECLARE_ENUM(EHolderState,
    // The holder had just registered but have not reported any heartbeats yet.
    (Registered)
    // The holder is reporting heartbeats.
    // We have a proper knowledge of its chunk set.
    (Active)
);

struct THolder
{
    THolder(
        THolderId id,
        Stroka address,
        EHolderState state,
        const THolderStatistics& statistics)
        : Id(id)
        , Address(address)
        , State(state)
        , Statistics(statistics)
    { }

    THolder(const THolder& other)
        : Id(other.Id)
        , Address(other.Address)
        , State(other.State)
        , Statistics(other.Statistics)
        , Chunks(other.Chunks)
        , Jobs(other.Jobs)
    { }

    TAutoPtr<THolder> Clone() const
    {
        return new THolder(*this);
    }

    void Save(TOutputStream* output) const
    {
        ::Save(output, Id);
        ::Save(output, Address);
        ::Save(output, (i32) State); // temp. For some reason could not DECLARE_PODTYPE(EHolderState)
        ::Save(output, Statistics);
        SaveSorted(output, Chunks);
        ::Save(output, Jobs);
    }

    static TAutoPtr<THolder> Load(TInputStream* input)
    {
        THolderId id;
        Stroka address;
        i32 state; // temp. For some reason could not DECLARE_PODTYPE(EHolderState)
        THolderStatistics statistics;
        ::Load(input, id);
        ::Load(input, address);
        ::Load(input, state);
        ::Load(input, statistics);
        auto* holder = new THolder(id, address, EHolderState(state), statistics);
        ::Load(input, holder->Chunks);
        ::Load(input, holder->Jobs);
        return holder;
    }

    void AddJob(const TJobId& id)
    {
        Jobs.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        auto it = Find(Jobs.begin(), Jobs.end(), id);
        if (it != Jobs.end()) {
            Jobs.erase(it);
        }
    }


    THolderId Id;
    Stroka Address;
    EHolderState State;
    THolderStatistics Statistics;
    yhash_set<TChunkId> Chunks;
    yvector<TJobId> Jobs;

};

////////////////////////////////////////////////////////////////////////////////

struct TReplicationSink
{
    explicit TReplicationSink(const Stroka &address)
        : Address(address)
    { }

    Stroka Address;
    yhash_set<TJobId> JobIds;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
