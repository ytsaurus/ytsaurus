#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

typedef int THolderId;

struct THolder
{
    typedef yhash_set<TChunkId> TChunkIds;
    typedef yvector<TJobId> TJobs;

    THolder()
    { }

    THolder(
        THolderId id,
        Stroka address,
        const THolderStatistics& statistics)
        : Id(id)
        , Address(address)
        , Statistics(statistics)
    { }

    THolder(const THolder& other)
        : Id(other.Id)
        , Address(other.Address)
        , Statistics(other.Statistics)
        , Chunks(other.Chunks)
        , Jobs(other.Jobs)
    { }

    THolder& operator = (const THolder& other)
    {
        // TODO: implement
        UNUSED(other);
        YASSERT(false);
        return *this;
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
    THolderStatistics Statistics;
    TChunkIds Chunks;
    TJobs Jobs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
