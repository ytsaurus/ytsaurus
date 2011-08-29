#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

#include "../master/meta_state_manager.h"
#include "../master/composite_meta_state.h"

#include "../chunk_holder/common.h"
#include "../misc/lease_manager.h"

namespace NYT {
namespace NChunkManager {

using NChunkHolder::THolderStatistics;

////////////////////////////////////////////////////////////////////////////////

struct THolder
{
    typedef yhash_set<TChunkId, TChunkIdHash> TChunkIds;

    THolder()
    { }

    THolder(
        int id,
        Stroka address,
        const THolderStatistics& statistics)
        : Id(id)
        , Address(address)
        , Statistics(statistics)
    { }

    THolder(const THolder& other)
        : Id(other.Id)
        , Address(other.Address)
        , Lease(other.Lease)
        , Statistics(other.Statistics)
        , RegularChunks(other.RegularChunks)
        , UnderreplicatedChunks(other.UnderreplicatedChunks)
        , OverreplicatedChunks(other.OverreplicatedChunks)
    { }

    THolder& operator = (const THolder& other)
    {
        // TODO: implement
        YASSERT(false);
        return *this;
    }

    int GetTotalChunkCount() const
    {
        return static_cast<int>(
            RegularChunks.size() +
            UnderreplicatedChunks.size() +
            OverreplicatedChunks.size());
    }


    int Id;
    Stroka Address;
    TLeaseManager::TLease Lease;
    THolderStatistics Statistics;
    TChunkIds RegularChunks;
    TChunkIds UnderreplicatedChunks;
    TChunkIds OverreplicatedChunks;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
