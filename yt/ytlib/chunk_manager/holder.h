#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

#include "../master/master_state_manager.h"
#include "../master/composite_meta_state.h"

#include "../chunk_holder/common.h"
#include "../misc/lease_manager.h"

namespace NYT {
namespace NChunkManager {

using NChunkHolder::THolderStatistics;

////////////////////////////////////////////////////////////////////////////////

class THolder
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolder> TPtr;
    typedef NStl::multimap< double, THolder::TPtr, TGreater<double> > TPreferenceMap;
    typedef yhash_set<TChunkId, TChunkIdHash> TChunkIds;

    THolder(int id, Stroka address)
        : Id(id)
        , Address(address)
    { }

    //! For serialization
    THolder()
    { }

    //! For putting in TMetaStateRefMap
    THolder(THolder& holder)
        : Id(holder.Id)
        , Address(holder.Address)
        , Lease_(holder.Lease_)
        , Statistics_(holder.Statistics_)
        , PreferenceIterator_(holder.PreferenceIterator_)
        , UnderreplicatedChunks_(holder.UnderreplicatedChunks_)
        , OverreplicatedChunks_(holder.OverreplicatedChunks_)
    { }

    int GetId() const
    {
        return Id;
    }

    Stroka GetAddress() const
    {
        return Address;
    }

    TLeaseManager::TLease& Lease()
    {
        return Lease_;
    }

    THolderStatistics& Statistics()
    {
        return Statistics_;
    }

    double GetPreference() const
    {
        return (1.0 + Statistics_.UsedSpace) / (1.0 + Statistics_.UsedSpace + Statistics_.AvailableSpace);
    }

    TPreferenceMap::iterator& PreferenceIterator()
    {
        return PreferenceIterator_;
    }

    TChunkIds& UnderreplicatedChunks()
    {
        return UnderreplicatedChunks_;
    }

    TChunkIds& OverreplicatedChunks()
    {
        return OverreplicatedChunks_;
    }

private:
    int Id;
    Stroka Address;
    TLeaseManager::TLease Lease_;
    THolderStatistics Statistics_;
    TPreferenceMap::iterator PreferenceIterator_;
    TChunkIds UnderreplicatedChunks_;
    TChunkIds OverreplicatedChunks_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
