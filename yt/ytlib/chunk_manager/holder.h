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

private:
    int Id;
    Stroka Address;
    TLeaseManager::TLease Lease_;
    THolderStatistics Statistics_;
    TPreferenceMap::iterator PreferenceIterator_;
    TChunkIds UnderreplicatedChunks;
    TChunkIds OverreplicatedChunks;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
