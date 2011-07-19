#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

#include "../chunk_holder/common.h"

#include "../misc/lease_manager.h"

namespace NYT {
namespace NChunkManager {

using NChunkHolder::THolderStatistics;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

class THolder
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolder> TPtr;
    typedef NStl::multimap<double, THolder::TPtr> TPreferenceMap;

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

    TLeaseManager::TLease GetLease() const
    {
        return Lease;
    }

    void SetLease(TLeaseManager::TLease lease)
    {
        Lease = lease;
    }

    THolderStatistics GetStatistics() const
    {
        return Statistics;
    }

    void SetStatistics(const THolderStatistics& statistics)
    {
        Statistics = statistics;
    }

    double GetPreference() const
    {
        return -(1.0 + Statistics.UsedSpace) / (1.0 + Statistics.UsedSpace + Statistics.AvailableSpace);
    }

    TPreferenceMap::iterator GetPreferenceIterator() const
    {
        return PreferenceIterator;
    }

    void SetPreferenceIterator(TPreferenceMap::iterator it)
    {
        PreferenceIterator = it;
    }

private:
    int Id;
    Stroka Address;
    TLeaseManager::TLease Lease;
    THolderStatistics Statistics;
    TPreferenceMap::iterator PreferenceIterator;

};

////////////////////////////////////////////////////////////////////////////////

class THolderTracker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolderTracker> TPtr;
    typedef TChunkManagerConfig TConfig;
    typedef yvector<THolder::TPtr> THolders;

    THolderTracker(
        const TConfig& config,
        IInvoker::TPtr serviceInvoker);

    THolder::TPtr RegisterHolder(
        const THolderStatistics& statistics,
        Stroka address);

    THolder::TPtr FindHolder(int id);

    THolder::TPtr GetHolder(int id);
    
    void RenewHolderLease(THolder::TPtr holder);
    void UpdateHolderPreference(THolder::TPtr holder);

    bool IsHolderAlive(int id);

    // TODO: proximity
    THolders GetTargetHolders(int count);

private:
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    typedef yhash_map<int, THolder::TPtr> THolderMap;

    TConfig Config;
    IInvoker::TPtr ServiceInvoker;
    int CurrentId;
    TLeaseManager::TPtr LeaseManager;
    THolderMap Holders;
    THolder::TPreferenceMap PreferenceMap;

    void OnHolderExpired(THolder::TPtr holder);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
