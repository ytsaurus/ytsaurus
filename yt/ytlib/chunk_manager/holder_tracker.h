#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"

#include "../misc/lease_manager.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

struct THolderStatistics
{
    THolderStatistics()
        : AvailableSpace(0)
        , UsedSpace(0)
        , ChunkCount(0)
    { }

    i64 AvailableSpace;
    i64 UsedSpace;
    i32 ChunkCount;

    static THolderStatistics FromProto(const NProto::THolderStatistics& proto)
    {
        THolderStatistics result;
        result.AvailableSpace = proto.GetAvailableSpace();
        result.UsedSpace = proto.GetUsedSpace();
        result.ChunkCount = proto.GetChunkCount();
        return result;
    }

    NProto::THolderStatistics ToProto() const
    {
        NProto::THolderStatistics result;
        result.SetAvailableSpace(AvailableSpace);
        result.SetUsedSpace(UsedSpace);
        result.SetChunkCount(0);
        return result;
    }

    Stroka ToString() const
    {
        return Sprintf("AvailableSpace: %" PRId64 ", UsedSpace: %" PRId64, ", ChunkCount: %d",
            AvailableSpace,
            UsedSpace,
            ChunkCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class THolder
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolder> TPtr;

    THolder(int id)
        : Id(id)
    { }

    int GetId() const
    {
        return Id;
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

private:
    int Id;
    TLeaseManager::TLease Lease;
    THolderStatistics Statistics;

};

////////////////////////////////////////////////////////////////////////////////

class THolderTracker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<THolderTracker> TPtr;
    typedef TChunkManagerConfig TConfig;

    THolderTracker(
        const TConfig& config,
        IInvoker::TPtr serviceInvoker);

    THolder::TPtr RegisterHolder(const THolderStatistics& statistics);

    THolder::TPtr FindHolder(int id);

    THolder::TPtr GetHolder(int id);
    
    void RenewHolderLease(THolder::TPtr holder);

    bool IsHolderAlive(int id);

private:
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    typedef yhash_map<int, THolder::TPtr> THolderMap;

    TConfig Config;
    IInvoker::TPtr ServiceInvoker;
    int CurrentId;
    TLeaseManager::TPtr LeaseManager;
    THolderMap Holders;

    void OnHolderExpired(THolder::TPtr holder);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
