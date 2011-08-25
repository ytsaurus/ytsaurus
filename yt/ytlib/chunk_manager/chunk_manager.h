#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager.pb.h"
#include "holder.h"
#include "chunk.h"

#include "../master/master_state_manager.h"
#include "../master/composite_meta_state.h"
#include "../master/meta_state_service.h"
#include "../master/map.h"

#include "../transaction/transaction_manager.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkRefresh;
class TChunkPlacement;

class TChunkManager
    : public TMetaStateServiceBase
{
public:
    typedef TIntrusivePtr<TChunkManager> TPtr;
    typedef TChunkManagerConfig TConfig;

    //! Creates an instance.
    TChunkManager(
        const TConfig& config,
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        IInvoker::TPtr serviceInvoker,
        NRpc::TServer::TPtr server,
        TTransactionManager::TPtr transactionManager);

    yvector<TChunkGroupId> GetChunkGroupIds();
    yvector<TChunkId> GetChunkGroup(TChunkGroupId id);

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, int);

private:
    typedef TChunkManager TThis;
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;
    typedef yvector<int> THolders;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Manages refresh of chunk data.
    TIntrusivePtr<TChunkRefresh> ChunkRefresh;

    //! Manages placement of new chunks.
    TIntrusivePtr<TChunkPlacement> ChunkPlacement;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();

    void ValidateHolderId(int holderId);
    void ValidateTransactionId(const TTransactionId& transactionId);
    void ValidateChunkId(const TChunkId& chunkId, const TTransactionId& transactionId);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    void OnHolderRegistered(
        int id,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    void OnHolderUpdated(
        TVoid,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, AddChunk);
    void OnChunkAdded(
        TChunkId id,
        TCtxAddChunk::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

};

// TODO: move to chunk_placement.h/cpp
class TChunkPlacement
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkPlacement> TPtr;

    void RegisterHolder(const THolder& holder)
    {
        double preference = GetPreference(holder);
        TPreferenceMap::iterator it = PreferenceMap.insert(MakePair(preference, holder.Id));
        YVERIFY(IteratorMap.insert(MakePair(holder.Id, it)).Second());
    }

    void UnregisterHolder(const THolder& holder)
    {
        TIteratorMap::iterator iteratorIt = IteratorMap.find(holder.Id);
        YASSERT(iteratorIt != IteratorMap.end());
        TPreferenceMap::iterator preferenceIt = iteratorIt->Second();
        PreferenceMap.erase(preferenceIt);
        IteratorMap.erase(iteratorIt);
    }

    void UpdateHolder(const THolder& holder)
    {
        UnregisterHolder(holder);
        RegisterHolder(holder);
    }

    yvector<int> GetNewChunkPlacement(int replicaCount)
    {
        yvector<int> result;
        TPreferenceMap::reverse_iterator it = PreferenceMap.rbegin();
        while (it != PreferenceMap.rend() && result.ysize() < replicaCount) {
            result.push_back((*it++).second);
        }
        return result;
    }

private:
    typedef ymultimap<double, int> TPreferenceMap;
    typedef yhash_map<int, TPreferenceMap::iterator> TIteratorMap;

    TPreferenceMap PreferenceMap;
    TIteratorMap IteratorMap;

    static double GetPreference(const THolder& holder)
    {
        const THolderStatistics& statistics = holder.Statistics;
        return
            (1.0 + statistics.UsedSpace) /
            (1.0 + statistics.UsedSpace + statistics.AvailableSpace);
    }
};

// TODO: move to chunk_refresh.h/cpp
class TChunkRefresh
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkRefresh> TPtr;
    typedef TChunkManagerConfig TConfig;

    TChunkRefresh(
        const TConfig& config,
        TChunkManager::TPtr chunkManager)
        : Config(config)
        , ChunkManager(chunkManager)
        , CurrentIndex(0)
    {
    }

    void StartBackground(IInvoker::TPtr epochInvoker)
    {
        YASSERT(~EpochInvoker == NULL);
        EpochInvoker = epochInvoker;
        RestartBackgroundRefresh();
    }

    void StopBackground()
    {
        EpochInvoker.Drop();
    }

    void RefreshChunk(TChunk& chunk)
    {
        RefreshChunkLocations(chunk);
        //UpdateChunkReplicaStatus(chunk);
    }   

private:
    TConfig Config;
    TChunkManager::TPtr ChunkManager;
    int CurrentIndex;
    IInvoker::TPtr EpochInvoker;
    yvector<TChunkGroupId> GroupIds;


    void RestartBackgroundRefresh()
    {
        GroupIds = ChunkManager->GetChunkGroupIds();
        ScheduleRefresh();
    }

    void ScheduleRefresh()
    {
        TDelayedInvoker::Get()->Submit(
            FromMethod(
                &TChunkRefresh::OnRefresh,
                TPtr(this))
            ->Via(EpochInvoker),
            Config.ChunkGroupRefreshPeriod);
    }

    void OnRefresh()
    {
        if (CurrentIndex >= GroupIds.ysize()) {
            RestartBackgroundRefresh();
            return;
        }

        RefreshChunkGroup(GroupIds[CurrentIndex++]);

        ScheduleRefresh();
    }


    void RefreshChunkGroup(TChunkGroupId groupId)
    {
        yvector<TChunkId> chunkIds = ChunkManager->GetChunkGroup(groupId);
        for (yvector<TChunkId>::iterator it = chunkIds.begin();
             it != chunkIds.end();
             ++it)
        {
            TChunk& chunk = ChunkManager->GetChunkForUpdate(*it);
            RefreshChunk(chunk);
        }
    }

    void RefreshChunkLocations(TChunk& chunk)
    {
        TChunk::TLocations& locations = chunk.Locations;
        TChunk::TLocations::iterator reader = locations.begin();
        TChunk::TLocations::iterator writer = locations.begin();
        while (reader != locations.end()) {
            int holderId = *reader;
            if (ChunkManager->FindHolder(holderId) != NULL) {
                *writer++ = holderId;
            }
            ++reader;
        } 
        locations.erase(writer, locations.end());
    }

    //void UpdateChunkReplicaStatus(TChunk::TPtr chunk)
    //{
    //    TChunkId chunkId = chunk->GetId();
    //    int delta = chunk->GetReplicaDelta();

    //    const TChunk::TLocations& locations = chunk->Locations();
    //    for (TChunk::TLocations::const_iterator it = locations.begin();
    //         it != locations.end();
    //         ++it)
    //    {
    //        int holderId = *it;
    //        THolder::TPtr holder = GetHolder(holderId);
    //        if (delta < 0) {
    //            holder->OverreplicatedChunks().erase(chunkId);
    //            holder->UnderreplicatedChunks().insert(chunkId);
    //        } else if (delta > 0) {
    //            holder->OverreplicatedChunks().insert(chunkId);
    //            holder->UnderreplicatedChunks().erase(chunkId);
    //        } else {
    //            holder->OverreplicatedChunks().erase(chunkId);
    //            holder->UnderreplicatedChunks().erase(chunkId);
    //        }
    //    }
    //}

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
