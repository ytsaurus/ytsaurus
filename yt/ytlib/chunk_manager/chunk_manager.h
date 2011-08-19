#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager.pb.h"
#include "holder.h"
#include "chunk.h"

#include "../master/master_state_manager.h"
#include "../master/composite_meta_state.h"
#include "../master/meta_state_service.h"

#include "../transaction/transaction_manager.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

class TChunkRefresh;

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
    yvector<TChunk::TPtr> GetChunkGroup(TChunkGroupId id);

    TChunk::TPtr FindChunk(const TChunkId& id, bool forUpdate = false);
    TChunk::TPtr GetChunk(
        const TChunkId& id,
        TTransaction::TPtr transaction,
        bool forUpdate = false);

    THolder::TPtr FindHolder(int id);

private:
    typedef TChunkManager TThis;
    typedef TChunkManagerProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;
    typedef yvector<THolder::TPtr> THolders;

    class TState;
    
    //! Configuration.
    TConfig Config;

    //! Manages transactions.
    TTransactionManager::TPtr TransactionManager;

    //! Manages refresh of chunk data.
    TIntrusivePtr<TChunkRefresh> ChunkRefresh;

    //! Meta-state.
    TIntrusivePtr<TState> State;

    //! Registers RPC methods.
    void RegisterMethods();

    TTransaction::TPtr GetTransaction(const TTransactionId& id, bool forUpdate = false);

    RPC_SERVICE_METHOD_DECL(NProto, RegisterHolder);
    void OnHolderRegistered(
        THolder::TPtr holder,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, HolderHeartbeat);
    void OnHolderUpdated(
        THolder::TPtr holder,
        TCtxRegisterHolder::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, AddChunk);
    void OnChunkAdded(
        TChunk::TPtr chunk,
        TCtxAddChunk::TPtr context);
    
    RPC_SERVICE_METHOD_DECL(NProto, FindChunk);

};

// TODO: move!
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

    void RefreshChunk(TChunk::TPtr chunk)
    {
        RefreshChunkLocations(chunk);
        //UpdateChunkReplicaStatus(chunk);p
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
        yvector<TChunk::TPtr> chunks = ChunkManager->GetChunkGroup(groupId);
        for (yvector<TChunk::TPtr>::iterator it = chunks.begin();
             it != chunks.end();
             ++it)
        {
            RefreshChunk(*it);
        }
    }

    void RefreshChunkLocations(TChunk::TPtr chunk)
    {
        TChunk::TLocations& locations = chunk->Locations();
        TChunk::TLocations::iterator reader = locations.begin();
        TChunk::TLocations::iterator writer = locations.begin();
        while (reader != locations.end()) {
            int holderId = *reader;
            if (~ChunkManager->FindHolder(holderId) != NULL) {
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
