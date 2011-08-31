#pragma once

#include "common.h"
#include "chunk_manager_rpc.h"
#include "chunk_manager_rpc.pb.h"
#include "chunk_manager.pb.h"
#include "holder.h"
#include "chunk.h"

#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/meta_state_service.h"
#include "../meta_state/map.h"

#include "../transaction/transaction_manager.h"

#include "../rpc/server.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

//class TChunkRefresh;
class TChunkPlacement;
class TChunkReplication;

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

    //yvector<TChunkGroupId> GetChunkGroupIds();
    //yvector<TChunkId> GetChunkGroup(TChunkGroupId id);

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, int);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

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
    //TIntrusivePtr<TChunkRefresh> ChunkRefresh;

    //! Manages placement of new chunks and rearranges existing ones.
    TIntrusivePtr<TChunkPlacement> ChunkPlacement;

    //! Manages chunk replication.
    TIntrusivePtr<TChunkReplication> ChunkReplication;

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

DECLARE_ENUM(EChunkState,
    (Lost)
    (Regular)
    (Overreplicated)
    (Underreplicated)
);

// TODO: move to chunk_replication.h/cpp
class TChunkReplication
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunkReplication> TPtr;

    TChunkReplication(TChunkManager::TPtr chunkManager)
        : ChunkManager(chunkManager)
    { }

    int GetDesiredReplicaCount(const TChunk& chunk)
    {
        // TODO: make configurable
        return 3;
    }

    EChunkState GetState(const TChunk& chunk)
    {
        int actualReplicaCount = chunk.Locations.ysize();
        if (actualReplicaCount == 0) {
            return EChunkState::Lost;
        }

        int desiredReplicaCount = GetDesiredReplicaCount(chunk);
        int potentialReplicaCount = actualReplicaCount + chunk.PendingReplicaCount;
        if (potentialReplicaCount > desiredReplicaCount) {
            return EChunkState::Overreplicated;
        } else if (potentialReplicaCount < desiredReplicaCount) {
            return EChunkState::Underreplicated;
        } else {
            return EChunkState::Regular;
        }
    }

    void RegisterHolder(THolder& holder)
    {
        // Intentionally empty.
        UNUSED(holder);
    }

    void UnregisterHolder(const THolder& holder)
    {
        yvector<TChunkId> chunkIds;
        chunkIds.reserve(holder.GetTotalChunkCount());
        NStl::copy(holder.RegularChunks.begin(), holder.RegularChunks.end(), NStl::back_inserter(chunkIds));
        NStl::copy(holder.UnderreplicatedChunks.begin(), holder.UnderreplicatedChunks.end(), NStl::back_inserter(chunkIds));
        NStl::copy(holder.OverreplicatedChunks.begin(), holder.OverreplicatedChunks.end(), NStl::back_inserter(chunkIds));

        for (yvector<TChunkId>::const_iterator it = chunkIds.begin();
             it != chunkIds.end();
             ++it)
        {
            const TChunkId& chunkId = *it;
            TChunk& chunk = ChunkManager->GetChunkForUpdate(chunkId);
            UnregisterReplica(chunk, holder);
        }
    }

    void RegisterReplica(TChunk& chunk, const THolder& holder)
    {
        EChunkState oldState = GetState(chunk);
        chunk.AddLocation(holder.Id);
        EChunkState newState = GetState(chunk);
        MaybeUpdateState(chunk, oldState, newState);

        // TODO: remove 
        static NLog::TLogger& Logger = ChunkManagerLogger;

        LOG_DEBUG("Chunk replica registered (HolderId: %d, ChunkId: %s, OldState: %s, NewState: %s, ReplicaCount: %d)",
            holder.Id,
            ~chunk.Id.ToString(),
            ~oldState.ToString(),
            ~newState.ToString(),
            chunk.Locations.ysize());
    }

    void UnregisterReplica(TChunk& chunk, const THolder& holder)
    {
        EChunkState oldState = GetState(chunk);
        chunk.RemoveLocation(holder.Id);
        EChunkState newState = GetState(chunk);
        MaybeUpdateState(chunk, oldState, newState);

        // TODO: remove 
        static NLog::TLogger& Logger = ChunkManagerLogger;

        LOG_DEBUG("Chunk replica unregistered (HolderId: %d, ChunkId: %s, OldState: %s, NewState: %s, ReplicaCount: %d)",
            holder.Id,
            ~chunk.Id.ToString(),
            ~oldState.ToString(),
            ~newState.ToString(),
            chunk.Locations.ysize());
    }

    void GetJobControl(
        const THolder& holder,
        yvector<NProto::TJobStartInfo>& jobsToStart,
        yvector<TJobId>& jobsToStop)
    {

    }

private:
    TChunkManager::TPtr ChunkManager;

    void MaybeUpdateState(
        TChunk& chunk,
        EChunkState oldState,
        EChunkState newState)
    {
        if (newState == oldState)
            return;

        const TChunk::TLocations& locations = chunk.Locations;
        for (TChunk::TLocations::const_iterator it = locations.begin();
                it != locations.end();
                ++it)
        {
            int holderId = *it;
            THolder& holder = ChunkManager->GetHolderForUpdate(holderId);

            switch (oldState) {
                case EChunkState::Lost:
                    break;
                case EChunkState::Regular:
                    YVERIFY(holder.RegularChunks.erase(chunk.Id) == 1);
                    break;
                case EChunkState::Overreplicated:
                    YVERIFY(holder.OverreplicatedChunks.erase(chunk.Id) == 1);
                    break;
                case EChunkState::Underreplicated:
                    YVERIFY(holder.UnderreplicatedChunks.erase(chunk.Id) == 1);
                    break;
                default:
                    YASSERT(false);
                    break;
            }

            switch (newState) {
                case EChunkState::Lost:
                    break;
                case EChunkState::Regular:
                    YVERIFY(holder.RegularChunks.insert(chunk.Id).Second());
                    break;
                case EChunkState::Overreplicated:
                    YVERIFY(holder.OverreplicatedChunks.insert(chunk.Id).Second());
                    break;
                case EChunkState::Underreplicated:
                    YVERIFY(holder.UnderreplicatedChunks.insert(chunk.Id).Second());
                    break;
                default:
                    YASSERT(false);
                    break;
            }
        }
    }

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
