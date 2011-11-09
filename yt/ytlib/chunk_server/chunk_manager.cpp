#include "stdafx.h"
#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"
#include "holder_expiration.h"

#include "../transaction_manager/transaction_manager.h"
#include "../meta_state/meta_state_manager.h"
#include "../meta_state/composite_meta_state.h"
#include "../meta_state/map.h"
#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../misc/assert.h"
#include "../misc/id_generator.h"

namespace NYT {
namespace NChunkServer {

using namespace NProto;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        const TConfig& config,
        TChunkManager::TPtr chunkManager,
        NMetaState::TMetaStateManager::TPtr metaStateManager,
        NMetaState::TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        // TODO: this makes a cyclic reference, don't forget to drop it in Stop
        , ChunkManager(chunkManager)
        , TransactionManager(transactionManager)
        // Some random number.
        , ChunkIdGenerator(0x7390bac62f716a19)
        // Some random number.
        , ChunkListIdGenerator(0x761ba739c541fcd0)
    {
        YASSERT(~chunkManager != NULL);
        YASSERT(~transactionManager != NULL);

        RegisterMethod(this, &TImpl::CreateChunk);
        RegisterMethod(this, &TImpl::RegisterHolder);
        RegisterMethod(this, &TImpl::UnregisterHolder);
        RegisterMethod(this, &TImpl::HeartbeatRequest);
        RegisterMethod(this, &TImpl::HeartbeatResponse);

        metaState->RegisterLoader(
            "ChunkManager.1",
            FromMethod(&TChunkManager::TImpl::Load, TPtr(this)));
        metaState->RegisterSaver(
            "ChunkManager.1",
            FromMethod(&TChunkManager::TImpl::Save, TPtr(this)));

        transactionManager->OnTransactionCommitted().Subscribe(FromMethod(
            &TThis::OnTransactionCommitted,
            TPtr(this)));
        transactionManager->OnTransactionAborted().Subscribe(FromMethod(
            &TThis::OnTransactionAborted,
            TPtr(this)));
    }


    const THolder* FindHolder(const Stroka& address)
    {
        auto it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolder(it->Second());
    }

    const TReplicationSink* FindReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        return it == ReplicationSinkMap.end() ? NULL : &it->Second();
    }

    yvector<THolderId> AllocateUploadTargets(int replicaCount)
    {
        auto holderIds = ChunkPlacement->GetUploadTargets(replicaCount);
        FOREACH(auto holderId, holderIds) {
            const auto& holder = GetHolder(holderId);
            ChunkPlacement->AddHolderSessionHint(holder);
        }
        return holderIds;
    }

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(ChunkList, TChunkList, TChunkListId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, THolderId);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

    TMetaChange<TChunkId>::TPtr InitiateCreateChunk(const TTransactionId& transactionId)
    {
        YASSERT(transactionId != NullTransactionId);

        TMsgCreateChunk message;
        message.SetTransactionId(transactionId.ToProto());

        return CreateMetaChange(
            MetaStateManager,
            message,
            &TThis::CreateChunk,
            TPtr(this));
    }

    TChunkList& CreateChunkList()
    {
        auto chunkListId = ChunkListIdGenerator.Next();

        auto* chunkList = new TChunkList(chunkListId);
        ChunkListMap.Insert(chunkListId, chunkList);
        LOG_INFO_IF(!IsRecovery(), "Chunk list created (ChunkListId: %s)",
            ~chunkListId.ToString());

        return *chunkList;
    }

    void AddChunkToChunkList(TChunk& chunk, TChunkList& chunkList) 
    {
        chunkList.ChunkIds().push_back(chunk.GetId());
        chunk.SetChunkListId(chunkList.GetId());
        RefChunk(chunk);
    }


    void RefChunk(const TChunkId& chunkId)
    {
        RefChunk(GetChunkForUpdate(chunkId));
    }

    void RefChunk(TChunk& chunk)
    {
        int refCounter = chunk.Ref();
        LOG_DEBUG_IF(!IsRecovery(), "Chunk referenced (ChunkId: %s, RefCounter: %d)",
            ~chunk.GetId().ToString(),
            refCounter);
    }

    void UnrefChunk(const TChunkId& chunkId)
    {
        UnrefChunk(GetChunkForUpdate(chunkId));
    }

    void UnrefChunk(TChunk& chunk)
    {
        int refCounter = chunk.Unref();
        LOG_DEBUG_IF(!IsRecovery(), "Chunk unreferenced (ChunkId: %s, RefCounter: %d)",
            ~chunk.GetId().ToString(),
            refCounter);

        if (refCounter == 0) {
            RemoveChunk(chunk);
        }
    }


    void RefChunkList(const TChunkListId& chunkListId)
    {
        RefChunkList(GetChunkListForUpdate(chunkListId));
    }

    void RefChunkList(TChunkList& chunkList)
    {
        int refCounter = chunkList.Ref();
        LOG_DEBUG_IF(!IsRecovery(), "Chunk list referenced (ChunkListId: %s, RefCounter: %d)",
            ~chunkList.GetId().ToString(),
            refCounter);
        
    }

    void UnrefChunkList(const TChunkListId& chunkListId)
    {
        UnrefChunkList(GetChunkListForUpdate(chunkListId));
    }

    void UnrefChunkList(TChunkList& chunkList)
    {
        int refCounter = chunkList.Unref();
        LOG_DEBUG_IF(!IsRecovery(), "Chunk list unreferenced (ChunkListId: %s, RefCounter: %d)",
            ~chunkList.GetId().ToString(),
            refCounter);

        if (refCounter == 0) {
            RemoveChunkList(chunkList);
        }
    }


    TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        Stroka address,
        const NChunkHolder::THolderStatistics& statistics)
    {
        TMsgRegisterHolder message;
        message.SetAddress(address);
        *message.MutableStatistics() = statistics.ToProto();

        return CreateMetaChange(
            MetaStateManager,
            message,
            &TThis::RegisterHolder,
            TPtr(this));
    }

    TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(THolderId holderId)
    {
        TMsgUnregisterHolder message;
        message.SetHolderId(holderId);

        return CreateMetaChange(
            MetaStateManager,
            message,
            &TThis::UnregisterHolder,
            TPtr(this));
    }


    TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(const TMsgHeartbeatRequest& message)
    {
        return CreateMetaChange(
            MetaStateManager,
            message,
            &TThis::HeartbeatRequest,
            TPtr(this));
    }

    TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(const TMsgHeartbeatResponse& message)
    {
        return CreateMetaChange(
            MetaStateManager,
            message,
            &TThis::HeartbeatResponse,
            TPtr(this));
    }


    void RunJobControl(
        const THolder& holder,
        const yvector<TJobInfo>& runningJobs,
        yvector<TJobStartInfo>* jobsToStart,
        yvector<TJobId>* jobsToStop)
    {
        ChunkReplication->RunJobControl(
            holder,
            runningJobs,
            jobsToStart,
            jobsToStop);
    }

private:
    typedef TImpl TThis;

    TConfig Config;
    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;
    
    TChunkPlacement::TPtr ChunkPlacement;
    TChunkReplication::TPtr ChunkReplication;
    THolderExpiration::TPtr HolderExpiration;
    
    TIdGenerator<TChunkId> ChunkIdGenerator;
    TIdGenerator<TChunkId> ChunkListIdGenerator;
    TIdGenerator<THolderId> HolderIdGenerator;

    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<TChunkListId, TChunkList> ChunkListMap;
    TMetaStateMap<THolderId, THolder> HolderMap;
    yhash_map<Stroka, THolderId> HolderAddressMap;
    TMetaStateMap<TChunkId, TJobList> JobListMap;
    TMetaStateMap<TJobId, TJob> JobMap;
    yhash_map<Stroka, TReplicationSink> ReplicationSinkMap;


    TChunkId CreateChunk(const TMsgCreateChunk& message)
    {
        auto chunkId = ChunkIdGenerator.Next();

        auto* chunk = new TChunk(chunkId);
        ChunkMap.Insert(chunkId, chunk);
        LOG_INFO_IF(!IsRecovery(), "Chunk created (ChunkId: %s)",
            ~chunkId.ToString());

        auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.RegisteredChunks().push_back(chunkId);

        // Every transaction keeps a reference to every its created chunk.
        RefChunk(*chunk);

        return chunkId;
    }

    void RemoveChunk(TChunk& chunk)
    {
        auto chunkId = chunk.GetId();

        // Unregister chunk replicas from all known locations.
        FOREACH (auto holderId, chunk.Locations()) {
            auto& holder = GetHolderForUpdate(holderId);
            YVERIFY(holder.ChunkIds().erase(chunkId) == 1);

            if (IsLeader()) {
                ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
            }
        }

        ChunkMap.Remove(chunkId);

        LOG_INFO_IF(!IsRecovery(), "Chunk removed (ChunkId: %s)",
            ~chunkId.ToString());
    }

    void RemoveChunkList(TChunkList& chunkList)
    {
        auto chunkListId = chunkList.GetId();

        // Drop references to chunks.
        FOREACH (const auto& chunkId, chunkList.ChunkIds()) {
            UnrefChunk(chunkId);
        }

        ChunkListMap.Remove(chunkListId);

        LOG_INFO_IF(!IsRecovery(), "Chunk list removed (ChunkListId: %s)",
            ~chunkListId.ToString());
    }


    THolderId RegisterHolder(const TMsgRegisterHolder& message)
    {
        Stroka address = message.GetAddress();
        auto statistics = NChunkHolder::THolderStatistics::FromProto(message.GetStatistics());
    
        THolderId holderId = HolderIdGenerator.Next();
    
        const auto* existingHolder = FindHolder(address);
        if (existingHolder != NULL) {
            LOG_INFO_IF(!IsRecovery(), "Holder kicked out due to address conflict (Address: %s, HolderId: %d)",
                ~address,
                existingHolder->GetId());
            DoUnregisterHolder(*existingHolder);
        }

        auto* newHolder = new THolder(
            holderId,
            address,
            EHolderState::Registered,
            statistics);

        HolderMap.Insert(holderId, newHolder);
        HolderAddressMap.insert(MakePair(address, holderId)).Second();

        if (IsLeader()) {
            StartHolderTracking(*newHolder);
        }

        LOG_INFO_IF(!IsRecovery(), "Holder registered (Address: %s, HolderId: %d, %s)",
            ~address,
            holderId,
            ~statistics.ToString());

        return holderId;
    }

    TVoid UnregisterHolder(const TMsgUnregisterHolder& message)
    { 
        auto holderId = message.GetHolderId();

        const auto& holder = GetHolder(holderId);

        DoUnregisterHolder(holder);

        return TVoid();
    }


    TVoid HeartbeatRequest(const TMsgHeartbeatRequest& message)
    {
        auto holderId = message.GetHolderId();
        auto statistics = NChunkHolder::THolderStatistics::FromProto(message.GetStatistics());

        auto& holder = GetHolderForUpdate(holderId);
        holder.Statistics() = statistics;

        if (IsLeader()) {
            HolderExpiration->RenewHolder(holder);
            ChunkPlacement->UpdateHolder(holder);
        }

        FOREACH(const auto& chunkInfo, message.GetAddedChunks()) {
            ProcessAddedChunk(holder, chunkInfo);
        }

        FOREACH(auto protoChunkId, message.GetRemovedChunks()) {
            ProcessRemovedChunk(holder, TChunkId::FromProto(protoChunkId));
        }

        bool isFirstHeartbeat = holder.GetState() == EHolderState::Registered;
        if (isFirstHeartbeat) {
            holder.SetState(EHolderState::Active);
        }
        
        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat request (Address: %s, HolderId: %d, IsFirst: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
            ~holder.GetAddress(),
            holderId,
            ~ToString(isFirstHeartbeat),
            ~statistics.ToString(),
            static_cast<int>(message.AddedChunksSize()),
            static_cast<int>(message.RemovedChunksSize()));

        return TVoid();
    }

    TVoid HeartbeatResponse(const TMsgHeartbeatResponse& message)
    {
        auto holderId = message.GetHolderId();
        auto& holder = GetHolderForUpdate(holderId);

        FOREACH(const auto& startInfo, message.GetStartedJobs()) {
            DoAddJob(holder, startInfo);
        }

        FOREACH(auto protoJobId, message.GetStoppedJobs()) {
            const auto& job = GetJob(TJobId::FromProto(protoJobId));
            DoRemoveJob(holder, job);
        }

        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat response (Address: %s, HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
            ~holder.GetAddress(),
            holderId,
            static_cast<int>(message.StartedJobsSize()),
            static_cast<int>(message.StoppedJobsSize()));

        return TVoid();
    }

    TFuture<TVoid>::TPtr Save(const TCompositeMetaState::TSaveContext& context)
    {
        auto* output = context.Output;
        auto invoker = context.Invoker;

        auto chunkIdGenerator = ChunkIdGenerator;
        auto chunkListIdGenerator = ChunkListIdGenerator;
        auto holderIdGenerator = HolderIdGenerator;
        invoker->Invoke(FromFunctor([=] ()
            {
                ::Save(output, chunkIdGenerator);
                ::Save(output, chunkListIdGenerator);
                ::Save(output, holderIdGenerator);
            }));
        
        ChunkMap.Save(invoker, output);
        ChunkListMap.Save(invoker, output);
        HolderMap.Save(invoker, output);
        JobMap.Save(invoker, output);
        return JobListMap.Save(invoker, output);
    }

    void Load(TInputStream* input)
    {
        ::Load(input, ChunkIdGenerator);
        ::Load(input, ChunkListIdGenerator);
        ::Load(input, HolderIdGenerator);
        
        ChunkMap.Load(input);
        ChunkListMap.Load(input);
        HolderMap.Load(input);
        JobMap.Load(input);
        JobListMap.Load(input);

        // Reconstruct HolderAddressMap.
        HolderAddressMap.clear();
        FOREACH(const auto& pair, HolderMap) {
            const auto* holder = pair.Second();
            YVERIFY(HolderAddressMap.insert(MakePair(holder->GetAddress(), holder->GetId())).Second());
        }

        // Reconstruct ReplicationSinkMap.
        ReplicationSinkMap.clear();
        FOREACH (const auto& pair, JobMap) {
            RegisterReplicationSinks(*pair.Second());
        }
    }

    virtual void Clear()
    {
        ChunkIdGenerator.Reset();
        ChunkListIdGenerator.Reset();
        HolderIdGenerator.Reset();
        ChunkMap.Clear();
        ChunkListMap.Clear();
        HolderMap.Clear();
        JobMap.Clear();
        JobListMap.Clear();

        HolderAddressMap.clear();
        ReplicationSinkMap.clear();
    }


    virtual void OnLeaderRecoveryComplete()
    {
        ChunkPlacement = New<TChunkPlacement>(ChunkManager);
        ChunkReplication = New<TChunkReplication>(
            ~ChunkManager,
            ~ChunkPlacement,
            ~MetaStateManager->GetEpochStateInvoker());
        HolderExpiration = New<THolderExpiration>(
            Config,
            ~ChunkManager,
            ~MetaStateManager->GetEpochStateInvoker());

        FOREACH(const auto& pair, HolderMap) {
            StartHolderTracking(*pair.Second());
        }
    }

    virtual void OnStopLeading()
    {
        ChunkPlacement.Reset();
        ChunkReplication.Reset();
        HolderExpiration.Reset();
    }


    virtual void OnTransactionCommitted(const TTransaction& transaction)
    {
        ReleaseTransactionChunkRefs(transaction);
    }

    virtual void OnTransactionAborted(const TTransaction& transaction)
    {
        ReleaseTransactionChunkRefs(transaction);
    }

    void ReleaseTransactionChunkRefs(const TTransaction& transaction)
    {
        // Release the references to every chunk created by the transaction.
        // For those chunks created but not assigned to any Cypress node
        // this also destroys them.
        FOREACH(const auto& chunkId, transaction.RegisteredChunks()) {
            UnrefChunk(chunkId);
        }
    }


    void StartHolderTracking(const THolder& holder)
    {
        HolderExpiration->AddHolder(holder);
        ChunkPlacement->AddHolder(holder);
        ChunkReplication->AddHolder(holder);
    }

    void StopHolderTracking(const THolder& holder)
    {
        HolderExpiration->RemoveHolder(holder);
        ChunkPlacement->RemoveHolder(holder);
        ChunkReplication->RemoveHolder(holder);
    }


    void DoUnregisterHolder(const THolder& holder)
    { 
        auto holderId = holder.GetId();

        if (IsLeader()) {
            StopHolderTracking(holder);
        }

        FOREACH(const auto& chunkId, holder.ChunkIds()) {
            auto& chunk = GetChunkForUpdate(chunkId);
            DoRemovedChunkReplicaAtDeadHolder(holder, chunk);
        }

        FOREACH(const auto& jobId, holder.JobIds()) {
            const auto& job = GetJob(jobId);
            DoRemoveJobAtDeadHolder(holder, job);
        }

        LOG_INFO_IF(!IsRecovery(), "Holder unregistered (Address: %s, HolderId: %d)",
            ~holder.GetAddress(),
            holderId);

        YVERIFY(HolderAddressMap.erase(holder.GetAddress()) == 1);
        HolderMap.Remove(holderId);
    }


    void DoAddChunkReplica(THolder& holder, TChunk& chunk)
    {
        YVERIFY(holder.ChunkIds().insert(chunk.GetId()).Second());
        chunk.AddLocation(holder.GetId());

        LOG_INFO_IF(!IsRecovery(), "Chunk replica added (ChunkId: %s, Address: %s, HolderId: %d, Size: %" PRId64 ")",
            ~chunk.GetId().ToString(),
            ~holder.GetAddress(),
            holder.GetId(),
            chunk.GetSize());

        if (IsLeader()) {
            ChunkReplication->AddReplica(holder, chunk);
        }
    }

    void DoRemoveChunkReplica(THolder& holder, TChunk& chunk)
    {
        YVERIFY(holder.ChunkIds().erase(chunk.GetId()) == 1);
        chunk.RemoveLocation(holder.GetId());

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed (ChunkId: %s, Address: %s, HolderId: %d)",
             ~chunk.GetId().ToString(),
             ~holder.GetAddress(),
             holder.GetId());

        if (IsLeader()) {
            ChunkReplication->RemoveReplica(holder, chunk);
        }
    }

    void DoRemovedChunkReplicaAtDeadHolder(const THolder& holder, TChunk& chunk)
    {
        chunk.RemoveLocation(holder.GetId());

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed since holder is dead (ChunkId: %s, Address: %s, HolderId: %d)",
             ~chunk.GetId().ToString(),
             ~holder.GetAddress(),
             holder.GetId());

        if (IsLeader()) {
            ChunkReplication->RemoveReplica(holder, chunk);
        }
    }


    void DoAddJob(THolder& holder, const TJobStartInfo& jobInfo)
    {
        auto chunkId = TChunkId::FromProto(jobInfo.GetChunkId());
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        auto targetAddresses = FromProto<Stroka>(jobInfo.GetTargetAddresses());
        auto jobType = EJobType(jobInfo.GetType());

        auto* job = new TJob(
            jobType,
            jobId,
            chunkId,
            holder.GetAddress(),
            targetAddresses);
        JobMap.Insert(jobId, job);

        auto& list = GetOrCreateJobListForUpdate(chunkId);
        list.AddJob(jobId);

        holder.AddJob(jobId);

        RegisterReplicationSinks(*job);

        LOG_INFO_IF(!IsRecovery(), "Job added (JobId: %s, Address: %s, HolderId: %d, JobType: %s, ChunkId: %s)",
            ~jobId.ToString(),
            ~holder.GetAddress(),
            holder.GetId(),
            ~jobType.ToString(),
            ~chunkId.ToString());
    }

    void DoRemoveJob(THolder& holder, const TJob& job)
    {
        auto jobId = job.GetJobId();

        auto& list = GetJobListForUpdate(job.GetChunkId());
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        holder.RemoveJob(jobId);

        UnregisterReplicationSinks(job);

        JobMap.Remove(job.GetJobId());

        LOG_INFO_IF(!IsRecovery(), "Job removed (JobId: %s, Address: %s, HolderId: %d)",
            ~jobId.ToString(),
            ~holder.GetAddress(),
            holder.GetId());
    }

    void DoRemoveJobAtDeadHolder(const THolder& holder, const TJob& job)
    {
        auto jobId = job.GetJobId();

        auto& list = GetJobListForUpdate(job.GetChunkId());
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        UnregisterReplicationSinks(job);

        JobMap.Remove(job.GetJobId());

        LOG_INFO_IF(!IsRecovery(), "Job removed since holder is dead (JobId: %s, Address: %s, HolderId: %d)",
            ~jobId.ToString(),
            ~holder.GetAddress(),
            holder.GetId());
    }


    void ProcessAddedChunk(
        THolder& holder,
        const TChunkInfo& chunkInfo)
    {
        auto holderId = holder.GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.GetId());
        i64 size = chunkInfo.GetSize();

        auto* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_INFO_IF(!IsRecovery(), "Unknown chunk added at holder, removal scheduled (Address: %s, HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
                ~holder.GetAddress(),
                holderId,
                ~chunkId.ToString(),
                size);
            if (IsLeader()) {
                ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
            }
            return;
        }

        //if (chunk->Size != size && chunk->Size != TChunk::UnknownSize) {
        //    LOG_ERROR("Chunk size mismatch (ChunkId: %s, OldSize: %" PRId64 ", NewSize: %" PRId64 ")",
        //        ~chunkId.ToString(),
        //        chunk->Size,
        //        size);
        //    return;
        //}

        //if (chunk->Size == TChunk::UnknownSize) {
        //    chunk->Size = size;
        //}

        DoAddChunkReplica(holder, *chunk);
    }

    void ProcessRemovedChunk(
        THolder& holder,
        const TChunkId& chunkId)
    {
        auto holderId = holder.GetId();

        auto* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_INFO_IF(!IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Address: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 ~holder.GetAddress(),
                 holderId);
            return;
        }

        DoRemoveChunkReplica(holder, *chunk);
    }


    TJobList& GetOrCreateJobListForUpdate(const TChunkId& id)
    {
        auto* list = FindJobListForUpdate(id);
        if (list != NULL)
            return *list;

        JobListMap.Insert(id, new TJobList(id));
        return GetJobListForUpdate(id);
    }

    void MaybeDropJobList(const TJobList& list)
    {
        if (list.JobIds().empty()) {
            JobListMap.Remove(list.GetChunkId());
        }
    }


    void RegisterReplicationSinks(const TJob& job)
    {
        switch (job.GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds.insert(job.GetJobId()).Second());
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YUNREACHABLE();
        }
    }

    void UnregisterReplicationSinks(const TJob& job)
    {
        switch (job.GetType()) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses()) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds.erase(job.GetJobId()) == 1);
                    MaybeDropReplicationSink(sink);
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YUNREACHABLE();
        }
    }

    TReplicationSink& GetOrCreateReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        if (it != ReplicationSinkMap.end())
            return it->Second();

        auto pair = ReplicationSinkMap.insert(MakePair(address, TReplicationSink(address)));
        YASSERT(pair.second);
        return pair.first->Second();
    }

    void MaybeDropReplicationSink(const TReplicationSink& sink)
    {
        if (sink.JobIds.empty()) {
            // NB: do not try to inline this variable! erase() will destroy the object
            // and will access the key afterwards.
            Stroka address = sink.Address;
            YVERIFY(ReplicationSinkMap.erase(address) == 1);
        }
    }

};

METAMAP_ACCESSORS_IMPL(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TImpl, Holder, THolder, THolderId, HolderMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TImpl, JobList, TJobList, TChunkId, JobListMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TImpl, Job, TJob, TJobId, JobMap)

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    NMetaState::TMetaStateManager* metaStateManager,
    NMetaState::TCompositeMetaState* metaState,
    TTransactionManager* transactionManager)
    : Config(config)
    , Impl(New<TImpl>(
        config,
        this,
        metaStateManager,
        metaState,
        transactionManager))
{
    metaState->RegisterPart(Impl);
}

const THolder* TChunkManager::FindHolder(const Stroka& address)
{
    return Impl->FindHolder(address);
}

const TReplicationSink* TChunkManager::FindReplicationSink(const Stroka& address)
{
    return Impl->FindReplicationSink(address);
}

yvector<THolderId> TChunkManager::AllocateUploadTargets(int replicaCount)
{
    return Impl->AllocateUploadTargets(replicaCount);
}

TMetaChange<TChunkId>::TPtr TChunkManager::InitiateCreateChunk(const TTransactionId& transactionId)
{
    return Impl->InitiateCreateChunk(transactionId);
}

TChunkList& TChunkManager::CreateChunkList()
{
    return Impl->CreateChunkList();
}

void TChunkManager::AddChunkToChunkList(TChunk& chunk, TChunkList& chunkList)
{
    return Impl->AddChunkToChunkList(chunk, chunkList);
}

void TChunkManager::RefChunk(const TChunkId& chunkId)
{
    Impl->RefChunk(chunkId);
}

void TChunkManager::RefChunk(TChunk& chunk)
{
    Impl->RefChunk(chunk);
}

void TChunkManager::UnrefChunk(const TChunkId& chunkId)
{
    Impl->UnrefChunk(chunkId);
}

void TChunkManager::UnrefChunk(TChunk& chunk)
{
    Impl->UnrefChunk(chunk);
}

void TChunkManager::RefChunkList(const TChunkListId& chunkListId)
{
    Impl->RefChunkList(chunkListId);
}

void TChunkManager::RefChunkList(TChunkList& chunkList)
{
    Impl->RefChunkList(chunkList);
}

void TChunkManager::UnrefChunkList(const TChunkListId& chunkListId)
{
    Impl->UnrefChunkList(chunkListId);
}

void TChunkManager::UnrefChunkList(TChunkList& chunkList)
{
    Impl->UnrefChunkList(chunkList);
}

TMetaChange<THolderId>::TPtr TChunkManager::InitiateRegisterHolder(Stroka address, const NChunkHolder::THolderStatistics& statistics)
{
    return Impl->InitiateRegisterHolder(address, statistics);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateUnregisterHolder(THolderId holderId)
{
    return Impl->InitiateUnregisterHolder(holderId);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateHeartbeatRequest(const TMsgHeartbeatRequest& message)
{
    return Impl->InitiateHeartbeatRequest(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateHeartbeatResponse(const TMsgHeartbeatResponse& message)
{
    return Impl->InitiateHeartbeatResponse(message);
}

void TChunkManager::RunJobControl(
    const THolder& holder,
    const yvector<TJobInfo>& runningJobs,
    yvector<TJobStartInfo>* jobsToStart,
    yvector<TJobId>* jobsToStop)
{
    Impl->RunJobControl(
        holder,
        runningJobs,
        jobsToStart,
        jobsToStop);
}

METAMAP_ACCESSORS_FWD(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
METAMAP_ACCESSORS_FWD(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)
METAMAP_ACCESSORS_FWD(TChunkManager, Holder, THolder, THolderId, *Impl)
METAMAP_ACCESSORS_FWD(TChunkManager, JobList, TJobList, TChunkId, *Impl)
METAMAP_ACCESSORS_FWD(TChunkManager, Job, TJob, TJobId, *Impl)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
