#include "stdafx.h"
#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"
#include "holder_lease_tracker.h"

#include "../transaction_server/transaction_manager.h"
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
using namespace NTransactionServer;
using namespace NChunkClient;
using namespace NChunkHolder;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        TConfig* config,
        TChunkManager* chunkManager,
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        TTransactionManager* transactionManager,
        IHolderRegistry* holderRegistry)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        // TODO: this makes a cyclic reference, don't forget to drop it in Stop
        , ChunkManager(chunkManager)
        , TransactionManager(transactionManager)
        , HolderRegistry(holderRegistry)
        // Some random number.
        , ChunkIdGenerator(0x7390bac62f716a19)
        // Some random number.
        , ChunkListIdGenerator(0x761ba739c541fcd0)
    {
        YASSERT(chunkManager);
        YASSERT(transactionManager);
        YASSERT(holderRegistry);

        RegisterMethod(this, &TImpl::AllocateChunk);
        RegisterMethod(this, &TImpl::ConfirmChunks);
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
            ChunkPlacement->OnSessionHinted(holder);
        }
        return holderIds;
    }

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    DEFINE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderRegistered);
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderUnregistered);

    TMetaChange<TChunkId>::TPtr InitiateAllocateChunk(const TTransactionId& transactionId)
    {
        YASSERT(transactionId != NTransactionServer::NullTransactionId);

        TMsgAllocateChunk message;
        message.set_transactionid(transactionId.ToProto());

        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::AllocateChunk,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateConfirmChunks(const TMsgConfirmChunks& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::ConfirmChunks,
            this);
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
        message.set_address(address);
        *message.mutable_statistics() = statistics.ToProto();

        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::RegisterHolder,
            this);
    }

    TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(THolderId holderId)
    {
        TMsgUnregisterHolder message;
        message.set_holderid(holderId);

        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::UnregisterHolder,
            this);
    }


    TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(const TMsgHeartbeatRequest& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::HeartbeatRequest,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(const TMsgHeartbeatResponse& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::HeartbeatResponse,
            this);
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

    TConfig::TPtr Config;
    TChunkManager::TPtr ChunkManager;
    TTransactionManager::TPtr TransactionManager;
    IHolderRegistry::TPtr HolderRegistry;
    
    TChunkPlacement::TPtr ChunkPlacement;
    TChunkReplication::TPtr ChunkReplication;
    THolderLeaseTracker::TPtr HolderLeaseTracking;
    
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


    TChunkId AllocateChunk(const TMsgAllocateChunk& message)
    {
        auto transactionId = TTransactionId::FromProto(message.transactionid());

        auto chunkId = ChunkIdGenerator.Next();
        auto* chunk = new TChunk(chunkId);
        ChunkMap.Insert(chunkId, chunk);

        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.AllocatedChunkIds().push_back(chunkId);

        // Every transaction keeps a reference to every its allocated chunk.
        RefChunk(*chunk);

        LOG_INFO_IF(!IsRecovery(), "Chunk allocated (ChunkId: %s, TransactionId: %s)",
            ~chunkId.ToString(),
            ~transactionId.ToString());

        return chunkId;
    }

    TVoid ConfirmChunks(const TMsgConfirmChunks& message)
    {
        auto transactionId = TTransactionId::FromProto(message.transactionid());
        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);

        FOREACH (const auto& chunkInfo, message.chunks()) {
            // TODO: add hinted locations
            auto chunkId = TChunkId::FromProto(chunkInfo.chunkid());
            auto& chunk = ChunkMap.GetForUpdate(chunkId);

            // Skip chunks that are already confirmed.
            if (chunk.IsConfirmed()) {
                continue;
            }

            TBlob blob;
            if (!SerializeProtobuf(&chunkInfo.attributes(), &blob)) {
                LOG_FATAL("Error serializing chunk attributes (ChunkId: %s)", ~chunkId.ToString());
            }
            
            chunk.SetAttributes(TSharedRef(MoveRV(blob)));
            transaction.ConfirmedChunkIds().push_back(chunkId);

            LOG_INFO_IF(!IsRecovery(), "Chunk confirmed (ChunkId: %s, TransactionId: %s)",
                ~chunkId.ToString(),
                ~transactionId.ToString());
        }

        return TVoid();
    }


    void RemoveChunk(TChunk& chunk)
    {
        auto chunkId = chunk.GetId();

        // Unregister chunk replicas from all known locations.
        FOREACH (auto holderId, chunk.StoredLocations()) {
            DoRemoveChunkFromLocation(chunk, false, holderId);
        }

        if (~chunk.CachedLocations()) {
            FOREACH (auto holderId, *chunk.CachedLocations()) {
                DoRemoveChunkFromLocation(chunk, true, holderId);
            }
        }

        ChunkMap.Remove(chunkId);

        LOG_INFO_IF(!IsRecovery(), "Chunk removed (ChunkId: %s)", ~chunkId.ToString());
    }

    void DoRemoveChunkFromLocation(TChunk& chunk, bool cached, THolderId holderId)
    {
        auto chunkId = chunk.GetId();
        auto& holder = GetHolderForUpdate(holderId);
        holder.RemoveChunk(chunkId, cached);

        if (!cached && IsLeader()) {
            ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
        }
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
        Stroka address = message.address();
        auto statistics = NChunkHolder::THolderStatistics::FromProto(message.statistics());
    
        THolderId holderId = HolderIdGenerator.Next();
    
        const auto* existingHolder = FindHolder(address);
        if (existingHolder) {
            LOG_INFO_IF(!IsRecovery(), "Holder kicked out due to address conflict (Address: %s, HolderId: %d)",
                ~address,
                existingHolder->GetId());
            DoUnregisterHolder(*existingHolder);
        }

        LOG_INFO_IF(!IsRecovery(), "Holder registered (Address: %s, HolderId: %d, %s)",
            ~address,
            holderId,
            ~statistics.ToString());

        auto* newHolder = new THolder(
            holderId,
            address,
            EHolderState::Inactive,
            statistics);

        HolderMap.Insert(holderId, newHolder);
        HolderAddressMap.insert(MakePair(address, holderId)).Second();

        if (IsLeader()) {
            StartHolderTracking(*newHolder);
        }

        return holderId;
    }

    TVoid UnregisterHolder(const TMsgUnregisterHolder& message)
    { 
        auto holderId = message.holderid();

        const auto& holder = GetHolder(holderId);

        DoUnregisterHolder(holder);

        return TVoid();
    }


    TVoid HeartbeatRequest(const TMsgHeartbeatRequest& message)
    {
        auto holderId = message.holderid();
        auto statistics = NChunkHolder::THolderStatistics::FromProto(message.statistics());

        auto& holder = GetHolderForUpdate(holderId);
        holder.Statistics() = statistics;

        if (IsLeader()) {
            HolderLeaseTracking->RenewHolderLease(holder);
            ChunkPlacement->OnHolderUpdated(holder);
        }

        FOREACH(const auto& chunkInfo, message.addedchunks()) {
            ProcessAddedChunk(holder, chunkInfo);
        }

        FOREACH(const auto& chunkInfo, message.removedchunks()) {
            ProcessRemovedChunk(holder, chunkInfo);
        }

        bool isFirstHeartbeat = holder.GetState() == EHolderState::Inactive;
        if (isFirstHeartbeat) {
            holder.SetState(EHolderState::Active);
        }
        
        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat request (Address: %s, HolderId: %d, IsFirst: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
            ~holder.GetAddress(),
            holderId,
            ~ToString(isFirstHeartbeat),
            ~statistics.ToString(),
            static_cast<int>(message.addedchunks_size()),
            static_cast<int>(message.removedchunks_size()));

        return TVoid();
    }

    TVoid HeartbeatResponse(const TMsgHeartbeatResponse& message)
    {
        auto holderId = message.holderid();
        auto& holder = GetHolderForUpdate(holderId);

        FOREACH(const auto& startInfo, message.startedjobs()) {
            DoAddJob(holder, startInfo);
        }

        FOREACH(auto protoJobId, message.stoppedjobs()) {
            const auto& job = GetJob(TJobId::FromProto(protoJobId));
            DoRemoveJob(holder, job);
        }

        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat response (Address: %s, HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
            ~holder.GetAddress(),
            holderId,
            static_cast<int>(message.startedjobs_size()),
            static_cast<int>(message.stoppedjobs_size()));

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
        ChunkPlacement = New<TChunkPlacement>(~ChunkManager);

        ChunkReplication = New<TChunkReplication>(
            ~ChunkManager,
            ~ChunkPlacement,
            ~MetaStateManager->GetEpochStateInvoker());

        HolderLeaseTracking = New<THolderLeaseTracker>(
            ~Config,
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
        HolderLeaseTracking.Reset();
    }


    virtual void OnTransactionCommitted(const TTransaction& transaction)
    {
        ReleaseAllocatedChunkRefs(transaction);

        // TODO: check that all allocated chunks were confirmed
    }

    virtual void OnTransactionAborted(const TTransaction& transaction)
    {
        ReleaseAllocatedChunkRefs(transaction);
    }

    void ReleaseAllocatedChunkRefs(const TTransaction& transaction)
    {
        // Release the references to every chunk allocated by the transaction.
        // For those chunks created but not assigned to any Cypress node
        // this also destroys them.
        FOREACH(const auto& chunkId, transaction.AllocatedChunkIds()) {
            UnrefChunk(chunkId);
        }
    }


    void StartHolderTracking(const THolder& holder)
    {
        HolderLeaseTracking->OnHolderRegistered(holder);
        ChunkPlacement->OnHolderRegistered(holder);
        ChunkReplication->OnHolderRegistered(holder);

        HolderRegistered_.Fire(holder); 
    }

    void StopHolderTracking(const THolder& holder)
    {
        HolderLeaseTracking->OnHolderUnregistered(holder);
        ChunkPlacement->OnHolderUnregistered(holder);
        ChunkReplication->OnHolderUnregistered(holder);

        HolderUnregistered_.Fire(holder);
    }


    void DoUnregisterHolder(const THolder& holder)
    { 
        auto holderId = holder.GetId();

        LOG_INFO_IF(!IsRecovery(), "Holder unregistered (Address: %s, HolderId: %d)",
            ~holder.GetAddress(),
            holderId);

        if (IsLeader()) {
            StopHolderTracking(holder);
        }

        FOREACH(const auto& chunkId, holder.StoredChunkIds()) {
            auto& chunk = GetChunkForUpdate(chunkId);
            DoRemoveChunkReplicaAtDeadHolder(holder, chunk, false);
        }

        FOREACH(const auto& chunkId, holder.CachedChunkIds()) {
            auto& chunk = GetChunkForUpdate(chunkId);
            DoRemoveChunkReplicaAtDeadHolder(holder, chunk, true);
        }

        FOREACH(const auto& jobId, holder.JobIds()) {
            const auto& job = GetJob(jobId);
            DoRemoveJobAtDeadHolder(holder, job);
        }

        YVERIFY(HolderAddressMap.erase(holder.GetAddress()) == 1);
        HolderMap.Remove(holderId);
    }


    void DoAddChunkReplica(THolder& holder, TChunk& chunk, bool cached)
    {
        auto chunkId = chunk.GetId();
        auto holderId = holder.GetId();

        if (holder.HasChunk(chunkId, cached)) {
            LOG_WARNING_IF(!IsRecovery(), "Chunk replica is already added (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~ToString(cached),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        holder.AddChunk(chunkId, cached);
        chunk.AddLocation(holderId, cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica added (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~ToString(cached),
            ~holder.GetAddress(),
            holderId);

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaAdded(holder, chunk);
        }
    }

    void DoRemoveChunkReplica(THolder& holder, TChunk& chunk, bool cached)
    {
        auto chunkId = chunk.GetId();
        auto holderId = holder.GetId();

        if (!holder.HasChunk(chunkId, cached)) {
            LOG_WARNING_IF(!IsRecovery(), "Chunk replica is already removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~ToString(cached),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        holder.RemoveChunk(chunk.GetId(), cached);
        chunk.RemoveLocation(holder.GetId(), cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
             ~chunkId.ToString(),
             ~ToString(cached),
             ~holder.GetAddress(),
             holderId);

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaRemoved(holder, chunk);
        }
    }

    void DoRemoveChunkReplicaAtDeadHolder(const THolder& holder, TChunk& chunk, bool cached)
    {
        chunk.RemoveLocation(holder.GetId(), cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed since holder is dead (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
             ~chunk.GetId().ToString(),
             ~ToString(cached),
             ~holder.GetAddress(),
             holder.GetId());

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaRemoved(holder, chunk);
        }
    }


    void DoAddJob(THolder& holder, const TRspHolderHeartbeat::TJobStartInfo& jobInfo)
    {
        auto chunkId = TChunkId::FromProto(jobInfo.chunkid());
        auto jobId = TJobId::FromProto(jobInfo.jobid());
        auto targetAddresses = FromProto<Stroka>(jobInfo.targetaddresses());
        auto jobType = EJobType(jobInfo.type());

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
        const TReqHolderHeartbeat::TChunkAddInfo& chunkInfo)
    {
        auto holderId = holder.GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.chunkid());
        i64 size = chunkInfo.size();
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunkForUpdate(chunkId);
        if (!chunk) {
            LOG_INFO_IF(!IsRecovery(), "Unknown chunk added at holder, removal scheduled (Address: %s, HolderId: %d, ChunkId: %s, Cached: %s, Size: %" PRId64 ")",
                ~holder.GetAddress(),
                holderId,
                ~chunkId.ToString(),
                ~ToString(cached),
                size);
            if (IsLeader()) {
                ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
            }
            return;
        }

        // Use size reported by the holder.
        if (chunk->GetSize() != TChunk::UnknownSize && chunk->GetSize() != size) {
            LOG_FATAL("Chunk size mismatch (ChunkId: %s, Cached: %s, KnownSize: %" PRId64 ", NewSize: %" PRId64 ", Address: %s, HolderId: %d",
                ~chunkId.ToString(),
                ~ToString(cached),
                chunk->GetSize(),
                size,
                ~holder.GetAddress(),
                holder.GetId());
        }
        chunk->SetSize(size);

        DoAddChunkReplica(holder, *chunk, cached);
    }

    void ProcessRemovedChunk(
        THolder& holder,
        const TReqHolderHeartbeat::TChunkRemoveInfo& chunkInfo)
    {
        auto holderId = holder.GetId();
        auto chunkId = TChunkId::FromProto(chunkInfo.chunkid());
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunkForUpdate(chunkId);
        if (!chunk) {
            LOG_INFO_IF(!IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 ~ToString(cached),
                 ~holder.GetAddress(),
                 holderId);
            return;
        }

        DoRemoveChunkReplica(holder, *chunk, cached);
    }


    TJobList& GetOrCreateJobListForUpdate(const TChunkId& id)
    {
        auto* list = FindJobListForUpdate(id);
        if (list)
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
                    DropReplicationSinkIfEmpty(sink);
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

    void DropReplicationSinkIfEmpty(const TReplicationSink& sink)
    {
        if (sink.JobIds.empty()) {
            // NB: Do not try to inline this variable! erase() will destroy the object
            // and will access the key afterwards.
            Stroka address = sink.Address;
            YVERIFY(ReplicationSinkMap.erase(address) == 1);
        }
    }

};

DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Holder, THolder, THolderId, HolderMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, JobList, TJobList, TChunkId, JobListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Job, TJob, TJobId, JobMap)

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TConfig* config,
    NMetaState::IMetaStateManager* metaStateManager,
    NMetaState::TCompositeMetaState* metaState,
    TTransactionManager* transactionManager,
    IHolderRegistry* holderRegistry)
    : Config(config)
    , Impl(New<TImpl>(
        config,
        this,
        metaStateManager,
        metaState,
        transactionManager,
        holderRegistry))
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

TMetaChange<TChunkId>::TPtr TChunkManager::InitiateAllocateChunk(const TTransactionId& transactionId)
{
    return Impl->InitiateAllocateChunk(transactionId);
}

NMetaState::TMetaChange<TVoid>::TPtr TChunkManager::InitiateConfirmChunks(
    const NProto::TMsgConfirmChunks& message)
{
    return Impl->InitiateConfirmChunks(message);
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

void TChunkManager::FillHolderAddresses(
    ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
    const TChunk& chunk)
{
    FOREACH(auto holderId, chunk.StoredLocations()) {
        const THolder& holder = GetHolder(holderId);
        addresses->Add()->assign(holder.GetAddress());
    }

    if (~chunk.CachedLocations()) {
        FOREACH(auto holderId, *chunk.CachedLocations()) {
            const THolder& holder = GetHolder(holderId);
            addresses->Add()->assign(holder.GetAddress());
        }
    }
}

DELEGATE_METAMAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Holder, THolder, THolderId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, JobList, TJobList, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Job, TJob, TJobId, *Impl)

DELEGATE_BYREF_RW_PROPERTY(TChunkManager, TParamSignal<const THolder&>, HolderRegistered, *Impl);
DELEGATE_BYREF_RW_PROPERTY(TChunkManager, TParamSignal<const THolder&>, HolderUnregistered, *Impl);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
