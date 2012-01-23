#include "stdafx.h"
#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"
#include "holder_lease_tracker.h"
#include "holder_statistics.h"
#include "chunk_ypath.pb.h"
#include "chunk_list_ypath.pb.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/id_generator.h>
#include <ytlib/misc/string.h>
#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/transaction_server/transaction.h>
#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/object_server/type_handler_detail.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NChunkServer {

using namespace NProto;
using namespace NMetaState;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandler
    : public TObjectTypeHandlerBase<TChunk>
{
public:
    TChunkTypeHandler(TImpl* owner);

    virtual EObjectType GetType()
    {
        return EObjectType::Chunk;
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifest);

private:
    TImpl* Owner;

    virtual void OnObjectDestroyed(TChunk& chunk);

    virtual IObjectProxy::TPtr CreateProxy(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListTypeHandler
    : public TObjectTypeHandlerBase<TChunkList>
{
public:
    TChunkListTypeHandler(TImpl* owner);

    virtual EObjectType GetType()
    {
        return EObjectType::ChunkList;
    }

    virtual TObjectId CreateFromManifest(
        const TTransactionId& transactionId,
        IMapNode* manifest);

private:
    TImpl* Owner;

    virtual void OnObjectDestroyed(TChunkList& chunkList);

    virtual IObjectProxy::TPtr CreateProxy(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public NMetaState::TMetaStatePart
{
public:
    typedef TIntrusivePtr<TImpl> TPtr;

    TImpl(
        TConfig* config,
        TChunkManager* owner,
        NMetaState::IMetaStateManager* metaStateManager,
        NMetaState::TCompositeMetaState* metaState,
        TTransactionManager* transactionManager,
        IHolderAuthority* holderAuthority,
        TObjectManager* objectManager)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , Owner(owner)
        , TransactionManager(transactionManager)
        , HolderAuthority(holderAuthority)
        , ObjectManager(objectManager)
    {
        YASSERT(owner);
        YASSERT(transactionManager);
        YASSERT(holderAuthority);
        YASSERT(objectManager);

        RegisterMethod(this, &TImpl::HeartbeatRequest);
        RegisterMethod(this, &TImpl::HeartbeatResponse);
        RegisterMethod(this, &TImpl::RegisterHolder);
        RegisterMethod(this, &TImpl::UnregisterHolder);
        RegisterMethod(this, &TImpl::CreateChunks);

        metaState->RegisterLoader(
            "ChunkManager.1",
            FromMethod(&TChunkManager::TImpl::Load, TPtr(this)));
        metaState->RegisterSaver(
            "ChunkManager.1",
            FromMethod(&TChunkManager::TImpl::Save, TPtr(this)));

        metaState->RegisterPart(this);

        objectManager->RegisterHandler(~New<TChunkTypeHandler>(this));
        objectManager->RegisterHandler(~New<TChunkListTypeHandler>(this));
    }

    TObjectManager* GetObjectManager() const
    {
        return ~ObjectManager;
    }

    TMetaChange<THolderId>::TPtr InitiateRegisterHolder(
        const TMsgRegisterHolder& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::RegisterHolder,
            this);
    }

    TMetaChange<TVoid>::TPtr  InitiateUnregisterHolder(
        const TMsgUnregisterHolder& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::UnregisterHolder,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateHeartbeatRequest(
        const TMsgHeartbeatRequest& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::HeartbeatRequest,
            this);
    }

    TMetaChange<TVoid>::TPtr InitiateHeartbeatResponse(
        const TMsgHeartbeatResponse& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::HeartbeatResponse,
            this);
    }

    TMetaChange< yvector<TChunkId> >::TPtr InitiateAllocateChunk(
        const TMsgCreateChunks& message)
    {
        return CreateMetaChange(
            ~MetaStateManager,
            message,
            &TThis::CreateChunks,
            this);
    }

    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);
    DECLARE_METAMAP_ACCESSORS(Holder, THolder, THolderId);
    DECLARE_METAMAP_ACCESSORS(JobList, TJobList, TChunkId);
    DECLARE_METAMAP_ACCESSORS(Job, TJob, TJobId);

    DEFINE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderRegistered);
    DEFINE_BYREF_RW_PROPERTY(TParamSignal<const THolder&>, HolderUnregistered);


    const THolder* FindHolder(const Stroka& address)
    {
        auto it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolder(it->Second());
    }

    THolder* FindHolderForUpdate(const Stroka& address)
    {
        auto it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolderForUpdate(it->Second());
    }

    const TReplicationSink* FindReplicationSink(const Stroka& address)
    {
        auto it = ReplicationSinkMap.find(address);
        return it == ReplicationSinkMap.end() ? NULL : &it->Second();
    }

    yvector<THolderId> AllocateUploadTargets(int replicaCount)
    {
        auto holderIds = ChunkPlacement->GetUploadTargets(replicaCount);
        FOREACH (auto holderId, holderIds) {
            const auto& holder = GetHolder(holderId);
            ChunkPlacement->OnSessionHinted(holder);
        }
        return holderIds;
    }


    TChunk& CreateChunk()
    {
        auto id = ObjectManager->GenerateId(EObjectType::Chunk);
        auto* chunk = new TChunk(id);
        ChunkMap.Insert(id, chunk);
        return *chunk;
    }

    TChunkList& CreateChunkList()
    {
        auto id = ObjectManager->GenerateId(EObjectType::ChunkList);
        auto* chunkList = new TChunkList(id);
        ChunkListMap.Insert(id, chunkList);
        return *chunkList;
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

    const yhash_set<TChunkId>& LostChunkIds() const;
    const yhash_set<TChunkId>& OverreplicatedChunkIds() const;
    const yhash_set<TChunkId>& UnderreplicatedChunkIds() const;

    void FillHolderAddresses(
        ::google::protobuf::RepeatedPtrField< TProtoStringType>* addresses,
        const TChunk& chunk)
    {
        FOREACH (auto holderId, chunk.StoredLocations()) {
            const THolder& holder = GetHolder(holderId);
            addresses->Add()->assign(holder.GetAddress());
        }

        if (~chunk.CachedLocations()) {
            FOREACH (auto holderId, *chunk.CachedLocations()) {
                const THolder& holder = GetHolder(holderId);
                addresses->Add()->assign(holder.GetAddress());
            }
        }
    }

private:
    typedef TImpl TThis;
    friend class TChunkTypeHandler;
    friend class TChunkProxy;
    friend class TChunkListTypeHandler;
    friend class TChunkListProxy;

    TConfig::TPtr Config;
    TWeakPtr<TChunkManager> Owner;
    TTransactionManager::TPtr TransactionManager;
    IHolderAuthority::TPtr HolderAuthority;
    TObjectManager::TPtr ObjectManager;
    
    TChunkPlacement::TPtr ChunkPlacement;
    TChunkReplication::TPtr ChunkReplication;
    THolderLeaseTracker::TPtr HolderLeaseTracking;
    
    TIdGenerator<THolderId> HolderIdGenerator;

    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<TChunkListId, TChunkList> ChunkListMap;
    TMetaStateMap<THolderId, THolder> HolderMap;
    yhash_map<Stroka, THolderId> HolderAddressMap;
    TMetaStateMap<TChunkId, TJobList> JobListMap;
    TMetaStateMap<TJobId, TJob> JobMap;
    yhash_map<Stroka, TReplicationSink> ReplicationSinkMap;


    yvector<TChunkId> CreateChunks(const TMsgCreateChunks& message)
    {
        auto transactionId = TTransactionId::FromProto(message.transaction_id());
        int chunkCount = message.chunk_count();

        yvector<TChunkId> chunkIds;
        chunkIds.reserve(chunkCount);
        for (int index = 0; index < chunkCount; ++index) {
            auto chunkId = ObjectManager->GenerateId(EObjectType::Chunk);
            auto* chunk = new TChunk(chunkId);
            ChunkMap.Insert(chunkId, chunk);

            // The newly created chunk is referenced from the transaction.
            auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
            YVERIFY(transaction.CreatedObjectIds().insert(chunkId).second);
            ObjectManager->RefObject(chunkId);

            chunkIds.push_back(chunkId);
        }

        LOG_INFO_IF(!IsRecovery(), "Chunks allocated (ChunkIds: [%s], TransactionId: %s)",
            ~JoinToString(chunkIds),
            ~transactionId.ToString());

        return chunkIds;
    }


    void OnChunkDestroyed(TChunk& chunk)
    {
        auto chunkId = chunk.GetId();

        // Unregister chunk replicas from all known locations.

        FOREACH (auto holderId, chunk.StoredLocations()) {
            DoRemoveChunkFromLocation(holderId, chunk, false);
        }

        if (~chunk.CachedLocations()) {
            FOREACH (auto holderId, *chunk.CachedLocations()) {
                DoRemoveChunkFromLocation(holderId, chunk, true);
            }
        }
    }

    void OnChunkListDestroyed(TChunkList& chunkList)
    {
        // Drop references to children.
        FOREACH (const auto& childId, chunkList.ChildrenIds()) {
            ObjectManager->UnrefObject(childId);
        }
    }


    THolderId RegisterHolder(const TMsgRegisterHolder& message)
    {
        Stroka address = message.address();
        const auto& statistics = message.statistics();
    
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
            ~ToString(statistics));

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
        auto holderId = message.holder_id();

        const auto& holder = GetHolder(holderId);

        DoUnregisterHolder(holder);

        return TVoid();
    }


    TVoid HeartbeatRequest(const TMsgHeartbeatRequest& message)
    {
        auto holderId = message.holder_id();
        const auto& statistics = message.statistics();

        auto& holder = GetHolderForUpdate(holderId);
        holder.Statistics() = statistics;

        if (IsLeader()) {
            HolderLeaseTracking->RenewHolderLease(holder);
            ChunkPlacement->OnHolderUpdated(holder);
        }

        FOREACH (const auto& chunkInfo, message.added_chunks()) {
            ProcessAddedChunk(holder, chunkInfo);
        }

        FOREACH (const auto& chunkInfo, message.removed_chunks()) {
            ProcessRemovedChunk(holder, chunkInfo);
        }

        yvector<TChunkId> unapprovedChunkIds(
            holder.UnapprovedChunkIds().begin(), holder.UnapprovedChunkIds().end());
        FOREACH (const auto& chunkId, unapprovedChunkIds) {
            DoRemoveUnapprovedChunkReplica(holder, GetChunkForUpdate(chunkId));
        }

        bool isFirstHeartbeat = holder.GetState() == EHolderState::Inactive;
        if (isFirstHeartbeat) {
            holder.SetState(EHolderState::Active);
        }
        
        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat request (Address: %s, HolderId: %d, IsFirst: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
            ~holder.GetAddress(),
            holderId,
            ~::ToString(isFirstHeartbeat),
            ~ToString(statistics),
            static_cast<int>(message.added_chunks_size()),
            static_cast<int>(message.removed_chunks_size()));

        return TVoid();
    }

    TVoid HeartbeatResponse(const TMsgHeartbeatResponse& message)
    {
        auto holderId = message.holder_id();
        auto& holder = GetHolderForUpdate(holderId);

        FOREACH (const auto& startInfo, message.started_jobs()) {
            DoAddJob(holder, startInfo);
        }

        FOREACH(auto protoJobId, message.stopped_jobs()) {
            const auto* job = FindJob(TJobId::FromProto(protoJobId));
            if (job) {
                DoRemoveJob(holder, *job);
            }
        }

        LOG_DEBUG_IF(!IsRecovery(), "Heartbeat response (Address: %s, HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
            ~holder.GetAddress(),
            holderId,
            static_cast<int>(message.started_jobs_size()),
            static_cast<int>(message.stopped_jobs_size()));

        return TVoid();
    }

    TFuture<TVoid>::TPtr Save(const TCompositeMetaState::TSaveContext& context)
    {
        auto* output = context.Output;
        auto invoker = context.Invoker;

        auto holderIdGenerator = HolderIdGenerator;
        invoker->Invoke(FromFunctor([=] ()
            {
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
        ::Load(input, HolderIdGenerator);
        
        ChunkMap.Load(input);
        ChunkListMap.Load(input);
        HolderMap.Load(input);
        JobMap.Load(input);
        JobListMap.Load(input);

        // Reconstruct HolderAddressMap.
        HolderAddressMap.clear();
        FOREACH (const auto& pair, HolderMap) {
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
        ChunkPlacement = New<TChunkPlacement>(
            ~Owner.Lock(),
            ~Config);

        ChunkReplication = New<TChunkReplication>(
            ~Owner.Lock(),
            ~ChunkPlacement,
            ~Config,
            ~MetaStateManager->GetEpochStateInvoker());

        HolderLeaseTracking = New<THolderLeaseTracker>(
            ~Config,
            ~Owner.Lock(),
            ~MetaStateManager->GetEpochStateInvoker());

        FOREACH (const auto& pair, HolderMap) {
            StartHolderTracking(*pair.Second());
        }
    }

    virtual void OnStopLeading()
    {
        ChunkPlacement.Reset();
        ChunkReplication.Reset();
        HolderLeaseTracking.Reset();
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

        FOREACH (const auto& chunkId, holder.StoredChunkIds()) {
            auto& chunk = GetChunkForUpdate(chunkId);
            DoRemoveChunkReplicaAtDeadHolder(holder, chunk, false);
        }

        FOREACH (const auto& chunkId, holder.CachedChunkIds()) {
            auto& chunk = GetChunkForUpdate(chunkId);
            DoRemoveChunkReplicaAtDeadHolder(holder, chunk, true);
        }

        FOREACH (const auto& jobId, holder.JobIds()) {
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
                ~::ToString(cached),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        holder.AddChunk(chunkId, cached);
        chunk.AddLocation(holderId, cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica added (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
            ~chunkId.ToString(),
            ~::ToString(cached),
            ~holder.GetAddress(),
            holderId);

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaAdded(holder, chunk);
        }
    }

    void DoRemoveChunkFromLocation(THolderId holderId, TChunk& chunk, bool cached)
    {
        auto chunkId = chunk.GetId();
        auto& holder = GetHolderForUpdate(holderId);
        holder.RemoveChunk(chunkId, cached);

        if (!cached && IsLeader()) {
            ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
        }
    }

    void DoRemoveChunkReplica(THolder& holder, TChunk& chunk, bool cached)
    {
        auto chunkId = chunk.GetId();
        auto holderId = holder.GetId();

        if (!holder.HasChunk(chunkId, cached)) {
            LOG_WARNING_IF(!IsRecovery(), "Chunk replica is already removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~::ToString(cached),
                ~holder.GetAddress(),
                holderId);
            return;
        }

        holder.RemoveChunk(chunk.GetId(), cached);
        chunk.RemoveLocation(holder.GetId(), cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
             ~chunkId.ToString(),
             ~::ToString(cached),
             ~holder.GetAddress(),
             holderId);

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaRemoved(holder, chunk);
        }
    }

    void DoRemoveUnapprovedChunkReplica(THolder& holder, TChunk& chunk)
    {
         auto chunkId = chunk.GetId();
         auto holderId = holder.GetId();

         holder.RemoveUnapprovedChunk(chunk.GetId());
         chunk.RemoveLocation(holder.GetId(), false);

        LOG_INFO_IF(!IsRecovery(), "Unapproved chunk replica removed (ChunkId: %s, Address: %s, HolderId: %d)",
             ~chunkId.ToString(),
             ~holder.GetAddress(),
             holderId);

        if (IsLeader()) {
            ChunkReplication->OnReplicaRemoved(holder, chunk);
        }
    }

    void DoRemoveChunkReplicaAtDeadHolder(const THolder& holder, TChunk& chunk, bool cached)
    {
        chunk.RemoveLocation(holder.GetId(), cached);

        LOG_INFO_IF(!IsRecovery(), "Chunk replica removed since holder is dead (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
             ~chunk.GetId().ToString(),
             ~::ToString(cached),
             ~holder.GetAddress(),
             holder.GetId());

        if (!cached && IsLeader()) {
            ChunkReplication->OnReplicaRemoved(holder, chunk);
        }
    }


    void DoAddJob(THolder& holder, const TRspHolderHeartbeat::TJobStartInfo& jobInfo)
    {
        auto chunkId = TChunkId::FromProto(jobInfo.chunk_id());
        auto jobId = TJobId::FromProto(jobInfo.job_id());
        auto targetAddresses = FromProto<Stroka>(jobInfo.target_addresses());
        auto jobType = EJobType(jobInfo.type());
        auto startTime = TInstant(jobInfo.start_time());

        auto* job = new TJob(
            jobType,
            jobId,
            chunkId,
            holder.GetAddress(),
            targetAddresses,
            startTime);
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
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        i64 size = chunkInfo.size();
        bool cached = chunkInfo.cached();

        if (!cached && holder.HasUnapprovedChunk(chunkId)) {
            holder.ApproveChunk(chunkId);
            return;
        }

        auto* chunk = FindChunkForUpdate(chunkId);
        if (!chunk) {
            // Holders may still contain cached replicas of chunks that no longer exist.
            // Here we just silently ignore this case.
            if (cached) {
                return;
            }

            LOG_INFO_IF(!IsRecovery(), "Unknown chunk added at holder, removal scheduled (Address: %s, HolderId: %d, ChunkId: %s, Cached: %s, Size: %" PRId64 ")",
                ~holder.GetAddress(),
                holderId,
                ~chunkId.ToString(),
                ~::ToString(cached),
                size);
            if (IsLeader()) {
                ChunkReplication->ScheduleChunkRemoval(holder, chunkId);
            }
            return;
        }

        // Use the size reported by the holder, but check it for consistency first.
        if (chunk->GetSize() != TChunk::UnknownSize && chunk->GetSize() != size) {
            LOG_FATAL("Chunk size mismatch (ChunkId: %s, Cached: %s, KnownSize: %" PRId64 ", NewSize: %" PRId64 ", Address: %s, HolderId: %d",
                ~chunkId.ToString(),
                ~::ToString(cached),
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
        auto chunkId = TChunkId::FromProto(chunkInfo.chunk_id());
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunkForUpdate(chunkId);
        if (!chunk) {
            LOG_INFO_IF(!IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 ~::ToString(cached),
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
                    YASSERT(sink.JobIds().insert(job.GetJobId()).Second());
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
                    YASSERT(sink.JobIds().erase(job.GetJobId()) == 1);
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
        if (sink.JobIds().empty()) {
            // NB: Do not try to inline this variable! erase() will destroy the object
            // and will access the key afterwards.
            auto address = sink.GetAddress();
            YVERIFY(ReplicationSinkMap.erase(address) == 1);
        }
    }

};

DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Holder, THolder, THolderId, HolderMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, JobList, TJobList, TChunkId, JobListMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Job, TJob, TJobId, JobMap)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, LostChunkIds, *ChunkReplication);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, OverreplicatedChunkIds, *ChunkReplication);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunkId>, UnderreplicatedChunkIds, *ChunkReplication);

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkProxy
    : public NObjectServer::TObjectProxyBase<TChunk>
{
public:
    TChunkProxy(TImpl* owner, const TChunkId& id)
        : TBase(~owner->ObjectManager, id, &owner->ChunkMap, ChunkServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        DECLARE_LOGGED_YPATH_SERVICE_METHOD(Confirm);
        return TBase::IsLogged(context);
    }

private:
    typedef TObjectProxyBase<TChunk> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributeNames(yvector<Stroka>* names)
    {
        const auto& chunk = GetTypedImpl();
        names->push_back("confirmed");
        names->push_back("cached_locations");
        names->push_back("stored_locations");
        if (chunk.IsConfirmed()) {
            names->push_back("size");
            names->push_back("chunk_type");
        }
        TBase::GetSystemAttributeNames(names);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& chunk = GetTypedImpl();

        if (name == "confirmed") {
            BuildYsonFluently(consumer)
                .Scalar(FormatBool(chunk.IsConfirmed()));
            return true;
        }

        if (name == "cached_locations") {
            if (~chunk.CachedLocations()) {
                BuildYsonFluently(consumer)
                    .DoListFor(*chunk.CachedLocations(), [=] (TFluentList fluent, THolderId holderId)
                        {
                            const auto& holder = Owner->GetHolder(holderId);
                            fluent.Item().Scalar(holder.GetAddress());
                        });
            } else {
                BuildYsonFluently(consumer)
                    .BeginList()
                    .EndList();
            }
            return true;
        }

        if (name == "stored_locations") {
            BuildYsonFluently(consumer)
                .DoListFor(chunk.StoredLocations(), [=] (TFluentList fluent, THolderId holderId)
                    {
                        const auto& holder = Owner->GetHolder(holderId);
                        fluent.Item().Scalar(holder.GetAddress());
                    });
            return true;
        }

        if (chunk.IsConfirmed()) {
            if (name == "size") {
                BuildYsonFluently(consumer)
                    .Scalar(chunk.GetSize());
                return true;
            }

            if (name == "chunk_type") {
                auto attributes = chunk.DeserializeAttributes();
                auto type = EChunkType(attributes.type());
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(type.ToString()));
                return true;
            }
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch)
    {
        UNUSED(request);

        const auto& chunk = GetTypedImpl();
        Owner->FillHolderAddresses(response->mutable_holder_addresses(), chunk);

        context->SetResponseInfo("HolderAddresses: [%s]",
            ~JoinToString(response->holder_addresses()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Confirm)
    {
        UNUSED(response);

        auto& holderAddresses = request->holder_addresses();
        YASSERT(holderAddresses.size() != 0);

        context->SetRequestInfo("HolderAddresses: [%s]", ~JoinToString(holderAddresses));

        auto& chunk = GetTypedImplForUpdate();
        auto chunkId = chunk.GetId();
        
        FOREACH (const auto& address, holderAddresses) {
            auto* holder = Owner->FindHolderForUpdate(address);
            if (!holder) {
                LOG_WARNING("Chunk is confirmed at unknown holder (ChunkId: %s, HolderAddress: %s)",
                    ~chunkId.ToString(),
                    ~address);
                continue;
            }

            chunk.AddLocation(holder->GetId(), false);
            holder->AddUnapprovedChunk(chunk.GetId());

            if (Owner->IsLeader()) {
                Owner->ChunkReplication->OnReplicaAdded(*holder, chunk);
            }
        }

        // Skip chunks that are already confirmed.
        if (!chunk.IsConfirmed()) {
            TBlob blob;
            if (!SerializeProtobuf(&request->attributes(), &blob)) {
                LOG_FATAL("Error serializing chunk attributes (ChunkId: %s)", ~chunkId.ToString());
            }

            chunk.SetAttributes(TSharedRef(MoveRV(blob)));

            LOG_INFO_IF(!Owner->IsRecovery(), "Chunk confirmed (ChunkId: %s)", ~chunkId.ToString());
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandler::TChunkTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(~owner->ObjectManager, &owner->ChunkMap)
    , Owner(owner)
{ }

IObjectProxy::TPtr TChunkManager::TChunkTypeHandler::CreateProxy(const TObjectId& id)
{
    return New<TChunkProxy>(Owner, id);
}

TObjectId TChunkManager::TChunkTypeHandler::CreateFromManifest(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);
    return Owner->CreateChunk().GetId();
}

void TChunkManager::TChunkTypeHandler::OnObjectDestroyed(TChunk& chunk)
{
    Owner->OnChunkDestroyed(chunk);
}

///////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListProxy
    : public NObjectServer::TObjectProxyBase<TChunkList>
{
public:
    TChunkListProxy(TImpl* owner, const TChunkListId& id)
        : TBase(~owner->ObjectManager, id, &owner->ChunkListMap, ChunkServerLogger.GetCategory())
        , Owner(owner)
    { }

    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        Stroka verb = context->GetVerb();
        if (verb == "Attach" ||
            verb == "Detach")
        {
            return true;
        }
        return TBase::IsLogged(context);;
    }

private:
    typedef TObjectProxyBase<TChunkList> TBase;

    TIntrusivePtr<TImpl> Owner;

    virtual void GetSystemAttributeNames(yvector<Stroka>* names)
    {
        names->push_back("children_ids");
        TBase::GetSystemAttributeNames(names);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& chunkList = GetTypedImpl();

        if (name == "children_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkList.ChildrenIds(), [=] (TFluentList fluent, TTransactionId id)
                    {
                        fluent.Item().Scalar(id.ToString());
                    });
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "Attach") {
            AttachThunk(context);
        } else if (verb == "Detach") {
            DetachThunk(context);
        } else {
            TBase::DoInvoke(context);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Attach)
    {
        UNUSED(response);

        auto childrenIds = FromProto<TChunkTreeId>(request->children_ids());

        context->SetRequestInfo("ChildrenIds: [%s]", ~JoinToString(childrenIds));

        auto& chunkList = GetTypedImplForUpdate();

        FOREACH (const auto& childId, childrenIds) {
            // TODO(babenko): check that all attached chunks are confirmed.
            chunkList.ChildrenIds().push_back(childId);
            Owner->ObjectManager->RefObject(childId);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Detach)
    {
        UNUSED(response);

        auto childrenIdsList = FromProto<TChunkTreeId>(request->children_ids());
        yhash_set<TChunkTreeId> childrenIdsSet(childrenIdsList.begin(), childrenIdsList.end());

        context->SetRequestInfo("ChildrenIds: [%s]", ~JoinToString(childrenIdsList));

        auto& chunkList = GetTypedImplForUpdate();

        auto it = chunkList.ChildrenIds().begin();
        while (it != chunkList.ChildrenIds().end()) {
            auto jt = it;
            ++jt;
            const auto& childId = *it;
            if (childrenIdsSet.find(childId) != childrenIdsSet.end()) {
                chunkList.ChildrenIds().erase(it);
                Owner->ObjectManager->UnrefObject(childId);
            }
            it = jt;
        }

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerBase(~owner->ObjectManager, &owner->ChunkListMap)
    , Owner(owner)
{ }

IObjectProxy::TPtr TChunkManager::TChunkListTypeHandler::CreateProxy(const TObjectId& id)
{
    return New<TChunkListProxy>(Owner, id);
}

TObjectId TChunkManager::TChunkListTypeHandler::CreateFromManifest(
    const TTransactionId& transactionId,
    IMapNode* manifest)
{
    UNUSED(transactionId);
    UNUSED(manifest);
    return Owner->CreateChunkList().GetId();
}

void TChunkManager::TChunkListTypeHandler::OnObjectDestroyed(TChunkList& chunkList)
{
    Owner->OnChunkListDestroyed(chunkList);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TConfig* config,
    NMetaState::IMetaStateManager* metaStateManager,
    NMetaState::TCompositeMetaState* metaState,
    TTransactionManager* transactionManager,
    IHolderAuthority* holderAuthority,
    TObjectManager* objectManager)
    : Impl(New<TImpl>(
        config,
        this,
        metaStateManager,
        metaState,
        transactionManager,
        holderAuthority,
        objectManager))
{ }

TObjectManager* TChunkManager::GetObjectManager() const
{
    return Impl->GetObjectManager();
}

const THolder* TChunkManager::FindHolder(const Stroka& address)
{
    return Impl->FindHolder(address);
}

THolder* TChunkManager::FindHolderForUpdate(const Stroka& address)
{
    return Impl->FindHolderForUpdate(address);
}

const TReplicationSink* TChunkManager::FindReplicationSink(const Stroka& address)
{
    return Impl->FindReplicationSink(address);
}

yvector<THolderId> TChunkManager::AllocateUploadTargets(int replicaCount)
{
    return Impl->AllocateUploadTargets(replicaCount);
}

TMetaChange<THolderId>::TPtr TChunkManager::InitiateRegisterHolder(
    const TMsgRegisterHolder& message)
{
    return Impl->InitiateRegisterHolder(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateUnregisterHolder(
    const TMsgUnregisterHolder& message)
{
    return Impl->InitiateUnregisterHolder(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateHeartbeatRequest(
    const TMsgHeartbeatRequest& message)
{
    return Impl->InitiateHeartbeatRequest(message);
}

TMetaChange<TVoid>::TPtr TChunkManager::InitiateHeartbeatResponse(
    const TMsgHeartbeatResponse& message)
{
    return Impl->InitiateHeartbeatResponse(message);
}

TMetaChange< yvector<TChunkId> >::TPtr TChunkManager::InitiateCreateChunks(
    const TMsgCreateChunks& message)
{
    return Impl->InitiateAllocateChunk(message);
}

TChunk& TChunkManager::CreateChunk()
{
    return Impl->CreateChunk();
}

TChunkList& TChunkManager::CreateChunkList()
{
    return Impl->CreateChunkList();
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
    Impl->FillHolderAddresses(addresses, chunk);
}

DELEGATE_METAMAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Holder, THolder, THolderId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, JobList, TJobList, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, Job, TJob, TJobId, *Impl)

DELEGATE_BYREF_RW_PROPERTY(TChunkManager, TParamSignal<const THolder&>, HolderRegistered, *Impl);
DELEGATE_BYREF_RW_PROPERTY(TChunkManager, TParamSignal<const THolder&>, HolderUnregistered, *Impl);

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, LostChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, OverreplicatedChunkIds, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunkId>, UnderreplicatedChunkIds, *Impl);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
