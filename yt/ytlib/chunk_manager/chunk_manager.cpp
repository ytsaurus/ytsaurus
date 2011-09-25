#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"
#include "holder_expiration.h"

#include "../misc/foreach.h"
#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../misc/assert.h"
#include "../misc/string.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TState
    : public TMetaStatePart
    , public NTransaction::ITransactionHandler
{
public:
    typedef TIntrusivePtr<TState> TPtr;

    TState(
        const TConfig& config,
        TMetaStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager,
        TChunkReplication::TPtr chunkReplication,
        TChunkPlacement::TPtr chunkPlacement,
        THolderExpiration::TPtr holderExpiration)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , TransactionManager(transactionManager)
        , ChunkReplication(chunkReplication)
        , ChunkPlacement(chunkPlacement)
        , HolderExpiration(holderExpiration)
        , CurrentHolderId(0)
    {
        RegisterMethod(this, &TState::AddChunk);
        RegisterMethod(this, &TState::RemoveChunk);
        RegisterMethod(this, &TState::RegisterHolder);
        RegisterMethod(this, &TState::UnregisterHolder);
        RegisterMethod(this, &TState::HeartbeatRequest);
        RegisterMethod(this, &TState::HeartbeatResponse);

        transactionManager->RegisterHander(this);
    }


    TChunkId AddChunk(const NProto::TMsgAddChunk& message)
    {
        auto chunkId = TChunkId::FromProto(message.GetChunkId());
        auto transactionId = TTransactionId::FromProto(message.GetTransactionId());
        
        TChunk chunk(chunkId, transactionId);

        auto& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.AddedChunks.push_back(chunkId);

        ChunkMap.Insert(chunkId, chunk);

        LOG_INFO("Chunk added (ChunkId: %s)",
            ~chunkId.ToString());

        return chunkId;
    }

    TVoid RemoveChunk(const NProto::TMsgRemoveChunk& message)
    {
        TChunkId chunkId = TChunkId::FromProto(message.GetChunkId());
        
        const TChunk& chunk = GetChunk(chunkId);
        DoRemoveChunk(chunk);

        return TVoid();
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

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, THolderId);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

    THolderId RegisterHolder(const NProto::TReqRegisterHolder& message)
    {
        Stroka address = message.GetAddress();
        auto statistics = THolderStatistics::FromProto(message.GetStatistics());
    
        THolderId holderId = CurrentHolderId++;
    
        const auto* existingHolder = FindHolder(address);
        if (existingHolder != NULL) {
            LOG_INFO("Holder kicked off due to address conflict (Address: %s, HolderId: %d)",
                ~address,
                existingHolder->Id);
            DoUnregisterHolder(*existingHolder);
        }

        THolder newHolder(
            holderId,
            address,
            EHolderState::Registered,
            statistics);

        YVERIFY(HolderMap.Insert(holderId, newHolder));
        YVERIFY(HolderAddressMap.insert(MakePair(address, holderId)).Second());

        if (IsLeader()) {
            StartHolderTracking(newHolder);
        }

        LOG_INFO("Holder registered (Address: %s, HolderId: %d, %s)",
            ~address,
            holderId,
            ~statistics.ToString());

        return holderId;
    }

    TVoid UnregisterHolder(const NProto::TMsgUnregisterHolder& message)
    { 
        auto holderId = message.GetHolderId();

        const auto& holder = GetHolder(holderId);

        DoUnregisterHolder(holder);

        return TVoid();
    }

    TVoid HeartbeatRequest(const NProto::TMsgHeartbeatRequest& message)
    {
        auto holderId = message.GetHolderId();
        auto statistics = THolderStatistics::FromProto(message.GetStatistics());

        auto& holder = GetHolderForUpdate(holderId);
        holder.Statistics = statistics;

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

        bool isFirstHeartbeat = holder.State == EHolderState::Registered;
        if (isFirstHeartbeat) {
            holder.State = EHolderState::Active;
        }
        
        LOG_DEBUG("Heartbeat request (Address: %s, HolderId: %d, IsFirst: %s, %s, ChunksAdded: %d, ChunksRemoved: %d)",
            ~holder.Address,
            holderId,
            ~ToString(isFirstHeartbeat),
            ~statistics.ToString(),
            static_cast<int>(message.AddedChunksSize()),
            static_cast<int>(message.RemovedChunksSize()));

        return TVoid();
    }

    TVoid HeartbeatResponse(const NProto::TMsgHeartbeatResponse& message)
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

        LOG_DEBUG("Heartbeat response (Address: %s, HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
            ~holder.Address,
            holderId,
            static_cast<int>(message.StartedJobsSize()),
            static_cast<int>(message.StoppedJobsSize()));

        return TVoid();
    }

private:
    TConfig Config;
    TTransactionManager::TPtr TransactionManager;
    TChunkReplication::TPtr ChunkReplication;
    TChunkPlacement::TPtr ChunkPlacement;
    THolderExpiration::TPtr HolderExpiration;
    THolderId CurrentHolderId;
    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<THolderId, THolder> HolderMap;
    yhash_map<Stroka, THolderId> HolderAddressMap;
    TMetaStateMap<TChunkId, TJobList> JobListMap;
    TMetaStateMap<TJobId, TJob> JobMap;
    yhash_map<Stroka, TReplicationSink> ReplicationSinkMap;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "ChunkManager";
    }

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream* stream)
    {
        auto invoker = GetSnapshotInvoker();
        invoker->Invoke(FromMethod(&TState::DoSave, TPtr(this), stream));
        HolderMap.Save(invoker, stream);
        return ChunkMap.Save(invoker, stream);
    }

    //! Saves the local state (not including the maps).
    void DoSave(TOutputStream* stream)
    {
        *stream << CurrentHolderId;
    }

    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream* stream)
    {
        auto invoker = GetSnapshotInvoker();
        invoker->Invoke(FromMethod(&TState::DoLoad, TPtr(this), stream));
        HolderMap.Load(invoker, stream);
        return ChunkMap.Load(invoker, stream)->Apply(FromMethod(
            &TState::OnLoaded,
            TPtr(this)));
    }

    //! Loads the local state (not including the maps).
    void DoLoad(TInputStream* stream)
    {
        *stream >> CurrentHolderId;
    }

    TVoid OnLoaded(TVoid)
    {
        // Reconstruct HolderAddressMap.
        HolderAddressMap.clear();
        FOREACH(const auto& pair, HolderMap) {
            const auto& holder = pair.Second();
            YVERIFY(HolderAddressMap.insert(MakePair(holder.Address, holder.Id)).Second());
        }

        // Reconstruct ReplicationSinkMap.
        ReplicationSinkMap.clear();
        FOREACH (const auto& pair, JobMap) {
            RegisterReplicationSinks(pair.Second());
        }

        // TODO: Reconstruct JobListMap

        return TVoid();
    }

    virtual void Clear()
    {
        HolderMap.Clear();
        HolderAddressMap.clear();
        ChunkMap.Clear();
        JobListMap.Clear();
        JobMap.Clear();
    }

    virtual void OnStartLeading()
    {
        TMetaStatePart::OnStartLeading();

        HolderExpiration->Start(GetEpochStateInvoker());
        FOREACH(auto pair, HolderMap) {
            StartHolderTracking(pair.Second());
        }

        ChunkReplication->Start(GetEpochStateInvoker());
    }

    virtual void OnStopLeading()
    {
        TMetaStatePart::OnStopLeading();

        FOREACH(auto pair, HolderMap) {
            StopHolderTracking(pair.Second());
        }
        HolderExpiration->Stop();

        ChunkReplication->Stop();
    }


    // ITransactionHandler overrides.
    virtual void OnTransactionStarted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionCommitted(TTransaction& transaction)
    {
        FOREACH(const TChunkId& chunkId, transaction.AddedChunks) {
            TChunk& chunk = GetChunkForUpdate(chunkId);
            chunk.TransactionId = TTransactionId();

            LOG_DEBUG("Chunk committed (ChunkId: %s)",
                ~chunk.Id.ToString());
        }

        // TODO: handle removed chunks
    }

    virtual void OnTransactionAborted(TTransaction& transaction)
    {
        FOREACH(const TChunkId& chunkId, transaction.AddedChunks) {
            const TChunk& chunk = GetChunk(chunkId);
            DoRemoveChunk(chunk);
        }

        // TODO: handle removed chunks
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
        THolderId holderId = holder.Id;

        if (IsLeader()) {
            StopHolderTracking(holder);
        }

        FOREACH(const TChunkId& chunkId, holder.Chunks) {
            TChunk& chunk = GetChunkForUpdate(chunkId);
            DoRemovedChunkReplicaAtDeadHolder(holder, chunk);
        }

        FOREACH(const TJobId& jobId, holder.Jobs) {
            const TJob& job = GetJob(jobId);
            DoRemoveJobAtDeadHolder(holder, job);
        }

        LOG_INFO("Holder unregistered (Address: %s, HolderId: %d)",
            ~holder.Address,
            holderId);

        YVERIFY(HolderAddressMap.erase(holder.Address) == 1);
        YVERIFY(HolderMap.Remove(holderId));
    }

    void DoRemoveChunk(const TChunk& chunk)
    {
        TChunkId chunkId = chunk.Id;
        YVERIFY(ChunkMap.Remove(chunkId));

        LOG_INFO("Chunk removed (ChunkId: %s)",
            ~chunkId.ToString());
    }

    void DoAddChunkReplica(THolder& holder, TChunk& chunk)
    {
        YVERIFY(holder.Chunks.insert(chunk.Id).Second());
        chunk.AddLocation(holder.Id);

        LOG_INFO("Chunk replica added (ChunkId: %s, Address: %s, HolderId: %d, Size: %" PRId64 ")",
            ~chunk.Id.ToString(),
            ~holder.Address,
            holder.Id,
            chunk.Size);

        if (IsLeader()) {
            ChunkReplication->AddReplica(holder, chunk);
        }
    }

    void DoRemoveChunkReplica(THolder& holder, TChunk& chunk)
    {
        YVERIFY(holder.Chunks.erase(chunk.Id) == 1);
        chunk.RemoveLocation(holder.Id);

        LOG_INFO("Chunk replica removed (ChunkId: %s, Address: %s, HolderId: %d)",
             ~chunk.Id.ToString(),
             ~holder.Address,
             holder.Id);

        if (IsLeader()) {
            ChunkReplication->RemoveReplica(holder, chunk);
        }
    }

    void DoRemovedChunkReplicaAtDeadHolder(const THolder& holder, TChunk& chunk)
    {
        chunk.RemoveLocation(holder.Id);

        LOG_INFO("Chunk replica removed due to holder's death (ChunkId: %s, Address: %s, HolderId: %d)",
             ~chunk.Id.ToString(),
             ~holder.Address,
             holder.Id);

        if (IsLeader()) {
            ChunkReplication->RemoveReplica(holder, chunk);
        }
    }

    void DoAddJob(THolder& holder, const NProto::TJobStartInfo& jobInfo)
    {
        auto chunkId = TChunkId::FromProto(jobInfo.GetChunkId());
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        auto targetAddresses = FromProto<Stroka>(jobInfo.GetTargetAddresses());
        auto jobType = EJobType(jobInfo.GetType());

        TJob job(
            jobType,
            jobId,
            chunkId,
            holder.Address,
            targetAddresses);
        YVERIFY(JobMap.Insert(jobId, job));

        auto& list = GetOrCreateJobListForUpdate(chunkId);
        list.AddJob(jobId);

        holder.AddJob(jobId);

        RegisterReplicationSinks(job);

        LOG_INFO("Job added (JobId: %s, Address: %s, HolderId: %d, JobType: %s, ChunkId: %s)",
            ~jobId.ToString(),
            ~holder.Address,
            holder.Id,
            ~jobType.ToString(),
            ~chunkId.ToString());
    }

    void DoRemoveJob(THolder& holder, const TJob& job)
    {
        auto jobId = job.JobId;

        auto& list = GetJobListForUpdate(job.ChunkId);
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        holder.RemoveJob(jobId);

        UnregisterReplicationSinks(job);

        YVERIFY(JobMap.Remove(job.JobId));

        LOG_INFO("Job removed (JobId: %s, Address: %s, HolderId: %d)",
            ~jobId.ToString(),
            ~holder.Address,
            holder.Id);
    }

    void DoRemoveJobAtDeadHolder(const THolder& holder, const TJob& job)
    {
        auto jobId = job.JobId;

        auto& list = GetJobListForUpdate(job.ChunkId);
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        UnregisterReplicationSinks(job);

        YVERIFY(JobMap.Remove(job.JobId));

        LOG_INFO("Job removed due to holder's death (JobId: %s, Address: %s, HolderId: %d)",
            ~jobId.ToString(),
            ~holder.Address,
            holder.Id);
    }


    void ProcessAddedChunk(
        THolder& holder,
        const NProto::TChunkInfo& chunkInfo)
    {
        auto holderId = holder.Id;
        auto chunkId = TChunkId::FromProto(chunkInfo.GetId());
        i64 size = chunkInfo.GetSize();

        TChunk* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_ERROR("Unknown chunk added at holder (Address: %s, HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
                ~holder.Address,
                holderId,
                ~chunkId.ToString(),
                size);
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
        auto holderId = holder.Id;

        auto* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_DEBUG("Unknown chunk replica removed (ChunkId: %s, Address: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 ~holder.Address,
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

        YVERIFY(JobListMap.Insert(id, TJobList(id)));
        return GetJobListForUpdate(id);
    }

    void MaybeDropJobList(const TJobList& list)
    {
        if (list.Jobs.empty()) {
            JobListMap.Remove(list.ChunkId);
        }
    }


    void RegisterReplicationSinks(const TJob& job)
    {
        switch (job.Type) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds.insert(job.JobId).Second());
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YASSERT(false);
                break;
        }
    }

    void UnregisterReplicationSinks(const TJob& job)
    {
        switch (job.Type) {
            case EJobType::Replicate: {
                FOREACH (const auto& address, job.TargetAddresses) {
                    auto& sink = GetOrCreateReplicationSink(address);
                    YASSERT(sink.JobIds.erase(job.JobId) == 1);
                    MaybeDropReplicationSink(sink);
                }
                break;
            }

            case EJobType::Remove:
                break;

            default:
                YASSERT(false);
                break;
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

METAMAP_ACCESSORS_IMPL(TChunkManager::TState, Chunk, TChunk, TChunkId, ChunkMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, Holder, THolder, THolderId, HolderMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, JobList, TJobList, TChunkId, JobListMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, Job, TJob, TJobId, JobMap)

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState,
    NRpc::TServer::TPtr server,
    TTransactionManager::TPtr transactionManager)
    : TMetaStateServiceBase(
        metaState->GetInvoker(),
        TChunkManagerProxy::GetServiceName(),
        ChunkManagerLogger.GetCategory())
    , Config(config)
    , TransactionManager(transactionManager)
    , ChunkPlacement(New<TChunkPlacement>(
        this))
    , ChunkReplication(New<TChunkReplication>(
        this,
        ChunkPlacement))
    , HolderExpiration(New<THolderExpiration>(
        config,
        this))
    , State(New<TState>(
        config,
        metaStateManager,
        metaState,
        transactionManager,
        ChunkReplication,
        ChunkPlacement,
        HolderExpiration))
{
    RegisterMethods();
    metaState->RegisterPart(~State);
    server->RegisterService(this);
}

void TChunkManager::RegisterMethods()
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterHolder));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(HolderHeartbeat));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(AddChunk));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(FindChunk));
}

void TChunkManager::ValidateHolderId(THolderId holderId)
{
    const auto* holder = FindHolder(holderId);
    if (holder == NULL) {
        ythrow NRpc::TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("Invalid or expired holder %d", holderId);
    }
}

void TChunkManager::ValidateChunkId(
    const TChunkId& chunkId,
    const TTransactionId& transactionId)
{
    const auto* chunk = FindChunk(chunkId);
    if (chunk == NULL || !chunk->IsVisible(transactionId)) {
        ythrow NRpc::TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("Invalid chunk %s", ~chunkId.ToString());
    }
}

void TChunkManager::ValidateTransactionId(const TTransactionId& transactionId)
{
    const auto* transaction = TransactionManager->FindTransaction(transactionId);
    if (transaction == NULL) {
        ythrow NRpc::TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("Invalid transaction %s", ~transactionId.ToString());
    }
}

void TChunkManager::UnregisterHolder(THolderId holderId)
{
    NProto::TMsgUnregisterHolder message;
    message.SetHolderId(holderId);
    CommitChange(
        State, message,
        &TState::UnregisterHolder);
}

const THolder* TChunkManager::FindHolder(const Stroka& address)
{
    return State->FindHolder(address);
}

const TReplicationSink* TChunkManager::FindReplicationSink(const Stroka& address)
{
    return State->FindReplicationSink(address);
}

METAMAP_ACCESSORS_FWD(TChunkManager, Chunk, TChunk, TChunkId, *State)
METAMAP_ACCESSORS_FWD(TChunkManager, Holder, THolder, THolderId, *State)
METAMAP_ACCESSORS_FWD(TChunkManager, JobList, TJobList, TChunkId, *State)
METAMAP_ACCESSORS_FWD(TChunkManager, Job, TJob, TJobId, *State)

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkManager, RegisterHolder)
{
    UNUSED(response);

    Stroka address = request->GetAddress();
    auto statistics = THolderStatistics::FromProto(request->GetStatistics());
    
    context->SetRequestInfo("Address: %s, %s",
        ~address,
        ~statistics.ToString());

    const NProto::TReqRegisterHolder& unregisterMessage = *request;
    CommitChange(
        this, context, State, unregisterMessage,
        &TState::RegisterHolder,
        &TThis::OnHolderRegistered);
}

void TChunkManager::OnHolderRegistered(
    THolderId id,
    TCtxRegisterHolder::TPtr context)
{
    auto* response = &context->Response();
    response->SetHolderId(id);
    context->SetResponseInfo("HolderId: %d", id);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, HolderHeartbeat)
{
    // TODO: do not commit if no changes reported
    UNUSED(response);

    auto holderId = request->GetHolderId();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    const auto& holder = GetHolder(holderId);

    context->SetRequestInfo("Address: %s, HolderId: %d",
        ~holder.Address,
        holderId);

    NProto::TMsgHeartbeatRequest requestMessage;
    requestMessage.SetHolderId(holderId);
    *requestMessage.MutableStatistics() = request->GetStatistics();

    FOREACH(const auto& chunkInfo, request->GetAddedChunks()) {
        auto chunkId = TChunkId::FromProto(chunkInfo.GetId());
        if (holder.Chunks.find(chunkId) == holder.Chunks.end()) {
            *requestMessage.AddAddedChunks() = chunkInfo;
        } else {
            LOG_WARNING("Chunk replica is already added (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.Address,
                holder.Id);
        }
    }

    FOREACH(const auto& protoChunkId, request->GetRemovedChunks()) {
        auto chunkId = TChunkId::FromProto(protoChunkId);
        if (holder.Chunks.find(chunkId) != holder.Chunks.end()) {
            requestMessage.AddRemovedChunks(chunkId.ToProto());
        } else {
            LOG_WARNING("Chunk replica does not exist or already removed (ChunkId: %s, Address: %s, HolderId: %d)",
                ~chunkId.ToString(),
                ~holder.Address,
                holder.Id);
        }
    }

    CommitChange(
        State, requestMessage,
        &TState::HeartbeatRequest);

    yvector<NProto::TJobInfo> runningJobs;
    runningJobs.reserve(request->JobsSize());
    FOREACH(const auto& jobInfo, request->GetJobs()) {
        auto jobId = TJobId::FromProto(jobInfo.GetJobId());
        const TJob* job = State->FindJob(jobId);
        if (job == NULL) {
            LOG_INFO("Stopping unknown or obsolete job (JobId: %s, Address: %s, HolderId: %d)",
                ~jobId.ToString(),
                ~holder.Address,
                holder.Id);
            response->AddJobsToStop(jobId.ToProto());
        } else {
            runningJobs.push_back(jobInfo);
        }
    }

    yvector<NProto::TJobStartInfo> jobsToStart;
    yvector<TJobId> jobsToStop;
    ChunkReplication->RunJobControl(
        holder,
        runningJobs,
        &jobsToStart,
        &jobsToStop);

    ToProto(*response->MutableJobsToStart(), jobsToStart);
    ToProto(*response->MutableJobsToStop(), jobsToStop, false);

    NProto::TMsgHeartbeatResponse responseMessage;
    responseMessage.SetHolderId(holderId);
    responseMessage.MutableStartedJobs()->MergeFrom(response->GetJobsToStart());
    responseMessage.MutableStoppedJobs()->MergeFrom(response->GetJobsToStop());

    CommitChange(
        this, context, State, responseMessage,
        &TState::HeartbeatResponse,
        &TThis::OnHolderHeartbeatProcessed);
}

void TChunkManager::OnHolderHeartbeatProcessed(
    TVoid,
    TCtxHolderHeartbeat::TPtr context)
{
    auto* response = &context->Response();

    context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
        static_cast<int>(response->JobsToStartSize()),
        static_cast<int>(response->JobsToStopSize()));

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    int replicaCount = request->GetReplicaCount();

    context->SetRequestInfo("TransactionId: %s, ReplicaCount: %d",
        ~transactionId.ToString(),
        replicaCount);

    auto holderIds = ChunkPlacement->GetUploadTargets(replicaCount);
    FOREACH(auto holderId, holderIds) {
        const THolder& holder = GetHolder(holderId);
        response->AddHolderAddresses(holder.Address);
    }

    auto chunkId = TChunkId::Create();

    NProto::TMsgAddChunk message;
    message.SetChunkId(chunkId.ToProto());
    message.SetTransactionId(transactionId.ToProto());

    CommitChange(
        this, context, State, message,
        &TState::AddChunk,
        &TThis::OnChunkAdded);
}

void TChunkManager::OnChunkAdded(
    TChunkId id,
    TCtxAddChunk::TPtr context)
{
    auto* response = &context->Response();
    response->SetChunkId(id.ToProto());

    context->SetResponseInfo("ChunkId: %s, Addresses: [%s]",
        ~id.ToString(),
        ~JoinToString(response->GetHolderAddresses(), ", "));

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, FindChunk)
{
    auto transactionId = TTransactionId::FromProto(request->GetTransactionId());
    auto chunkId = TChunkId::FromProto(request->GetChunkId());

    context->SetRequestInfo("TransactionId: %s, ChunkId: %s",
        ~transactionId.ToString(),
        ~chunkId.ToString());

    ValidateTransactionId(transactionId);
    ValidateChunkId(chunkId, transactionId);

    auto& chunk = GetChunkForUpdate(chunkId);

    // TODO: sort w.r.t. proximity
    FOREACH(auto holderId, chunk.Locations) {
        const THolder& holder = GetHolder(holderId);
        response->AddHolderAddresses(holder.Address);
    }

    context->SetResponseInfo("HolderCount: %d",
        static_cast<int>(response->HolderAddressesSize()));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
