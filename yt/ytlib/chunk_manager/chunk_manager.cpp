#include "chunk_manager.h"
#include "chunk_manager.pb.h"
#include "chunk_placement.h"
#include "chunk_replication.h"

#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../misc/assert.h"

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
        TChunkPlacement::TPtr chunkPlacement)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , TransactionManager(transactionManager)
        , ChunkReplication(chunkReplication)
        , ChunkPlacement(chunkPlacement)
        , HolderLeaseManager(New<TLeaseManager>())
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
        TChunkId chunkId = TChunkId::FromProto(message.GetChunkId());
        TTransactionId transactionId = TTransactionId::FromProto(message.GetTransactionId());
        
        TChunk chunk(chunkId, transactionId);

        TTransaction& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.AddedChunks.push_back(chunkId);

        ChunkMap.Insert(chunkId, chunk);

        LOG_INFO("Chunk added (ChunkId: %s)",
            ~chunkId.ToString());

        return chunkId;
    }

    TVoid RemoveChunk(const NProto::TMsgRemoveChunk& message)
    {
        TChunkId chunkId = TGuid::FromProto(message.GetChunkId());
        
        const TChunk& chunk = GetChunk(chunkId);
        DoRemoveChunk(chunk);

        return TVoid();
    }

    const THolder* FindHolder(Stroka address)
    {
        THolderAddressMap::iterator it = HolderAddressMap.find(address);
        return it == HolderAddressMap.end() ? NULL : FindHolder(it->Second());
    }

    METAMAP_ACCESSORS_DECL(Chunk, TChunk, TChunkId);
    METAMAP_ACCESSORS_DECL(Holder, THolder, THolderId);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

    THolderId RegisterHolder(const NProto::TReqRegisterHolder& message)
    {
        Stroka address = message.GetAddress();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());
    
        THolderId holderId = CurrentHolderId++;
    
        const THolder* existingHolder = FindHolder(address);
        if (existingHolder != NULL) {
            LOG_INFO("Holder kicked off due to address conflict (HolderId: %d)",
                existingHolder->Id);
            DoUnregisterHolder(*existingHolder);
        }

        THolder newHolder(holderId, address, statistics);

        if (IsLeader()) {
            CreateLease(newHolder);
            ChunkPlacement->RegisterHolder(newHolder);
            ChunkReplication->RegisterHolder(newHolder);
        }

        YVERIFY(HolderMap.Insert(holderId, newHolder));
        YVERIFY(HolderAddressMap.insert(MakePair(address, holderId)).Second());

        LOG_INFO("Holder registered (HolderId: %d, Address: %s, %s)",
            holderId,
            ~address,
            ~statistics.ToString());

        return holderId;
    }

    TVoid UnregisterHolder(const NProto::TMsgUnregisterHolder& message)
    { 
        THolderId holderId = message.GetHolderId();

        const THolder& holder = GetHolder(holderId);

        DoUnregisterHolder(holder);

        return TVoid();
    }

    TVoid HeartbeatRequest(const NProto::TMsgHeartbeatRequest& message)
    {
        THolderId holderId = message.GetHolderId();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());

        THolder& holder = GetHolderForUpdate(holderId);
        holder.Statistics = statistics;

        if (IsLeader()) {
            RenewLease(holder);
            ChunkPlacement->UpdateHolder(holder);
        }

        for (int i = 0; i < static_cast<int>(message.AddedChunksSize()); ++i) {
            ProcessAddedChunk(holder, message.GetAddedChunks(i));
        }

        for (int i = 0; i < static_cast<int>(message.RemovedChunksSize()); ++i) {
            ProcessRemovedChunk(holder, TChunkId::FromProto(message.GetRemovedChunks(i)));
        }

        LOG_DEBUG("Heartbeat request (HolderId: %d, %s, ChunksAdded: %d, ChunksRemoved: %d)",
            holderId,
            ~statistics.ToString(),
            static_cast<int>(message.AddedChunksSize()),
            static_cast<int>(message.RemovedChunksSize()));

        return TVoid();
    }

    TVoid HeartbeatResponse(const NProto::TMsgHeartbeatResponse& message)
    {
        THolderId holderId = message.GetHolderId();
        THolder& holder = GetHolderForUpdate(holderId);

        for (int i = 0; i < static_cast<int>(message.StartedJobsSize()); ++i) {
            DoAddJob(holder, message.GetStartedJobs(i));
        }

        for (int i = 0; i < static_cast<int>(message.StoppedJobsSize()); ++i) {
            const TJob& job = GetJob(TJobId::FromProto(message.GetStoppedJobs(i)));
            DoRemoveJob(holder, job);
        }

        LOG_DEBUG("Heartbeat response (HolderId: %d, JobsStarted: %d, JobsStopped: %d)",
            holderId,
            static_cast<int>(message.StartedJobsSize()),
            static_cast<int>(message.StoppedJobsSize()));

        return TVoid();
    }

private:
    typedef TMetaStateMap<TChunkId, TChunk> TChunkMap;
    
    typedef TMetaStateMap<THolderId, THolder> THolderMap;
    
    typedef yhash_map<Stroka, THolderId> THolderAddressMap;

    typedef TMetaStateMap<TChunkId, TJobList> TJobListMap;

    typedef TMetaStateMap<TJobId, TJob> TJobMap;
    
    TConfig Config;
    TTransactionManager::TPtr TransactionManager;
    TChunkReplication::TPtr ChunkReplication;
    TChunkPlacement::TPtr ChunkPlacement;
    TLeaseManager::TPtr HolderLeaseManager;
    THolderId CurrentHolderId;
    TChunkMap ChunkMap;
    THolderMap HolderMap;
    THolderAddressMap HolderAddressMap;
    TJobListMap JobListMap;
    TJobMap JobMap;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "ChunkManager";
    }

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream* stream)
    {
        IInvoker::TPtr invoker = GetSnapshotInvoker();
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
        IInvoker::TPtr invoker = GetSnapshotInvoker();
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
        // TODO: reconstruct HolderAddressMap
        return TVoid();
    }

    virtual void Clear()
    {
        if (IsLeader()) {
            UnregisterAllHolders();
        }
        HolderMap.Clear();
        HolderAddressMap.clear();
        ChunkMap.Clear();
        JobListMap.Clear();
        JobMap.Clear();
    }

    virtual void OnStartLeading()
    {
        RegisterAllHolders();
        ChunkReplication->StartRefresh(GetEpochStateInvoker());
    }

    virtual void OnStopLeading()
    {
        UnregisterAllHolders();
        ChunkReplication->StopRefresh();
    }


    void CreateLease(const THolder& holder)
    {
        YASSERT(IsLeader());
        YASSERT(holder.Lease == TLeaseManager::TLease());
        holder.Lease = HolderLeaseManager->CreateLease(
            Config.HolderLeaseTimeout,
            FromMethod(
                &TState::OnHolderExpired,
                TPtr(this),
                holder)
            ->Via(GetEpochStateInvoker()));
    }

    void RenewLease(const THolder& holder)
    {
        YASSERT(IsLeader());
        HolderLeaseManager->RenewLease(holder.Lease);
    }

    void CloseLease(const THolder& holder)
    {
        YASSERT(holder.Lease != TLeaseManager::TLease());
        HolderLeaseManager->CloseLease(holder.Lease);
        holder.Lease.Drop();
    }

    void RegisterAllHolders()
    {
        for (THolderMap::TIterator it = HolderMap.Begin();
             it != HolderMap.End();
             ++it)
        {
            const THolder& holder = it->Second();
            CreateLease(holder);
            ChunkPlacement->RegisterHolder(holder);
            ChunkReplication->RegisterHolder(holder);
        }

        LOG_INFO("Created fresh leases for all holders");
    }

    void UnregisterAllHolders()
    {
        for (THolderMap::TIterator it = HolderMap.Begin();
             it != HolderMap.End();
             ++it)
        {
            THolder& holder = it->Second();
            ChunkPlacement->UnregisterHolder(holder);
            ChunkReplication->UnregisterHolder(holder);
            CloseLease(holder);
        }

        LOG_INFO("Closed all holder leases");
    }

    void OnHolderExpired(const THolder& holder)
    {
        THolderId holderId = holder.Id;
        
        // Check if the holder is still registered.
        if (!HolderMap.Contains(holderId))
            return;

        LOG_INFO("Holder expired (HolderId: %d)", holderId);

        NProto::TMsgUnregisterHolder message;
        message.SetHolderId(holderId);
        CommitChange(message, FromMethod(&TState::UnregisterHolder, TPtr(this)));
    }


    // ITransactionHandler overrides.
    virtual void OnTransactionStarted(TTransaction& transaction)
    {
        UNUSED(transaction);
    }

    virtual void OnTransactionCommitted(TTransaction& transaction)
    {
        TTransaction::TChunks& addedChunks = transaction.AddedChunks;
        for (TTransaction::TChunks::iterator it = addedChunks.begin();
            it != addedChunks.end();
            ++it)
        {
            TChunk& chunk = GetChunkForUpdate(*it);
            chunk.TransactionId = TTransactionId();

            LOG_DEBUG("Chunk committed (ChunkId: %s)",
                ~chunk.Id.ToString());
        }

        // TODO: handle removed chunks
    }

    virtual void OnTransactionAborted(TTransaction& transaction)
    {
        TTransaction::TChunks& addedChunks = transaction.AddedChunks;
        for (TTransaction::TChunks::iterator it = addedChunks.begin();
            it != addedChunks.end();
            ++it)
        {
            const TChunk& chunk = GetChunk(*it);
            DoRemoveChunk(chunk);
        }

        // TODO: handle removed chunks
    }

    
    void DoUnregisterHolder(const THolder& holder)
    { 
        THolderId holderId = holder.Id;

        if (IsLeader()) {
            ChunkPlacement->UnregisterHolder(holder);
            ChunkReplication->UnregisterHolder(holder);
        }

        for (THolder::TChunkIds::const_iterator it = holder.Chunks.begin();
             it != holder.Chunks.end();
             ++it)
        {
            TChunk& chunk = GetChunkForUpdate(*it);
            DoRemovedChunkReplicaAtDeadHolder(holder, chunk);
        }

        for (THolder::TJobs::const_iterator it = holder.Jobs.begin();
             it != holder.Jobs.end();
             ++it)
        {
            const TJob& job = GetJob(*it);
            DoRemoveJobAtDeadHolder(holder, job);
        }

        YVERIFY(HolderAddressMap.erase(holder.Address) == 1);
        YVERIFY(HolderMap.Remove(holderId));

        LOG_INFO("Holder unregistered (HolderId: %d)", holderId);
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

        LOG_INFO("Chunk replica added (ChunkId: %s, HolderId: %d, Size: %" PRId64 ")",
            ~chunk.Id.ToString(),
            holder.Id,
            chunk.Size);

        if (IsLeader()) {
            ChunkReplication->RegisterReplica(holder, chunk);
        }
    }

    void DoRemoveChunkReplica(THolder& holder, TChunk& chunk)
    {
        YVERIFY(holder.Chunks.erase(chunk.Id) == 1);
        chunk.RemoveLocation(holder.Id);

        LOG_INFO("Chunk replica removed (ChunkId: %s, HolderId: %d)",
             ~chunk.Id.ToString(),
             holder.Id);

        if (IsLeader()) {
            ChunkReplication->UnregisterReplica(holder, chunk);
        }
    }

    void DoRemovedChunkReplicaAtDeadHolder(const THolder& holder, TChunk& chunk)
    {
        chunk.RemoveLocation(holder.Id);

        LOG_INFO("Chunk replica removed due to holder's death (ChunkId: %s, HolderId: %d)",
             ~chunk.Id.ToString(),
             holder.Id);

        if (IsLeader()) {
            ChunkReplication->UnregisterReplica(holder, chunk);
        }
    }

    void DoAddJob(THolder& holder, const NProto::TJobStartInfo& jobInfo)
    {
        TChunkId chunkId = TChunkId::FromProto(jobInfo.GetChunkId());
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        yvector<Stroka> targetAddresses = FromProto<Stroka>(jobInfo.GetTargetAddresses());
        EJobType jobType(jobInfo.GetType());

        TJob job(
            jobType,
            jobId,
            chunkId,
            holder.Address,
            targetAddresses);
        YVERIFY(JobMap.Insert(jobId, job));

        TJobList& list = GetOrCreateJobListForUpdate(chunkId);
        list.AddJob(jobId);

        holder.AddJob(jobId);

        LOG_INFO("Job added (HolderId: %d, JobId: %s, JobType: %s, ChunkId: %s)",
            holder.Id,
            ~jobId.ToString(),
            ~jobType.ToString(),
            ~chunkId.ToString());
    }

    void DoRemoveJob(THolder& holder, const TJob& job)
    {
        TJobId jobId = job.JobId;

        TJobList& list = GetJobListForUpdate(job.ChunkId);
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        holder.RemoveJob(jobId);

        LOG_INFO("Job removed (HolderId: %d, JobId: %s)",
            holder.Id,
            ~jobId.ToString());
    }

    void DoRemoveJobAtDeadHolder(const THolder& holder, const TJob& job)
    {
        TJobId jobId = job.JobId;

        TJobList& list = GetJobListForUpdate(job.ChunkId);
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        LOG_INFO("Job removed due to holder's death (HolderId: %d, JobId: %s)",
            holder.Id,
            ~jobId.ToString());
    }

    void ProcessAddedChunk(
        THolder& holder,
        const NProto::TChunkInfo& chunkInfo)
    {
        THolderId holderId = holder.Id;
        TChunkId chunkId = TGuid::FromProto(chunkInfo.GetId());
        i64 size = chunkInfo.GetSize();

        TChunk* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_ERROR("Unknown chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
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
        THolderId holderId = holder.Id;

        TChunk* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_DEBUG("Unknown chunk replica removed (ChunkId: %s, HolderId: %d)",
                 ~chunkId.ToString(),
                 holderId);
            return;
        }

        DoRemoveChunkReplica(holder, *chunk);
    }


    TJobList& GetOrCreateJobListForUpdate(const TChunkId& id)
    {
        TJobList* list = FindJobListForUpdate(id);
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
    , ChunkPlacement(New<TChunkPlacement>())
    , ChunkReplication(New<TChunkReplication>(
        this,
        ChunkPlacement))
    , State(New<TState>(
        config,
        metaStateManager,
        metaState,
        transactionManager,
        ChunkReplication,
        ChunkPlacement))
{
    RegisterMethods();
    metaState->RegisterPart(~State);
    server->RegisterService(this);
}

void TChunkManager::RegisterMethods()
{
    RPC_REGISTER_METHOD(TChunkManager, RegisterHolder);
    RPC_REGISTER_METHOD(TChunkManager, HolderHeartbeat);
    RPC_REGISTER_METHOD(TChunkManager, AddChunk);
    RPC_REGISTER_METHOD(TChunkManager, FindChunk);
}

void TChunkManager::ValidateHolderId(THolderId holderId)
{
    const THolder* holder = FindHolder(holderId);
    if (holder == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchHolder) <<
            Sprintf("invalid or expired holder %d", holderId);
    }
}

void TChunkManager::ValidateChunkId(
    const TChunkId& chunkId,
    const TTransactionId& transactionId)
{
    const TChunk* chunk = FindChunk(chunkId);
    if (chunk == NULL || !chunk->IsVisible(transactionId)) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) <<
            Sprintf("invalid chunk %s", ~chunkId.ToString());
    }
}

void TChunkManager::ValidateTransactionId(const TTransactionId& transactionId)
{
    const TTransaction* transaction = TransactionManager->FindTransaction(transactionId);
    if (transaction == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchChunk) << 
            Sprintf("invalid transaction %s", ~transactionId.ToString());
    }
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
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());
    
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
    TRspRegisterHolder* response = &context->Response();
    response->SetHolderId(id);
    context->SetResponseInfo("HolderId: %d", id);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, HolderHeartbeat)
{
    // TODO: do not commit if no changes reported
    UNUSED(response);

    THolderId holderId = request->GetHolderId();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    const THolder& holder = GetHolder(holderId);

    NProto::TMsgHeartbeatRequest requestMessage;
    requestMessage.SetHolderId(holderId);
    *requestMessage.MutableStatistics() = request->GetStatistics();

    for (int i = 0; i < static_cast<int>(request->AddedChunksSize()); ++i) {
        const NProto::TChunkInfo& chunkInfo = request->GetAddedChunks(i);
        TChunkId chunkId = TChunkId::FromProto(chunkInfo.GetId());
        if (holder.Chunks.find(chunkId) == holder.Chunks.end()) {
            *requestMessage.AddAddedChunks() = chunkInfo;
        } else {
            LOG_WARNING("Chunk replica is already added (ChunkId: %s, HolderId: %d)",
                ~chunkId.ToString(),
                holder.Id);
        }
    }

    for (int i = 0; i < static_cast<int>(request->RemovedChunksSize()); ++i) {
        TChunkId chunkId = TChunkId::FromProto(request->GetRemovedChunks(i));
        if (holder.Chunks.find(chunkId) != holder.Chunks.end()) {
            requestMessage.AddRemovedChunks(chunkId.ToProto());
        } else {
            LOG_WARNING("Chunk replica does not exist or already removed (ChunkId: %s, HolderId: %d)",
                ~chunkId.ToString(),
                holder.Id);
        }
    }

    CommitChange(
        State, requestMessage,
        &TState::HeartbeatRequest);

    yvector<NProto::TJobInfo> runningJobs;
    runningJobs.reserve(request->JobsSize());
    for (int i = 0; i < static_cast<int>(request->JobsSize()); ++i) {
        const NProto::TJobInfo& jobInfo = request->GetJobs(i);
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        const TJob* job = State->FindJob(jobId);
        if (job == NULL) {
            LOG_INFO("Stopping unknown or obsolete job (HolderId: %d, JobId: %s)",
                holderId,
                ~jobId.ToString());
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
    TRspHolderHeartbeat* response = &context->Response();

    context->SetResponseInfo("JobsToStart: %d, JobsToStop: %d",
        static_cast<int>(response->JobsToStartSize()),
        static_cast<int>(response->JobsToStopSize()));

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    TTransactionId transactionId = TGuid::FromProto(request->GetTransactionId());
    int replicaCount = request->GetReplicaCount();

    context->SetRequestInfo("TransactionId: %s, ReplicaCount: %d",
        ~transactionId.ToString(),
        replicaCount);

    yvector<THolderId> holderIds = ChunkPlacement->GetTargetHolders(replicaCount);
    for (yvector<THolderId>::iterator it = holderIds.begin();
        it != holderIds.end();
        ++it)
    {
        const THolder& holder = GetHolder(*it);
        response->AddHolderAddresses(holder.Address);
    }

    TChunkId chunkId = TChunkId::Create();

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
    TRspAddChunk* response = &context->Response();
    response->SetChunkId(id.ToProto());

    // TODO: probably log holder addresses
    context->SetResponseInfo("ChunkId: %s, HolderCount: %d",
        ~id.ToString(),
        static_cast<int>(response->HolderAddressesSize()));

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, FindChunk)
{
    TTransactionId transactionId = TGuid::FromProto(request->GetTransactionId());
    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());

    context->SetRequestInfo("TransactionId: %s, ChunkId: %s",
        ~transactionId.ToString(),
        ~chunkId.ToString());

    ValidateTransactionId(transactionId);
    ValidateChunkId(chunkId, transactionId);

    TChunk& chunk = GetChunkForUpdate(chunkId);

    //ChunkRefresh->RefreshChunk(chunk);

    // TODO: sort w.r.t. proximity
    TChunk::TLocations& locations = chunk.Locations;
    for (TChunk::TLocations::iterator it = locations.begin();
         it != locations.end();
         ++it) 
    {
        const THolder& holder = GetHolder(*it);
        response->AddHolderAddresses(holder.Address);
    }

    context->SetResponseInfo("HolderCount: %d", locations.ysize());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
