#include "chunk_manager.h"
#include "chunk_manager.pb.h"

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
        RegisterMethod(this, &TState::ProcessHeartbeat);

        transactionManager->RegisterHander(this);
    }


    //yvector<TChunkGroupId> GetChunkGroupIds()
    //{
    //    yvector<TChunkGroupId> result;
    //    for (TChunkGroupMap::iterator it = ChunkGroupMap.begin();
    //        it != ChunkGroupMap.end();
    //        ++it)
    //    {
    //        result.push_back(it->First());
    //    }
    //    return result;
    //}
    //
    //yvector<TChunkId> GetChunkGroup(TChunkGroupId id)
    //{
    //    yvector<TChunkId> result;
    //    
    //    TChunkGroupMap::iterator groupIt = ChunkGroupMap.find(id);
    //    if (groupIt == ChunkGroupMap.end())
    //        return result;

    //    const TChunkGroup& group = groupIt->Second();
    //    for (TChunkGroup::const_iterator chunkIt = group.begin();
    //         chunkIt != group.end();
    //         ++chunkIt)
    //    {
    //        result.push_back(*chunkIt);
    //    }

    //    return result;
    //}

    TChunkId AddChunk(const NProto::TMsgAddChunk& message)
    {
        TChunkId chunkId = TChunkId::FromProto(message.GetChunkId());
        TTransactionId transactionId = TTransactionId::FromProto(message.GetTransactionId());
        
        TChunk chunk(chunkId, transactionId);

        TTransaction& transaction = TransactionManager->GetTransactionForUpdate(transactionId);
        transaction.AddedChunks.push_back(chunkId);

        ChunkMap.Insert(chunkId, chunk);

        //TChunkGroup& group = ChunkGroupMap[chunk.GetGroupId()];
        //group.insert(chunkId);

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
    METAMAP_ACCESSORS_DECL(Holder, THolder, int);
    METAMAP_ACCESSORS_DECL(JobList, TJobList, TChunkId);
    METAMAP_ACCESSORS_DECL(Job, TJob, TJobId);

    int RegisterHolder(const NProto::TReqRegisterHolder& message)
    {
        Stroka address = message.GetAddress();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());
    
        int id = CurrentHolderId++;
    
        const THolder* existingHolder = FindHolder(address);
        if (existingHolder != NULL) {
            LOG_INFO("Holder kicked off due to address conflict (HolderId: %d)",
                existingHolder->Id);
            DoUnregisterHolder(*existingHolder);
        }

        THolder newHolder(id, address, statistics);

        if (IsLeader()) {
            CreateLease(newHolder);
            ChunkPlacement->RegisterHolder(newHolder);
        }

        ChunkReplication->RegisterHolder(newHolder);

        YVERIFY(HolderMap.Insert(id, newHolder));
        YVERIFY(HolderAddressMap.insert(MakePair(address, id)).Second());

        LOG_INFO("Holder registered (HolderId: %d, Address: %s, %s)",
            id,
            ~address,
            ~statistics.ToString());

        return id;
    }

    TVoid UnregisterHolder(const NProto::TMsgUnregisterHolder& message)
    { 
        int id = message.GetHolderId();

        const THolder& holder = GetHolder(id);

        DoUnregisterHolder(holder);

        return TVoid();
    }

    TVoid ProcessHeartbeat(const NProto::TMsgProcessHeartbeat& message)
    {
        int holderId = message.GetHolderId();
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

        for (int i = 0; i < static_cast<int>(message.StartedJobsSize()); ++i) {
            ProcessStartedJob(holder, message.GetStartedJobs(i));
        }

        for (int i = 0; i < static_cast<int>(message.UpdatedJobsSize()); ++i) {
            ProcessUpdatedJob(holder, message.GetUpdatedJobs(i));
        }

        for (int i = 0; i < static_cast<int>(message.StoppedJobsSize()); ++i) {
            ProcessStoppedJob(holder, TJobId::FromProto(message.GetStoppedJobs(i)));
        }

        LOG_DEBUG("Holder updated (HolderId: %d, %s)",
            holderId,
            ~statistics.ToString());

        return TVoid();
    }


private:
    typedef TMetaStateMap<TChunkId, TChunk, TChunkIdHash> TChunkMap;
    
    typedef TMetaStateMap<int, THolder> THolderMap;
    
    typedef yhash_map<Stroka, int> THolderAddressMap;

    typedef TMetaStateMap<TChunkId, TJobList, TChunkIdHash> TJobListMap;

    typedef TMetaStateMap<TJobId, TJob, TJobIdHash> TJobMap;
    
    //typedef yhash_set<TChunkId, TChunkIdHash> TChunkGroup;
    //typedef yhash_map<TChunkGroupId, TChunkGroup> TChunkGroupMap;

    TConfig Config;
    TTransactionManager::TPtr TransactionManager;
    TChunkReplication::TPtr ChunkReplication;
    TChunkPlacement::TPtr ChunkPlacement;
    TLeaseManager::TPtr HolderLeaseManager;
    int CurrentHolderId;
    TChunkMap ChunkMap;
    //TChunkGroupMap ChunkGroupMap;
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
        // TODO: reconstruct ChunkGroupMap
        // TODO: reconstruct HolderAddressMap
        // TODO: reconstruct JobMap
        // TODO: reconstruct overreplicated and underreplicated chunk sets
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
        //ChunkReplication->StartBackground(GetEpochStateInvoker());
    }

    virtual void OnStopLeading()
    {
        UnregisterAllHolders();
        //ChunkReplication->StopBackground();
    }

    virtual void OnStartFollowing()
    {
        //ChunkReplication->StartBackground(GetEpochStateInvoker());
    }

    virtual void OnStopFollowing()
    {
        //ChunkReplication->StopBackground();
    }


    void CreateLease(THolder& holder)
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

    void RenewLease(THolder& holder)
    {
        YASSERT(IsLeader());
        HolderLeaseManager->RenewLease(holder.Lease);
    }

    void CloseLease(THolder& holder)
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
            THolder& holder = it->Second();
            CreateLease(holder);
            ChunkPlacement->RegisterHolder(holder);
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
            CloseLease(holder);
        }

        LOG_INFO("Closed all holder leases");
    }

    void OnHolderExpired(const THolder& holder)
    {
        int holderId = holder.Id;
        
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
        int id = holder.Id;

        if (IsLeader()) {
            ChunkPlacement->UnregisterHolder(holder);
        }

        ChunkReplication->UnregisterHolder(holder);

        YVERIFY(HolderAddressMap.erase(holder.Address) == 1);
        YVERIFY(HolderMap.Remove(id));

        LOG_INFO("Holder unregistered (HolderId: %d)", id);
    }

    void DoRemoveChunk(const TChunk& chunk)
    {
        TChunkId chunkId = chunk.Id;
        //TChunkGroupId groupId = chunk.GetGroupId();
        
        YVERIFY(ChunkMap.Remove(chunkId));
        
        //TChunkGroup& group = ChunkGroupMap[groupId];
        //YVERIFY(group.erase(chunkId) == 1);
        //if (group.empty()) {
        //    YVERIFY(ChunkGroupMap.erase(groupId) == 1);
        //}

        LOG_INFO("Chunk removed (ChunkId: %s)",
            ~chunkId.ToString());
    }


    void ProcessAddedChunk(
        THolder& holder,
        const NProto::TChunkInfo& chunkInfo)
    {
        int holderId = holder.Id;
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

        if (chunk->Size != size && chunk->Size != TChunk::UnknownSize) {
            LOG_ERROR("Chunk size mismatch (ChunkId: %s, OldSize: %" PRId64 ", NewSize: %" PRId64 ")",
                ~chunkId.ToString(),
                chunk->Size,
                size);
        }

        ChunkReplication->RegisterReplica(*chunk, holder);

        LOG_INFO("Chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
            holderId,
            ~chunkId.ToString(),
            size);
    }

    void ProcessRemovedChunk(
        THolder& holder,
        const TChunkId& chunkId)
    {
        int holderId = holder.Id;

        TChunk* chunk = FindChunkForUpdate(chunkId);
        if (chunk == NULL) {
            LOG_ERROR("Unknown chunk removed at holder (HolderId: %d, ChunkId: %s)",
                holderId,
                ~chunkId.ToString());
            return;
        }

        chunk->RemoveLocation(holderId);

        ChunkReplication->UnregisterReplica(*chunk, holder);

        LOG_DEBUG("Chunk removed at holder (HolderId: %d, ChunkId: %s)",
             holderId,
             ~chunkId.ToString());
    }


    void ProcessStartedJob(const THolder& holder, const NProto::TJobStartInfo& jobInfo)
    {
        TChunkId chunkId = TChunkId::FromProto(jobInfo.GetChunkId());
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());

        TJob job(
            EJobType(jobInfo.GetType()),
            jobId,
            chunkId,
            holder.Address,
            FromProto<Stroka>(jobInfo.GetTargetAddresses()),
            EJobState::Running);
        YVERIFY(JobMap.Insert(jobId, job));

        TJobList& list = GetOrCreateJobListForUpdate(chunkId);
        list.AddJob(jobId);

        // TODO: logging
    }

    void ProcessUpdatedJob(const THolder& holder, const NProto::TJobInfo& jobInfo)
    {
        UNUSED(holder);

        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        TJob& job = GetJobForUpdate(jobId);
        job.State = EJobState(jobInfo.GetState());

        // TODO: logging
    }

    void ProcessStoppedJob(const THolder& holder, const TJobId& jobId)
    {
        UNUSED(holder);

        const TJob& job = GetJob(jobId); 
        TJobList& list = GetJobListForUpdate(job.ChunkId);
        list.RemoveJob(jobId);
        MaybeDropJobList(list);

        // TODO: logging
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
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, Holder, THolder, int, HolderMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, JobList, TJobList, TChunkId, JobListMap)
METAMAP_ACCESSORS_IMPL(TChunkManager::TState, Job, TJob, TJobId, JobMap)

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    TMetaStateManager::TPtr metaStateManager,
    TCompositeMetaState::TPtr metaState,
    IInvoker::TPtr serviceInvoker,
    NRpc::TServer::TPtr server,
    TTransactionManager::TPtr transactionManager)
    : TMetaStateServiceBase(
        serviceInvoker,
        TChunkManagerProxy::GetServiceName(),
        ChunkManagerLogger.GetCategory())
    , Config(config)
    , TransactionManager(transactionManager)
    //, ChunkRefresh(New<TChunkRefresh>(
    //    config,
    //    this))
    , ChunkPlacement(New<TChunkPlacement>())
    , ChunkReplication(New<TChunkReplication>(this))
    , State(New<TState>(
        config,
        metaStateManager,
        metaState,
        transactionManager,
//        ChunkRefresh,
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

//yvector<TChunkGroupId> TChunkManager::GetChunkGroupIds()
//{
//    return State->GetChunkGroupIds();
//}
//
//yvector<TChunkId> TChunkManager::GetChunkGroup(TChunkGroupId id)
//{
//    return State->GetChunkGroup(id);
//}
//
void TChunkManager::ValidateHolderId(int holderId)
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
METAMAP_ACCESSORS_FWD(TChunkManager, Holder, THolder, int, *State)
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
    int id,
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

    int holderId = request->GetHolderId();

    context->SetRequestInfo("HolderId: %d", holderId);

    ValidateHolderId(holderId);

    const THolder& holder = GetHolder(holderId);

    NProto::TMsgProcessHeartbeat message;
    message.SetHolderId(holderId);
    *message.MutableStatistics() = request->GetStatistics();
    message.MutableAddedChunks()->MergeFrom(request->GetAddedChunks());
    message.MutableAddedChunks()->MergeFrom(request->GetAddedChunks());

    yvector<NProto::TJobStartInfo> jobsToStart;
    yvector<TJobId> jobsToStop;
    ChunkReplication->GetJobControl(
        holder,
        jobsToStart,
        jobsToStop);

    ToProto(*context->Response().MutableJobsToStart(), jobsToStart);
    ToProto(*context->Response().MutableJobsToStop(), jobsToStop);

    CommitChange(
        this, context, State, message,
        &TState::ProcessHeartbeat);
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    TTransactionId transactionId = TGuid::FromProto(request->GetTransactionId());
    int replicaCount = request->GetReplicaCount();

    context->SetRequestInfo("TransactionId: %s, ReplicaCount: %d",
        ~transactionId.ToString(),
        replicaCount);

    yvector<int> holderIds = ChunkPlacement->GetNewChunkPlacement(replicaCount);
    for (yvector<int>::iterator it = holderIds.begin();
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
