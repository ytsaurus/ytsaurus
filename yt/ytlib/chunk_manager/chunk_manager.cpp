#include "chunk_manager.h"
#include "chunk_manager.pb.h"

#include "../master/map.h"

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
        TTransactionManager::TPtr transactionManager)
        : TMetaStatePart(metaStateManager, metaState)
        , Config(config)
        , TransactionManager(transactionManager)
        , HolderLeaseManager(new TLeaseManager())
        , CurrentHolderId(0)
    {
        RegisterMethod(this, &TState::AddChunk);
        RegisterMethod(this, &TState::RemoveChunk);
        RegisterMethod(this, &TState::RegisterHolder);
        RegisterMethod(this, &TState::HolderHeartbeat);
        RegisterMethod(this, &TState::UnregisterHolder);

        transactionManager->RegisterHander(this);
    }


    TChunk::TPtr AddChunk(const NProto::TMsgAddChunk& message)
    {
        TChunkId chunkId = TChunkId::FromProto(message.GetChunkId());
        TTransactionId transactionId = TTransactionId::FromProto(message.GetTransactionId());
        
        TChunk::TPtr chunk = new TChunk(chunkId);
        chunk->TransactionId() = transactionId;

        TTransaction::TPtr transaction = TransactionManager->FindTransaction(transactionId, true);
        YASSERT(~transaction != NULL);
        transaction->AddedChunks().push_back(chunk->GetId());

        ChunkMap.Insert(chunkId, chunk);

        TChunkGroup& group = ChunkGroupMap[chunk->GetGroupId()];
        group.insert(chunkId);

        LOG_INFO("Chunk added (ChunkId: %s)",
            ~chunkId.ToString());

        return chunk;
    }

    TVoid RemoveChunk(const NProto::TMsgRemoveChunk& message)
    {
        TChunkId chunkId = TGuid::FromProto(message.GetChunkId());
        
        TChunk::TPtr chunk = GetChunk(chunkId, false);
        
        YVERIFY(ChunkMap.Remove(chunkId));
        
        TChunkGroup& group = ChunkGroupMap[chunk->GetGroupId()];
        YVERIFY(group.erase(chunkId) == 1);
        if (group.empty()) {
            YVERIFY(ChunkGroupMap.erase(chunk->GetGroupId()) == 1);
        }

        LOG_INFO("Chunk removed (ChunkId: %s)",
            ~chunkId.ToString());
        return TVoid();
    }

    TChunk::TPtr FindChunk(const TChunkId& id, bool forUpdate = false)
    {
        return ChunkMap.Find(id, forUpdate);
    }

    TChunk::TPtr GetChunk(
        const TChunkId& id,
        TTransaction::TPtr transaction,
        bool forUpdate = false)
    {
        TChunk::TPtr chunk = FindChunk(id, forUpdate);
        if (~chunk == NULL || !chunk->IsVisible(transaction->GetId())) {
            ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
                Sprintf("invalid chunk %s", ~id.ToString());
        }
        return chunk;
    }


    THolder::TPtr RegisterHolder(const NProto::TReqRegisterHolder& message)
    {
        Stroka address = message.GetAddress();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());
    
        int id = CurrentHolderId++;
    
        THolder::TPtr existingHolder = FindHolder(address);
        if (~existingHolder != NULL) {
            LOG_INFO("Holder kicked off due to address conflict (HolderId: %d)",
                existingHolder->GetId());
            DoUnregisterHolder(existingHolder);
        }

        THolder::TPtr newHolder = new THolder(id, address);
        newHolder->Statistics() = statistics;
        if (IsLeader()) {
            CreateLease(newHolder);
        }

        YVERIFY(HolderMap.Insert(id, newHolder));
        YVERIFY(HolderAddressMap.insert(MakePair(address, newHolder)).Second());

        UpdatePreference(newHolder);

        LOG_INFO("Holder registered (HolderId: %d, Address: %s, %s)",
            newHolder->GetId(),
            ~newHolder->GetAddress(),
            ~newHolder->Statistics().ToString());

        return newHolder;
    }

    TVoid UnregisterHolder(const NProto::TMsgUnregisterHolder& message)
    { 
        int id = message.GetHolderId();
        THolder::TPtr holder = GetHolder(id);
        DoUnregisterHolder(holder);
        return TVoid();
    }

    // TODO: remove
    TVoid HolderHeartbeat(const NProto::TReqHolderHeartbeat& message)
    {
        int holderId = message.GetHolderId();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());

        THolder::TPtr holder = HolderMap.Get(holderId, true);
        holder->Statistics() = statistics;

        if (IsLeader()) {
            RenewLease(holder);
        }

        UpdatePreference(holder);

        for (int i = 0; i < static_cast<int>(message.AddedChunksSize()); ++i) {
            const NProto::TChunkInfo& info = message.GetAddedChunks(i);
            TChunkId chunkId = TGuid::FromProto(info.GetId());
            i64 size = info.GetSize();

            TChunk::TPtr chunk = FindChunk(chunkId, true);
            if (~chunk == NULL) {
                LOG_ERROR("Unknown chunk reported by holder (HolderId: %d, ChunkId: %s Size: %" PRId64 ")",
                    holderId,
                    ~chunkId.ToString(),
                    size);
                continue;
            }

            if (chunk->Size() != size && chunk->Size() != TChunk::UnknownSize) {
                LOG_ERROR("Chunk size mismatch (ChunkId: %s, OldSize: %" PRId64 ", NewSize: %" PRId64 ")",
                    ~chunkId.ToString(),
                    chunk->Size(),
                    size);
            }

            chunk->AddLocation(holderId);

            LOG_INFO("Chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
                holderId,
                ~chunkId.ToString(),
                size);
        }

        for (int i = 0; i < static_cast<int>(message.RemovedChunksSize()); ++i) {
            TChunkId chunkId = TGuid::FromProto(message.GetRemovedChunks(i));

            // TODO: code here

            LOG_DEBUG("Chunk removed at holder (HolderId: %d, ChunkId: %s)",
                holderId,
                ~chunkId.ToString());
        }

        LOG_DEBUG("Holder updated (HolderId: %d, %s)",
            holderId,
            ~statistics.ToString());

        return TVoid();
    }

    TVoid ProcessHeartbeat(const NProto::TMsgProcessHeartbeat& message)
    {
        int holderId = message.GetHolderId();
        THolderStatistics statistics = THolderStatistics::FromProto(message.GetStatistics());

        THolder::TPtr holder = HolderMap.Get(holderId, true);
        holder->Statistics() = statistics;

        if (IsLeader()) {
            RenewLease(holder);
        }

        UpdatePreference(holder);

        for (int i = 0; i < static_cast<int>(message.AddedChunksSize()); ++i) {
            ProcessAddedChunk(holder, message.GetAddedChunks(i));
        }

        for (int i = 0; i < static_cast<int>(message.RemovedChunksSize()); ++i) {
            ProcessRemovedChunk(holder, TChunkId::FromProto(message.GetRemovedChunks(i)));
        }

        for (int i = 0; i < static_cast<int>(message.StartedJobsSize()); ++i) {
            ProcessStartedJob(message.GetStartedJobs(i));
        }

        for (int i = 0; i < static_cast<int>(message.UpdatedJobsSize()); ++i) {
            ProcessUpdatedJob(message.GetUpdatedJobs(i));
        }

        for (int i = 0; i < static_cast<int>(message.StoppedJobsSize()); ++i) {
            ProcessStoppedJob(TJobId::FromProto(message.GetStoppedJobs(i)));
        }

        LOG_DEBUG("Holder updated (HolderId: %d, %s)",
            holderId,
            ~statistics.ToString());

        return TVoid();
    }


    THolder::TPtr FindHolder(int id)    
    {
        return HolderMap.Find(id);
    }

    THolder::TPtr FindHolder(Stroka address)
    {
        THolderAddressMap::iterator it = HolderAddressMap.find(address);
        if (it == HolderAddressMap.end())
            return NULL;
        else
            return it->Second();
    }

    THolder::TPtr GetHolder(int id)
    {
        THolder::TPtr holder = FindHolder(id);
        if (~holder == NULL) {
            ythrow TServiceException(EErrorCode::NoSuchHolder) <<
                Sprintf("invalid or expired holder id %d", id);
        }
        return holder;
    }

    THolders GetTargetHolders(int count)
    {
        yvector<THolder::TPtr> result;
        THolder::TPreferenceMap::reverse_iterator it = PreferenceMap.rbegin();
        while (it != PreferenceMap.rend() && result.ysize() < count) {
            result.push_back((*it++).second);
        }
        return result;
    }


    TReplicationJob::TPtr FindJob(const TJobId& jobId)
    {
        TJobMap::iterator it = JobMap.find(jobId);
        if (it == JobMap.end())
            return NULL;
        else
            return it->Second();
    }

    TReplicationJob::TPtr GetJob(const TJobId& jobId)
    {
        TReplicationJob::TPtr job = FindJob(jobId);
        YASSERT(~job != NULL);
        return job;
    }


    void RefreshChunk(TChunk::TPtr chunk)
    {
        UpdateChunkLocations(chunk);
        UpdateChunkReplicaStatus(chunk);
    }   

    void RefreshChunkGroup(TChunkGroupId groupId)
    {
        TChunkGroupMap::iterator groupIt = ChunkGroupMap.find(groupId);
        if (groupIt == ChunkGroupMap.end())
            return;

        TChunkGroup& group = groupIt->Second();

        for (TChunkGroup::iterator chunkIt = group.begin();
             chunkIt != group.end();
             ++chunkIt)
        {
            TChunkId chunkId = *chunkIt;
            TChunk::TPtr chunk = FindChunk(chunkId);
            if (~chunk != NULL) {
                RefreshChunk(chunk);
            }
        }
    }

private:
    typedef TMetaStateRefMap<TChunkId, TChunk, TChunkIdHash> TChunkMap;
    
    typedef TMetaStateRefMap<int, THolder> THolderMap;
    
    typedef yhash_map<Stroka, THolder::TPtr> THolderAddressMap;
    
    typedef yhash_map<TJobId, TReplicationJob::TPtr, TJobIdHash> TJobMap;
    
    typedef yhash_set<TChunkId, TChunkIdHash> TChunkGroup;
    typedef yhash_map<TChunkGroupId, TChunkGroup> TChunkGroupMap;

    TConfig Config;
    TTransactionManager::TPtr TransactionManager;
    TLeaseManager::TPtr HolderLeaseManager;
    int CurrentHolderId;
    TChunkMap ChunkMap;
    TChunkGroupMap ChunkGroupMap;
    THolderMap HolderMap;
    THolderAddressMap HolderAddressMap;
    THolder::TPreferenceMap PreferenceMap;
    TJobMap JobMap;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "ChunkManager";
    }

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& stream)
    {
        IInvoker::TPtr invoker = GetSnapshotInvoker();
        //TODO: fix this under gcc
        //invoker->Invoke(FromMethod(&TState::DoSave, TPtr(this), stream));
        HolderMap.Save(invoker, stream);
        return ChunkMap.Save(invoker, stream);
    }

    //! Saves the local state (not including the maps).
    void DoSave(TOutputStream& stream)
    {
        stream << CurrentHolderId;
    }

    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& stream)
    {
        IInvoker::TPtr invoker = GetSnapshotInvoker();
        //TODO: fix this under gcc
        //invoker->Invoke(FromMethod(&TState::DoLoad, TPtr(this), stream));
        HolderMap.Load(invoker, stream);
        return ChunkMap.Load(invoker, stream)->Apply(FromMethod(
            &TState::OnLoaded,
            TPtr(this)));
    }

    //! Loads the local state (not including the maps).
    void DoLoad(TInputStream& stream)
    {
        stream >> CurrentHolderId;
    }

    TVoid OnLoaded(TVoid)
    {
        UpdateAllPreferences();
        // TODO: reconstruct ChunkGroupMap
        // TODO: reconstruct HolderAddressMap
        // TODO: reconstruct JobMap
        // TODO: reconstruct overreplicated and underreplicated chunk sets
        return TVoid();
    }

    virtual void Clear()
    {
        if (IsLeader()) {
            CloseAllLeases();
        }
        HolderMap.Clear();
        HolderAddressMap.clear();
        ChunkMap.Clear();
        PreferenceMap.clear();
        JobMap.clear();
    }

    virtual void OnStartLeading()
    {
        CreateAllLeases();
        RefreshChunks();
    }

    virtual void OnStopLeading()
    {
        CloseAllLeases();
    }


    void CreateLease(THolder::TPtr holder)
    {
        YASSERT(IsLeader());
        YASSERT(holder->Lease() == TLeaseManager::TLease());
        holder->Lease() = HolderLeaseManager->CreateLease(
            Config.HolderLeaseTimeout,
            FromMethod(
                &TState::OnHolderExpired,
                TPtr(this),
                holder)
            ->Via(GetEpochStateInvoker()));
    }

    void RenewLease(THolder::TPtr holder)
    {
        YASSERT(IsLeader());
        HolderLeaseManager->RenewLease(holder->Lease());
    }

    void CloseLease(THolder::TPtr holder)
    {
        YASSERT(holder->Lease() != TLeaseManager::TLease());
        HolderLeaseManager->CloseLease(holder->Lease());
        holder->Lease().Drop();
    }

    void CreateAllLeases()
    {
        for (THolderMap::TIterator it = HolderMap.Begin();
             it != HolderMap.End();
             ++it)
        {
            CreateLease(it->Second());
        }
        LOG_INFO("Created fresh leases for all holders");
    }

    void CloseAllLeases()
    {
        for (THolderMap::TIterator it = HolderMap.Begin();
             it != HolderMap.End();
             ++it)
        {
            CloseLease(it->Second());
        }
        LOG_INFO("Closed all holder leases");
    }

    void OnHolderExpired(THolder::TPtr holder)
    {
        int holderId = holder->GetId();
        
        // Check if the holder is still registered.
        if (!HolderMap.Contains(holderId))
            return;

        LOG_INFO("Holder expired (HolderId: %d)", holderId);

        NProto::TMsgUnregisterHolder message;
        message.SetHolderId(holderId);
        CommitChange(message, FromMethod(&TState::UnregisterHolder, TPtr(this)));
    }


    // ITransactionHandler overrides.
    void OnTransactionStarted(TTransaction::TPtr transaction)
    {
        UNUSED(transaction);
    }

    void OnTransactionCommitted(TTransaction::TPtr transaction)
    {
        TTransaction::TChunks& addedChunks = transaction->AddedChunks();
        for (TTransaction::TChunks::iterator it = addedChunks.begin();
            it != addedChunks.end();
            ++it)
        {
            TChunk::TPtr chunk = ChunkMap.Find(*it, true);
            YASSERT(~chunk != NULL);

            chunk->TransactionId() = TTransactionId();

            LOG_DEBUG("Chunk committed (ChunkId: %s)",
                ~chunk->GetId().ToString());
        }

        // TODO: handle removed chunks
    }

    void OnTransactionAborted(TTransaction::TPtr transaction)
    {
        TTransaction::TChunks& addedChunks = transaction->AddedChunks();
        for (TTransaction::TChunks::iterator it = addedChunks.begin();
            it != addedChunks.end();
            ++it)
        {
            TChunk::TPtr chunk = ChunkMap.Find(*it);
            YASSERT(~chunk != NULL);

            YVERIFY(ChunkMap.Remove(chunk->GetId()));

            LOG_DEBUG("Chunk aborted (ChunkId: %s)",
                ~chunk->GetId().ToString());
        }
        // TODO: handle removed chunks
    }

    
    void DoUnregisterHolder(THolder::TPtr holder)
    { 
        int id = holder->GetId();

        YVERIFY(HolderMap.Remove(id));
        YVERIFY(HolderAddressMap.erase(holder->GetAddress()) == 1);

        if (holder->PreferenceIterator() != THolder::TPreferenceMap::iterator()) {
            PreferenceMap.erase(holder->PreferenceIterator());
            holder->PreferenceIterator() = THolder::TPreferenceMap::iterator();
        }

        LOG_INFO("Holder unregistered (HolderId: %d)", id);
    }

    void UpdateAllPreferences()
    {
        for (THolderMap::TIterator it = HolderMap.Begin();
             it != HolderMap.End();
             ++it)
        {
            UpdatePreference(it->Second());
        }
    }

    void UpdatePreference(THolder::TPtr holder)
    {
        if (holder->PreferenceIterator() != THolder::TPreferenceMap::iterator()) {
            PreferenceMap.erase(holder->PreferenceIterator());
        }

        double preference = holder->GetPreference();
        holder->PreferenceIterator() = PreferenceMap.insert(MakePair(preference, holder));
    }


    void ProcessAddedChunk(
        THolder::TPtr holder,
        const NProto::TChunkInfo& chunkInfo)
    {
        int holderId = holder->GetId();
        TChunkId chunkId = TGuid::FromProto(chunkInfo.GetId());
        i64 size = chunkInfo.GetSize();

        TChunk::TPtr chunk = FindChunk(chunkId, true);
        if (~chunk == NULL) {
            LOG_ERROR("Unknown chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
                holderId,
                ~chunkId.ToString(),
                size);
            return;
        }

        if (chunk->Size() != size && chunk->Size() != TChunk::UnknownSize) {
            LOG_ERROR("Chunk size mismatch (ChunkId: %s, OldSize: %" PRId64 ", NewSize: %" PRId64 ")",
                ~chunkId.ToString(),
                chunk->Size(),
                size);
        }

        chunk->AddLocation(holderId);

        RefreshChunk(chunk);

        LOG_INFO("Chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
            holderId,
            ~chunkId.ToString(),
            size);
    }

    void ProcessRemovedChunk(
        THolder::TPtr holder,
        const TChunkId& chunkId)
    {
        int holderId = holder->GetId();

        TChunk::TPtr chunk = FindChunk(chunkId, true);
        if (~chunk == NULL) {
            LOG_ERROR("Unknown chunk removed at holder (HolderId: %d, ChunkId: %s)",
                holderId,
                ~chunkId.ToString());
            return;
        }

        chunk->RemoveLocation(holderId);
        RefreshChunk(chunk);

        LOG_DEBUG("Chunk removed at holder (HolderId: %d, ChunkId: %s)",
             holderId,
             ~chunkId.ToString());
    }


    void ProcessStartedJob(const NProto::TJobStartInfo& jobInfo)
    {
        TChunkId chunkId = TChunkId::FromProto(jobInfo.GetChunkId());
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());
        
        TChunk::TPtr chunk = ChunkMap.Get(chunkId, true);
        
        TReplicationJob::TPtr job = new TReplicationJob(
            jobId,
            chunkId,
            FromProto(jobInfo.GetTargetAddresses()),
            EJobState::Running);

        TChunkReplication* replication = chunk->GetReplication();
        replication->StartJob(job);

        YVERIFY(JobMap.insert(MakePair(jobId, job)).Second());

        // TODO: logging
    }

    void ProcessUpdatedJob(const NProto::TJobInfo& jobInfo)
    {
        TJobId jobId = TJobId::FromProto(jobInfo.GetJobId());

        TReplicationJob::TPtr job = GetJob(jobId);
        TChunkId chunkId = job->GetChunkId();
        TChunk::TPtr chunk = ChunkMap.Get(chunkId, true);

        job->State() = EJobState(jobInfo.GetState());

        // TODO: logging
    }

    void ProcessStoppedJob(const TJobId& jobId)
    {
        TReplicationJob::TPtr job = GetJob(jobId);
        TChunkId chunkId = job->GetChunkId();
        TChunk::TPtr chunk = ChunkMap.Get(chunkId, true);

        TChunkReplication* replication = chunk->GetReplication();
        YVERIFY(replication->StopJob(jobId));

        YVERIFY(JobMap.erase(jobId) == 1);

        chunk->TryTrimReplication();
        
        // TODO: logging
    }


    void RefreshChunks()
    {
        TRefresher::TPtr refresher = new TRefresher(this);
        refresher->Run();
    }

    class TRefresher
        : public TRefCountedBase
    {
    public:
        typedef TIntrusivePtr<TRefresher> TPtr;

        TRefresher(TState::TPtr state)
            : State(state)
            , CurrentIndex(0)
        { }

        void Run()
        {
            for (TChunkGroupMap::iterator it = State->ChunkGroupMap.begin();
                it != State->ChunkGroupMap.end();
                ++it)
            {
                GroupIds.push_back(it->First());
            }

            ScheduleNextRefresh();
        }

    private:
        TState::TPtr State;
        int CurrentIndex;
        yvector<TChunkGroupId> GroupIds;

        void ScheduleNextRefresh()
        {
            TDelayedInvoker::Get()->Submit(
                FromMethod(&TRefresher::RefreshGroup, TPtr(this))
                ->Via(State->GetEpochStateInvoker()),
                State->Config.ChunkGroupRefreshPeriod);
        }

        void RefreshGroup()
        {
            if (CurrentIndex >= GroupIds.ysize()) {
                State->RefreshChunks();
                return;
            }

            TChunkGroupId groupId = GroupIds[CurrentIndex++];
            State->RefreshChunkGroup(groupId);
            ScheduleNextRefresh();
        }
    };


    void UpdateChunkLocations(TChunk::TPtr chunk)
    {
        TChunk::TLocations& locations = chunk->Locations();
        TChunk::TLocations::iterator reader = locations.begin();
        TChunk::TLocations::iterator writer = locations.begin();
        while (reader != locations.end()) {
            int holderId = *reader;
            if (HolderMap.Contains(holderId)) {
                *writer++ = holderId;
            }
            ++reader;
        } 
        locations.erase(writer, locations.end());
    }

    void UpdateChunkReplicaStatus(TChunk::TPtr chunk)
    {
        TChunkId chunkId = chunk->GetId();
        int delta = chunk->GetReplicaDelta();

        const TChunk::TLocations& locations = chunk->Locations();
        for (TChunk::TLocations::const_iterator it = locations.begin();
             it != locations.end();
             ++it)
        {
            int holderId = *it;
            THolder::TPtr holder = GetHolder(holderId);
            if (delta < 0) {
                holder->OverreplicatedChunks().erase(chunkId);
                holder->UnderreplicatedChunks().insert(chunkId);
            } else if (delta > 0) {
                holder->OverreplicatedChunks().insert(chunkId);
                holder->UnderreplicatedChunks().erase(chunkId);
            } else {
                holder->OverreplicatedChunks().erase(chunkId);
                holder->UnderreplicatedChunks().erase(chunkId);
            }
        }
    }

    // To enable access from nested classes.
    IInvoker::TPtr GetEpochStateInvoker()
    {
        return TMetaStatePart::GetEpochStateInvoker();
    }
};

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
    , State(new TState(
        config,
        metaStateManager,
        metaState,
        transactionManager))
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

TTransaction::TPtr TChunkManager::GetTransaction(const TTransactionId& id, bool forUpdate)
{
    TTransaction::TPtr transaction = TransactionManager->FindTransaction(id, forUpdate);
    if (~transaction == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
            Sprintf("invalid or expired transaction %s", ~id.ToString());
    }
    return transaction;
}

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
    THolder::TPtr holder,
    TCtxRegisterHolder::TPtr context)
{
    TRspRegisterHolder* response = &context->Response();
    response->SetHolderId(holder->GetId());
    context->SetResponseInfo("HolderId: %d", holder->GetId());
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, HolderHeartbeat)
{
    // TODO: do not commit if no changes reported
    UNUSED(response);

    int holderId = request->GetHolderId();
    State->GetHolder(holderId);

    const NProto::TReqHolderHeartbeat& message = *request;
    CommitChange(
        this, context, State, message,
        &TState::HolderHeartbeat);
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    TTransactionId transactionId = TGuid::FromProto(request->GetTransactionId());
    int replicationFactor = request->GetReplicationFactor();

    context->SetRequestInfo("TransactionId: %s, ReplicationFactor: %d",
        ~transactionId.ToString(),
        replicationFactor);

    THolders holders = State->GetTargetHolders(replicationFactor);
    for (THolders::iterator it = holders.begin();
        it != holders.end();
        ++it)
    {
        THolder::TPtr holder = *it;
        response->AddHolderAddresses(holder->GetAddress());
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
    TChunk::TPtr chunk,
    TCtxAddChunk::TPtr context)
{
    TRspAddChunk* response = &context->Response();
    response->SetChunkId(chunk->GetId().ToProto());

    // TODO: probably log holder addresses
    context->SetResponseInfo("ChunkId: %s, HolderCount: %d",
        ~chunk->GetId().ToString(),
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

    TTransaction::TPtr transaction = GetTransaction(transactionId);
    
    TChunk::TPtr chunk = State->GetChunk(chunkId, transaction, true);
    State->RefreshChunk(chunk);

    // TODO: sort w.r.t. proximity
    TChunk::TLocations& locations = chunk->Locations();
    for (TChunk::TLocations::iterator it = locations.begin();
         it != locations.end();
         ++it) 
    {
        int holderId = *it;
        THolder::TPtr holder = State->FindHolder(holderId);
        YASSERT(~holder != NULL);
        response->AddHolderAddresses(holder->GetAddress());
    }

    context->SetResponseInfo("HolderCount: %d", locations.ysize());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
