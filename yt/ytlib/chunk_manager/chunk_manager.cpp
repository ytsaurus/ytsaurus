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
    TState(
        TMasterStateManager::TPtr metaStateManager,
        TCompositeMetaState::TPtr metaState,
        TTransactionManager::TPtr transactionManager)
        : TMetaStatePart(metaStateManager, metaState)
        , TransactionManager(transactionManager)
    {
        RegisterMethod(this, &TState::AddChunk);
        RegisterMethod(this, &TState::RemoveChunk);

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

        Chunks.Insert(chunkId, chunk);

        LOG_INFO("Chunk added (ChunkId: %s)",
            ~chunkId.ToString());

        return chunk;
    }

    TVoid RemoveChunk(const NProto::TMsgRemoveChunk& message)
    {
        TChunkId id = TGuid::FromProto(message.GetChunkId());
        
        YVERIFY(Chunks.Remove(id));
        
        LOG_INFO("Chunk removed (ChunkId: %s)",
            ~id.ToString());
        return TVoid();
    }

    TChunk::TPtr FindChunk(const TChunkId& id, bool forUpdate = false)
    {
        return Chunks.Find(id, forUpdate);
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

private:
    TTransactionManager::TPtr TransactionManager;

    typedef TMetaStateMap<TChunkId, TChunk, TChunkIdHash> TChunkMap;
    TChunkMap Chunks;

    // TMetaStatePart overrides.
    virtual Stroka GetPartName() const
    {
        return "ChunkManager";
    }

    virtual TAsyncResult<TVoid>::TPtr Save(TOutputStream& stream)
    {
        return Chunks.Save(GetSnapshotInvoker(), stream);
    }

    virtual TAsyncResult<TVoid>::TPtr Load(TInputStream& stream)
    {
        return Chunks.Load(GetSnapshotInvoker(), stream);
    }

    virtual void Clear()
    {
        Chunks.Clear();
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
            TChunk::TPtr chunk = Chunks.Find(*it, true);
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
            TChunk::TPtr chunk = Chunks.Find(*it);
            YASSERT(~chunk != NULL);

            YVERIFY(Chunks.Remove(chunk->GetId()));

            LOG_DEBUG("Chunk aborted (ChunkId: %s)",
                ~chunk->GetId().ToString());
        }
        // TODO: handle removed chunks
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    TMasterStateManager::TPtr metaStateManager,
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
        metaStateManager,
        metaState,
        transactionManager))
    , HolderTracker(new THolderTracker(
        Config,
        ServiceInvoker))
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

void TChunkManager::UpdateChunk(TChunk::TPtr chunk)
{
    CleanupChunkLocations(chunk);
}

void TChunkManager::CleanupChunkLocations(TChunk::TPtr chunk)
{
    TChunk::TLocations& locations = chunk->Locations();
    TChunk::TLocations::iterator reader = locations.begin();
    TChunk::TLocations::iterator writer = locations.begin();
    while (reader != locations.end()) {
        int holderId = *reader;
        if (HolderTracker->IsHolderAlive(holderId)) {
            *writer++ = holderId;
        }
        ++reader;
    } 
    locations.erase(writer, locations.end());
}

void TChunkManager::AddChunkLocation(TChunk::TPtr chunk, THolder::TPtr holder)
{
    int holderId = holder->GetId();
    TChunk::TLocations& locations = chunk->Locations();
    TChunk::TLocations::iterator it = Find(locations.begin(), locations.end(), holderId);
    if (it == locations.end()) {
        locations.push_back(holderId);
    }
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkManager, RegisterHolder)
{
    Stroka address = request->GetAddress();
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());
    
    context->SetRequestInfo(statistics.ToString());

    THolder::TPtr holder = HolderTracker->RegisterHolder(statistics, address);

    response->SetHolderId(holder->GetId());

    context->SetResponseInfo("HolderId: %d", holder->GetId());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, HolderHeartbeat)
{
    UNUSED(response);

    int holderId = request->GetHolderId();
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());

    context->SetRequestInfo("HolderId: %d, %s, AddedChunkCount: %d, RemovedChunkCount: %d",
        holderId,
        ~statistics.ToString(),
        request->AddedChunksSize(),
        request->RemovedChunksSize());

    THolder::TPtr holder = HolderTracker->GetHolder(holderId);
    holder->SetStatistics(statistics);
    HolderTracker->UpdateHolderPreference(holder);

    for (int i = 0; i < static_cast<int>(request->AddedChunksSize()); ++i) {
        const NProto::TChunkInfo& info = request->GetAddedChunks(i);
        TChunkId chunkId = TGuid::FromProto(info.GetId());
        i64 size = info.GetSize();

        TChunk::TPtr chunk = State->FindChunk(chunkId);
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

        AddChunkLocation(chunk, holder);

        LOG_INFO("Chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ")",
            holderId,
            ~chunkId.ToString(),
            size);
    }

    for (int i = 0; i < static_cast<int>(request->RemovedChunksSize()); ++i) {
        TChunkId chunkId = TGuid::FromProto(request->GetRemovedChunks(i));

        // TODO: code here

        LOG_DEBUG("Chunk removed at holder (HolderId: %d, ChunkId: %s)",
            holderId,
            ~chunkId.ToString());
    }

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    TTransactionId transactionId = TGuid::FromProto(request->GetTransactionId());
    int replicationFactor = request->GetReplicationFactor();

    context->SetRequestInfo("TransactionId: %s, ReplicationFactor: %d",
        ~transactionId.ToString(),
        replicationFactor);

    THolderTracker::THolders holders = HolderTracker->GetTargetHolders(replicationFactor);
    for (THolderTracker::THolders::iterator it = holders.begin();
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
    TChunk::TPtr chunk = State->GetChunk(chunkId, transaction);

    UpdateChunk(chunk);

    // TODO: sort w.r.t. proximity
    TChunk::TLocations& locations = chunk->Locations();
    for (TChunk::TLocations::iterator it = locations.begin();
         it != locations.end();
         ++it) 
    {
        int holderId = *it;
        THolder::TPtr holder = HolderTracker->FindHolder(holderId);
        YASSERT(~holder != NULL);
        response->AddHolderAddresses(holder->GetAddress());
    }

    context->SetResponseInfo("HolderCount: %d", locations.ysize());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
