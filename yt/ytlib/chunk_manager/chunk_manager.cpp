#include "chunk_manager.h"

#include "../misc/serialize.h"
#include "../misc/string.h"

namespace NYT {
namespace NChunkManager {

using namespace NTransaction;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TState
    : public TRefCountedBase
{
public:
    TChunk::TPtr AddChunk()
    {
        // Don't trust anyone!
        TChunkId id;
        do {
            CreateGuid(&id);
        } while (Chunks.find(id) != Chunks.end());

        TChunk::TPtr chunk = new TChunk(id);
        Chunks.insert(MakePair(id, chunk));

        LOG_INFO("Chunk added (ChunkId: %s)",
            ~StringFromGuid(id));

        return chunk;
    }

    TChunk::TPtr RegisterChunk(const TChunkId& id, i64 size)
    {
        TChunk::TPtr chunk = new TChunk(id, size);
        Chunks.insert(MakePair(id, chunk));

        LOG_INFO("Chunk registered (ChunkId: %s, Size: %" PRId64 ")",
            ~StringFromGuid(id),
            size);

        return chunk;
    }

    void RemoveChunk(TChunk::TPtr chunk)
    {
        // TODO: schedule removal on holders
        // TOOD: use YVERIFY
        VERIFY(Chunks.erase(chunk->GetId()) == 1, "oops");
    }

    TChunk::TPtr FindChunk(const TChunkId& id, bool forUpdate = false)
    {
        UNUSED(forUpdate);

        TChunkMap::iterator it = Chunks.find(id);
        if (it == Chunks.end())
            return NULL;
        else
            return it->Second();
    }

    TChunk::TPtr GetChunk(
        const TChunkId& id,
        NTransaction::TTransaction::TPtr transaction,
        bool forUpdate = false)
    {
        TChunk::TPtr chunk = FindChunk(id, forUpdate);
        if (~chunk == NULL || !chunk->IsVisible(transaction->GetId())) {
            ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
                Sprintf("invalid chunk %s", ~StringFromGuid(id));
        }
        return chunk;
    }

private:
    typedef yhash_map<TChunkId, TChunk::TPtr, TGUIDHash> TChunkMap;
    
    TChunkMap Chunks;

};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    NRpc::TServer::TPtr server,
    TTransactionManager::TPtr transactionManager)
    : TServiceBase(
        TChunkManagerProxy::GetServiceName(),
        ChunkManagerLogger.GetCategory())
    , Config(config)
    , TransactionManager(transactionManager)
    , ServiceInvoker(server->GetInvoker())
    , State(new TState())
    , HolderTracker(new THolderTracker(
        Config,
        ServiceInvoker))
{
    server->RegisterService(this);
    transactionManager->RegisterHander(this);
}

TTransaction::TPtr TChunkManager::GetTransaction(const TTransactionId& id, bool forUpdate)
{
    TTransaction::TPtr transaction = TransactionManager->FindTransaction(id, forUpdate);
    if (~transaction == NULL) {
        ythrow TServiceException(EErrorCode::NoSuchTransaction) <<
            Sprintf("invalid or expired transaction %s", ~StringFromGuid(id));
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

void TChunkManager::OnTransactionStarted(TTransaction::TPtr transaction)
{
    UNUSED(transaction);
}

void TChunkManager::OnTransactionCommitted(TTransaction::TPtr transaction)
{
    TTransaction::TChunks& addedChunks = transaction->AddedChunks();
    for (TTransaction::TChunks::iterator it = addedChunks.begin();
         it != addedChunks.end();
         ++it)
    {
        TChunk::TPtr chunk = State->FindChunk(*it, true);
        YASSERT(~chunk != NULL);

        chunk->SetTransactionId(TTransactionId());

        LOG_DEBUG("Chunk committed (ChunkId: %s)",
            ~StringFromGuid(chunk->GetId()));
    }

    // TODO: handle removed chunks
}

void TChunkManager::OnTransactionAborted(TTransaction::TPtr transaction)
{
    TTransaction::TChunks& addedChunks = transaction->AddedChunks();
    for (TTransaction::TChunks::iterator it = addedChunks.begin();
         it != addedChunks.end();
         ++it)
    {
        TChunk::TPtr chunk = State->FindChunk(*it);
        YASSERT(~chunk != NULL);

        State->RemoveChunk(chunk);

        LOG_DEBUG("Chunk aborted (ChunkId: %s)",
            ~StringFromGuid(chunk->GetId()));
    }

    // TODO: handle removed chunks
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
    // TODO: fixme
    UNUSED(response);

    int holderId = request->GetHolderId();
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());

    context->SetRequestInfo("HolderId: %d, %s, AddedChunkCount: %d, RemovedChunkCount: %d",
        holderId,
        ~statistics.ToString(),
        request->AddedChunksSize(),
        request->RemovedChunkSize());

    THolder::TPtr holder = GetHolder(holderId);
    holder->SetStatistics(statistics);
    
    HolderTracker->UpdateHolderPreference(holder);

    // TODO: refactor this once the state becomes persistent
    for (int i = 0; request->AddedChunksSize(); ++i) {
        const NProto::TChunkInfo& info = request->GetAddedChunks(i);
        TChunkId chunkId = GuidFromProtoGuid(info.GetId());
        i64 size = info.GetSize();

        bool firstSeen;
        TChunk::TPtr chunk = State->FindChunk(chunkId);
        if (~chunk == NULL) {
            firstSeen = true;
            chunk = State->RegisterChunk(chunkId, size);
        } else {
            firstSeen = false;
            if (chunk->GetSize() != size) {
                LOG_ERROR("Chunk size mismatch (ChunkId: %s, OldSize: %" PRId64 ", NewSize: %" PRId64 ")",
                    ~StringFromGuid(chunkId),
                    chunk->GetSize(),
                    size);
            }
        }

        AddChunkLocation(chunk, holder);

        LOG_DEBUG("Chunk added at holder (HolderId: %d, ChunkId: %s, Size: %" PRId64 ", FirstSeen: %d)",
            holderId,
            ~StringFromGuid(chunkId),
            size,
            static_cast<int>(firstSeen));
    }

    for (int i = 0; request->AddedChunksSize(); ++i) {
        TChunkId chunkId = GuidFromProtoGuid(request->GetRemovedChunk(i));

        // TODO: code here

        LOG_DEBUG("Chunk removed at holder (HolderId: %d, ChunkId: %s)",
            holderId,
            ~StringFromGuid(chunkId));
    }

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, AddChunk)
{
    TTransactionId transactionId = GuidFromProtoGuid(request->GetTransactionId());
    int replicationFactor = request->GetReplicationFactor();

    context->SetRequestInfo("TransactionId: %s, ReplicationFactor: %d",
        ~StringFromGuid(transactionId),
        replicationFactor);

    TTransaction::TPtr transaction = GetTransaction(transactionId, true);

    TChunk::TPtr chunk = State->AddChunk();
    chunk->SetTransactionId(transactionId);

    transaction->AddedChunks().push_back(chunk->GetId());

    response->SetChunkId(ProtoGuidFromGuid(chunk->GetId()));

    THolderTracker::THolders holders = HolderTracker->GetTargetHolders(replicationFactor);
    for (THolderTracker::THolders::iterator it = holders.begin();
         it != holders.end();
         ++it)
    {
        THolder::TPtr holder = *it;
        response->AddHolderAddresses(holder->GetAddress());
    }

    // TODO: probably log holder addresses
    context->SetResponseInfo("ChunkId: %s, HolderCount: %d",
        ~StringFromGuid(chunk->GetId()),
        holders.ysize());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, FindChunk)
{
    TTransactionId transactionId = GuidFromProtoGuid(request->GetTransactionId());
    TChunkId chunkId = GuidFromProtoGuid(request->GetChunkId());

    context->SetRequestInfo("TransactionId: %s, ChunkId: %s",
        ~StringFromGuid(transactionId),
        ~StringFromGuid(chunkId));

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
