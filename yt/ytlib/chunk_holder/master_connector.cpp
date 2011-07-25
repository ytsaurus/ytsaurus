#include "master_connector.h"

#include <util/system/hostname.h>

#include "../rpc/client.h"
#include "../master/cell_channel.h"

#include "../misc/delayed_invoker.h"
#include "../misc/serialize.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TMasterConnector::TMasterConnector(
    const TConfig& config,
    TChunkStore::TPtr chunkStore,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , ChunkStore(chunkStore)
    , ServiceInvoker(serviceInvoker)
    , Registered(false)
    , IncrementalHeartbeat(false)
    , HolderId(InvalidHolderId)
{ }

void TMasterConnector::Initialize()
{
    InitializeProxy();
    InitializeAddress();
    OnHeartbeat();

    LOG_INFO("Chunk holder address is %s, master addresses are %s",
        ~Address,
        ~Config.Masters.ToString());
}

void TMasterConnector::InitializeProxy()
{
    NRpc::IChannel::TPtr channel = new TCellChannel(Config.Masters);
    Proxy.Reset(new TProxy(~channel));
}

void TMasterConnector::InitializeAddress()
{
    Address = Sprintf("%s:%d", ~HostName(), Config.Port);
}

void TMasterConnector::ScheduleHeartbeat()
{
    TDelayedInvoker::Get()->Submit(
        FromMethod(&TMasterConnector::OnHeartbeat, TPtr(this))->Via(ServiceInvoker),
        Config.HeartbeatPeriod);
}

void TMasterConnector::OnHeartbeat()
{
    if (Registered) {
        SendHeartbeat();
    } else {
        SendRegister();
    }
}

void TMasterConnector::SendRegister()
{
    TProxy::TReqRegisterHolder::TPtr request = Proxy->RegisterHolder();
    
    THolderStatistics statistics = ChunkStore->GetStatistics();
    *request->MutableStatistics() = statistics.ToProto();

    request->SetAddress(Address);

    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(&TMasterConnector::OnRegisterResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_INFO("Register request sent (%s)",
        ~statistics.ToString());
}

void TMasterConnector::OnRegisterResponse(TProxy::TRspRegisterHolder::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        OnDisconnected();

        LOG_WARNING("Error registering at master (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());
        return;
    }

    HolderId = response->GetHolderId();
    Registered = true;
    IncrementalHeartbeat = false;

    LOG_INFO("Successfully registered at master (HolderId: %d)",
        HolderId);
}

void TMasterConnector::SendHeartbeat()
{
    TProxy::TReqHolderHeartbeat::TPtr request = Proxy->HolderHeartbeat();

    YASSERT(HolderId != InvalidHolderId);
    request->SetHolderId(HolderId);

    THolderStatistics statistics = ChunkStore->GetStatistics();
    *request->MutableStatistics() = statistics.ToProto();

    if (IncrementalHeartbeat) {
        ReportedAdded = AddedSinceLastSuccess;
        ReportedRemoved = RemovedSinceLastSuccess;

        for (TChunks::iterator it = ReportedAdded.begin();
            it != ReportedAdded.end();
            ++it)
        {
            *request->AddAddedChunks() = GetInfo(*it);
        }

        for (TChunks::iterator it = ReportedRemoved.begin();
            it != ReportedRemoved.end();
            ++it)
        {
            request->AddRemovedChunks((*it)->GetId().ToProto());
        }
    } else {
        TChunkStore::TChunks chunks = ChunkStore->GetChunks();
        for (TChunkStore::TChunks::iterator it = chunks.begin();
             it != chunks.end();
             ++it)
        {
            *request->AddAddedChunks() = GetInfo(*it);
        }
    }

    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(ServiceInvoker));

    LOG_DEBUG("Heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d)",
        ~statistics.ToString(),
        static_cast<int>(request->AddedChunksSize()),
        static_cast<int>(request->RemovedChunksSize()));
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();
    
    EErrorCode errorCode = response->GetErrorCode();
    if (errorCode != EErrorCode::OK) {
        LOG_WARNING("Error sending heartbeat to master (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());

        // Don't panic upon getting TransportError or Unavailable.
        if (errorCode != EErrorCode::TransportError && errorCode != EErrorCode::Unavailable) {
            OnDisconnected();
        }

        return;
    }

    if (IncrementalHeartbeat) {
        TChunks newAddedSinceLastSuccess;
        TChunks newRemovedSinceLastSuccess;

        for (TChunks::iterator it = AddedSinceLastSuccess.begin();
            it != AddedSinceLastSuccess.end();
            ++it)
        {
            if (ReportedAdded.find(*it) == ReportedAdded.end()) {
                newAddedSinceLastSuccess.insert(*it);
            }
        }

        for (TChunks::iterator it = RemovedSinceLastSuccess.begin();
            it != RemovedSinceLastSuccess.end();
            ++it)
        {
            if (ReportedRemoved.find(*it) == ReportedRemoved.end()) {
                newRemovedSinceLastSuccess.insert(*it);
            }
        }

        AddedSinceLastSuccess.swap(newAddedSinceLastSuccess);
        RemovedSinceLastSuccess.swap(newRemovedSinceLastSuccess);
    } else {
        IncrementalHeartbeat = true;
    }

    // TODO: handle chunk removals

    LOG_INFO("Successfully reported heartbeat to master");
}

void TMasterConnector::OnDisconnected()
{
    HolderId = InvalidHolderId;
    Registered = false;
    IncrementalHeartbeat = false;
    ReportedAdded.clear();
    ReportedRemoved.clear();
    AddedSinceLastSuccess.clear();
    RemovedSinceLastSuccess.clear();
}

NChunkManager::NProto::TChunkInfo TMasterConnector::GetInfo(TChunk::TPtr chunk)
{
    NChunkManager::NProto::TChunkInfo result;
    result.SetId(chunk->GetId().ToProto());
    result.SetSize(chunk->GetSize());
    return result;
}

void TMasterConnector::RegisterAddedChunk(TChunk::TPtr chunk)
{
    if (!IncrementalHeartbeat)
        return;

    LOG_DEBUG("Registered addition of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end())
        return;

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end()) {
        RemovedSinceLastSuccess.erase(chunk);
    }

    AddedSinceLastSuccess.insert(chunk);
}

void TMasterConnector::RegisterRemovedChunk(TChunk::TPtr chunk)
{
    if (!IncrementalHeartbeat)
        return;

    LOG_DEBUG("Registered removal of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());

    if (RemovedSinceLastSuccess.find(chunk) != RemovedSinceLastSuccess.end())
        return;

    if (AddedSinceLastSuccess.find(chunk) != AddedSinceLastSuccess.end()) {
        AddedSinceLastSuccess.erase(chunk);
    }

    RemovedSinceLastSuccess.insert(chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
