#include "master_connector.h"

#include <util/system/hostname.h>

#include "../rpc/client.h"

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

    LOG_INFO("Chunk holder address is %s, master address is %s",
        ~Address,
        ~Config.MasterAddress);
}

void TMasterConnector::InitializeProxy()
{
    NBus::TBusClient::TPtr busClient = new NBus::TBusClient(Config.MasterAddress);
    NRpc::TChannel::TPtr channel = new NRpc::TChannel(busClient);
    Proxy.Reset(new TProxy(channel));
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
        OnError();

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
        for (TChunks::iterator it = AddedChunks.begin();
            it != AddedChunks.end();
            ++it)
        {
            *request->AddAddedChunks() = GetInfo(*it);
        }

        for (TChunks::iterator it = RemovedChunks.begin();
            it != RemovedChunks.end();
            ++it)
        {
            request->AddRemovedChunk(ProtoGuidFromGuid((*it)->GetId()));
        }
    } else {
        TChunks chunks = ChunkStore->GetChunks();
        for (TChunks::iterator it = chunks.begin();
             it != chunks.end();
             ++it)
        {
            *request->AddAddedChunks() = GetInfo(*it);
        }
    }

    request->Invoke(Config.RpcTimeout)->Subscribe(
        FromMethod(&TMasterConnector::OnHeartbeatResponse, TPtr(this))
        ->Via(ServiceInvoker));

    IncrementalHeartbeat = true;
    AddedChunks.clear();
    RemovedChunks.clear();

    LOG_DEBUG("Heartbeat sent (%s, AddedChunks: %d, RemovedChunks: %d)",
        ~statistics.ToString(),
        static_cast<int>(request->AddedChunksSize()),
        static_cast<int>(request->RemovedChunkSize()));
}

void TMasterConnector::OnHeartbeatResponse(TProxy::TRspHolderHeartbeat::TPtr response)
{
    ScheduleHeartbeat();

    if (!response->IsOK()) {
        OnError();

        LOG_WARNING("Error sending heartbeat to master (ErrorCode: %s)",
            ~response->GetErrorCode().ToString());

        return;
    }

    // TODO: handle chunk removals

    LOG_INFO("Successfully reported heartbeat to master");
}

void TMasterConnector::OnError()
{
    HolderId = InvalidHolderId;
    Registered = false;
    IncrementalHeartbeat = false;
}

NChunkManager::NProto::TChunkInfo TMasterConnector::GetInfo(TChunk::TPtr chunk)
{
    NChunkManager::NProto::TChunkInfo result;
    result.SetId(ProtoGuidFromGuid(chunk->GetId()));
    result.SetSize(chunk->GetSize());
    return result;
}

void TMasterConnector::RegisterAddedChunk(TChunk::TPtr chunk)
{
    AddedChunks.push_back(chunk);

    LOG_DEBUG("Registered addition of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());
}

void TMasterConnector::RegisterRemovedChunk(TChunk::TPtr chunk)
{
    RemovedChunks.push_back(chunk);

    LOG_DEBUG("Registered removal of chunk (ChunkId: %s)",
        ~chunk->GetId().ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
