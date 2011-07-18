#include "chunk_manager.h"

#include "../misc/serialize.h"
#include "../misc/string.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkManagerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TState
    : public TRefCountedBase
{
public:

private:

};

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    const TConfig& config,
    NRpc::TServer* server)
    : TServiceBase(
        TChunkManagerProxy::GetServiceName(),
        ChunkManagerLogger.GetCategory())
    , Config(config)
    , ServiceInvoker(server->GetInvoker())
    , State(new TState())
    , HolderTracker(new THolderTracker(
        Config,
        ServiceInvoker))
{
    server->RegisterService(this);
}

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TChunkManager, RegisterHolder)
{
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());
    
    context->SetRequestInfo(statistics.ToString());

    THolder::TPtr holder = HolderTracker->RegisterHolder(statistics);

    response->SetHolderId(holder->GetId());

    context->SetResponseInfo("HolderId: %d", holder->GetId());

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkManager, HolderHeartbeat)
{
    // TODO: fixme
    UNUSED(response);

    int id = request->GetHolderId();
    THolderStatistics statistics = THolderStatistics::FromProto(request->GetStatistics());

    context->SetRequestInfo("HolderId: %d, %s, AddedChunkCount: %d, RemovedChunkCount: %d",
        id,
        statistics.ToString(),
        request->AddedChunksSize(),
        request->RemovedChunkSize());

    THolder::TPtr holder = GetHolder(id);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
