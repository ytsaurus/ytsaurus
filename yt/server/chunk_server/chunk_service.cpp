#include "stdafx.h"
#include "chunk_service.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <server/node_tracker_server/node_directory_builder.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>
#include <server/cell_master/master_hydra_service.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NNodeTrackerServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    explicit TChunkService(TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            TChunkServiceProxy::GetServiceName(),
            ChunkServerLogger)
    {
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(LocateChunks)
                .SetInvoker(bootstrap->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkMaintenance)));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        context->SetRequestInfo("ChunkCount: %v",
            request->chunk_ids_size());

        auto chunkManager = Bootstrap->GetChunkManager();
        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());

        for (const auto& protoChunkId : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto chunkIdWithIndex = DecodeChunkId(chunkId);

            auto* chunk = chunkManager->FindChunk(chunkIdWithIndex.Id);
            if (!chunk)
                continue;

            TChunkPtrWithIndex chunkWithIndex(chunk, chunkIdWithIndex.Index);
            auto replicas = chunkManager->LocateChunk(chunkWithIndex);

            auto* info = response->add_chunks();
            ToProto(info->mutable_chunk_id(), chunkId);
            ToProto(info->mutable_replicas(), replicas);

            for (auto replica : replicas) {
                nodeDirectoryBuilder.Add(replica);
            }
        }

        context->SetResponseInfo("ChunkCount: %v",
            response->chunks_size());
        context->Reply();
    }
};

NRpc::IServicePtr CreateChunkService(TBootstrap* boostrap)
{
    return New<TChunkService>(boostrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
