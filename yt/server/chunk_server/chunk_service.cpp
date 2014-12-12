#include "stdafx.h"
#include "chunk_service.h"
#include "chunk_manager.h"
#include "node_directory_builder.h"
#include "chunk.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <server/cell_master/meta_state_service.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NChunkClient;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TChunkService
    : public NCellMaster::TMetaStateServiceBase
{
public:
    explicit TChunkService(TBootstrap* bootstrap)
        : TMetaStateServiceBase(
            bootstrap,
            TChunkServiceProxy::GetServiceName(),
            ChunkServerLogger.GetCategory())
    {
        RegisterMethod(
            RPC_SERVICE_METHOD_DESC(LocateChunks)
                .SetInvoker(bootstrap->GetMetaStateFacade()->GetGuardedInvoker(EStateThreadQueue::ChunkLocator)));
    }

private:
    typedef TChunkService TThis;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        context->SetRequestInfo("ChunkCount: %d",
            request->chunk_ids_size());

        auto chunkManager = Bootstrap->GetChunkManager();
        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());

        FOREACH (const auto& protoChunkId, request->chunk_ids()) {
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

            FOREACH (auto replica, replicas) {
                nodeDirectoryBuilder.Add(replica);
            }
        }

        context->SetResponseInfo("ChunkCount: %d",
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
