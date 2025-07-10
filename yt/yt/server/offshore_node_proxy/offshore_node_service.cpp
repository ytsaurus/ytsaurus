#include "offshore_node_service.h"

#include "private.h"

#include <yt/yt/ytlib/chunk_client/offshore_node_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/s3_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NOffshoreNodeProxy {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

// static constexpr auto& Logger = OffshoreNodeProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeService
    : public TServiceBase
{
public:
    TOffshoreNodeService(IInvokerPtr invoker, IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TOffshoreNodeServiceProxy::GetDescriptor(),
            OffshoreNodeProxyLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockSet));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetBlockRange));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetChunkMeta));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockSet)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());
        auto blockIndexes = FromProto<std::vector<int>>(request->block_indexes());

        context->SetRequestInfo("ChunkId: %v, BlockIndexes: %v",
            chunkId,
            blockIndexes);

        auto mediumDescriptor = TMediumDescriptor::CreateFromProto(request->medium_descriptor())->As<TS3MediumDescriptor>();
        YT_VERIFY(mediumDescriptor);

        auto reader = CreateS3Reader(mediumDescriptor, New<TS3ReaderConfig>(), chunkId);

        auto blocks = WaitFor(reader->ReadBlocks({}, blockIndexes))
            .ValueOrThrow();

        response->set_has_complete_chunk(true);
        response->set_net_throttling(false);
        response->set_net_queue_size(0);
        response->set_disk_throttling(false);
        response->set_disk_queue_size(0);

        SetRpcAttachedBlocks(response, blocks);

        context->SetResponseInfo("BlockCount: %v", blocks.size());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetBlockRange)
    {
        YT_UNIMPLEMENTED();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, GetChunkMeta)
    {
        auto chunkId = FromProto<TChunkId>(request->chunk_id());

        Cerr << Format("GetChunkMeta request for chunk %v", chunkId) << Endl;

        context->SetRequestInfo("ChunkId: %v", chunkId);

        auto mediumDescriptor = TMediumDescriptor::CreateFromProto(request->medium_descriptor())->As<TS3MediumDescriptor>();
        YT_VERIFY(mediumDescriptor);

        auto reader = CreateS3Reader(mediumDescriptor, New<TS3ReaderConfig>(), chunkId);

        auto meta = WaitFor(reader->GetMeta({}))
            .ValueOrThrow();

        Cerr << Format("Much wow such meta: %v", meta->ByteSize()) << Endl;;

        *response->mutable_chunk_meta() = static_cast<NChunkClient::NProto::TChunkMeta>(*meta);

        context->SetResponseInfo("MetaSize: %v", response->chunk_meta().ByteSize());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateOffshoreNodeService(IInvokerPtr invoker, IAuthenticatorPtr authenticator)
{
    return New<TOffshoreNodeService>(std::move(invoker), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
