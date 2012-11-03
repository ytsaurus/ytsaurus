#include "stdafx.h"
#include "chunk_proxy.h"
#include "private.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "node.h"

#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/table_client/schema.h>

#include <ytlib/table_client/table_ypath.pb.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NTableClient;
using namespace NObjectServer;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TChunkProxy
    : public TUnversionedObjectProxyBase<TChunk>
{
public:
    TChunkProxy(
        NCellMaster::TBootstrap* bootstrap,
        TMap* map,
        const TChunkId& id)
        : TBase(bootstrap, id, map)
    {
        Logger = ChunkServerLogger;
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Confirm);
        return TBase::IsWriteRequest(context);
    }

private:
    typedef TUnversionedObjectProxyBase<TChunk> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override
    {
        const auto* chunk = GetTypedImpl();
        auto miscExt = FindProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

        YCHECK(!chunk->IsConfirmed() || miscExt);

        attributes->push_back("confirmed");
        attributes->push_back("cached_locations");
        attributes->push_back("stored_locations");
        attributes->push_back("replication_factor");
        attributes->push_back("movable");
        attributes->push_back("master_meta_size");
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        attributes->push_back(TAttributeInfo("size", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("chunk_type", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("meta_size", chunk->IsConfirmed() && miscExt->has_meta_size()));
        attributes->push_back(TAttributeInfo("compressed_data_size", chunk->IsConfirmed() && miscExt->has_compressed_data_size()));
        attributes->push_back(TAttributeInfo("uncompressed_data_size", chunk->IsConfirmed() && miscExt->has_uncompressed_data_size()));
        attributes->push_back(TAttributeInfo("data_weight", chunk->IsConfirmed() && miscExt->has_data_weight()));
        attributes->push_back(TAttributeInfo("codec_id", chunk->IsConfirmed() && miscExt->has_codec_id()));
        attributes->push_back(TAttributeInfo("row_count", chunk->IsConfirmed() && miscExt->has_row_count()));
        attributes->push_back(TAttributeInfo("value_count", chunk->IsConfirmed() && miscExt->has_value_count()));
        attributes->push_back(TAttributeInfo("sorted", chunk->IsConfirmed() && miscExt->has_sorted()));
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        const auto* chunk = GetTypedImpl();

        if (key == "confirmed") {
            BuildYsonFluently(consumer)
                .Scalar(FormatBool(chunk->IsConfirmed()));
            return true;
        }

        if (key == "cached_locations") {
            if (~chunk->CachedLocations()) {
                BuildYsonFluently(consumer)
                    .DoListFor(*chunk->CachedLocations(), [=] (TFluentList fluent, TNodeId nodeId) {
                        const auto& node = chunkManager->GetNode(nodeId);
                        fluent.Item().Scalar(node->GetAddress());
                    });
            } else {
                BuildYsonFluently(consumer)
                    .BeginList()
                    .EndList();
            }
            return true;
        }

        if (key == "stored_locations") {
            BuildYsonFluently(consumer)
                .DoListFor(chunk->StoredLocations(), [=] (TFluentList fluent, TNodeId nodeId) {
                    const auto& node = chunkManager->GetNode(nodeId);
                    fluent.Item().Scalar(node->GetAddress());
                });
            return true;
        }

        if (key == "replication_factor") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->GetReplicationFactor());
            return true;
        }

        if (key == "movable") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->GetMovable());
            return true;
        }

        if (key == "master_meta_size") {
            BuildYsonFluently(consumer)
                .Scalar(chunk->ChunkMeta().ByteSize());
            return true;
        }

        if (key == "owning_nodes") {
            auto paths = chunkManager->GetOwningNodes(TChunkTreeRef(const_cast<TChunk*>(chunk)));
            BuildYsonFluently(consumer)
                .Scalar(paths);
            return true;
        }

        if (chunk->IsConfirmed()) {
            auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

            if (key == "size") {
                BuildYsonFluently(consumer)
                    .Scalar(chunk->ChunkInfo().size());
                return true;
            }

            if (key == "chunk_type") {
                auto type = EChunkType(chunk->ChunkMeta().type());
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(type.ToString()));
                return true;
            }

            if (key == "meta_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.meta_size());
                return true;
            }

            if (key == "compressed_data_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.compressed_data_size());
                return true;
            }

            if (key == "uncompressed_data_size") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.uncompressed_data_size());
                return true;
            }

            if (key == "data_weight") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.data_weight());
                return true;
            }

            if (key == "codec_id") {
                BuildYsonFluently(consumer)
                    .Scalar(CamelCaseToUnderscoreCase(ECodecId(miscExt.codec_id()).ToString()));
                return true;
            }

            if (key == "row_count") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.row_count());
                return true;
            }

            if (key == "value_count") {
                BuildYsonFluently(consumer)
                    .Scalar(miscExt.value_count());
                return true;
            }

            if (key == "sorted") {
                BuildYsonFluently(consumer)
                    .Scalar(FormatBool(miscExt.sorted()));
                return true;
            }
        }

        return TBase::GetSystemAttribute(key, consumer);
    }

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Locate);
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        TBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Locate)
    {
        UNUSED(request);

        auto chunkManager = Bootstrap->GetChunkManager();

        const auto* chunk = GetTypedImpl();
        chunkManager->FillNodeAddresses(response->mutable_node_addresses(), chunk);

        context->SetResponseInfo("NodeAddresses: [%s]",
            ~JoinToString(response->node_addresses()));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, Fetch)
    {
        UNUSED(request);

        auto chunkManager = Bootstrap->GetChunkManager();
        const auto* chunk = GetTypedImpl();

        if (chunk->ChunkMeta().type() != EChunkType::Table) {
            THROW_ERROR_EXCEPTION("Unable to execute Fetch verb for non-table chunk");
        }

        auto* inputChunk = response->add_chunks();
        *inputChunk->mutable_slice()->mutable_chunk_id() = chunk->GetId().ToProto();
        inputChunk->mutable_slice()->mutable_start_limit();
        inputChunk->mutable_slice()->mutable_end_limit();
        *inputChunk->mutable_channel() = TChannel::Universal().ToProto();
        inputChunk->mutable_extensions()->CopyFrom(chunk->ChunkMeta().extensions());

        auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
        inputChunk->set_row_count(miscExt.row_count());
        inputChunk->set_uncompressed_data_size(miscExt.uncompressed_data_size());

        if (request->fetch_node_addresses()) {
            chunkManager->FillNodeAddresses(inputChunk->mutable_node_addresses(), chunk);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, Confirm)
    {
        UNUSED(response);

        auto chunkManager = Bootstrap->GetChunkManager();

        auto addresses = FromProto<Stroka>(request->node_addresses());
        YCHECK(!addresses.empty());

        context->SetRequestInfo("Size: %" PRId64 ", Addresses: [%s]",
            request->chunk_info().size(),
            ~JoinToString(addresses));

        auto* chunk = GetTypedImpl();

        // Skip chunks that are already confirmed.
        if (chunk->IsConfirmed()) {
            context->SetResponseInfo("Chunk is already confirmed");
            context->Reply();
            return;
        }

        // Use the size reported by the client, but check it for consistency first.
        if (!chunk->ValidateChunkInfo(request->chunk_info())) {
            LOG_FATAL("Invalid chunk info reported by client (ChunkId: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s})",
                ~Id.ToString(),
                ~chunk->ChunkInfo().DebugString(),
                ~request->chunk_info().DebugString());
        }

        chunkManager->ConfirmChunk(
            chunk,
            addresses,
            request->mutable_chunk_info(),
            request->mutable_chunk_meta());

        context->Reply();
    }
};

IObjectProxyPtr CreateChunkProxy(
    NCellMaster::TBootstrap* bootstrap,
    NMetaState::TMetaStateMap<TChunkId, TChunk>* map,
    const TChunkId& id)
{
    return New<TChunkProxy>(
        bootstrap,
        map,
        id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
