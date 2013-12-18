#include "stdafx.h"
#include "chunk_proxy.h"
#include "private.h"
#include "chunk_manager.h"
#include "chunk.h"

#include <core/ytree/fluent.h>

#include <ytlib/chunk_client/chunk.pb.h>
#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/node_directory_builder.h>

#include <server/object_server/object_detail.h>

#include <server/transaction_server/transaction.h>

#include <server/security_server/account.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NNodeTrackerServer;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

class TChunkProxy
    : public TNonversionedObjectProxyBase<TChunk>
{
public:
    TChunkProxy(NCellMaster::TBootstrap* bootstrap, TChunk* chunk)
        : TBase(bootstrap, chunk)
    {
        Logger = ChunkServerLogger;
    }

private:
    typedef TNonversionedObjectProxyBase<TChunk> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* chunk = GetThisTypedImpl();
        auto miscExt = FindProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
        YCHECK(!chunk->IsConfirmed() || miscExt);

        attributes->push_back("cached_replicas");
        attributes->push_back("stored_replicas");
        attributes->push_back(TAttributeInfo("replication_factor", !chunk->IsErasure(), false));
        attributes->push_back(TAttributeInfo("erasure_codec", chunk->IsErasure(), false));
        attributes->push_back("movable");
        attributes->push_back("vital");
        attributes->push_back("overreplicated");
        attributes->push_back("underreplicated");
        attributes->push_back("lost");
        attributes->push_back(TAttributeInfo("data_missing", chunk->IsErasure()));
        attributes->push_back(TAttributeInfo("parity_missing", chunk->IsErasure()));
        attributes->push_back("confirmed");
        attributes->push_back("available");
        attributes->push_back("master_meta_size");
        attributes->push_back(TAttributeInfo("owning_nodes", true, true));
        attributes->push_back(TAttributeInfo("disk_space", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("chunk_type", chunk->IsConfirmed()));
        attributes->push_back(TAttributeInfo("meta_size", chunk->IsConfirmed() && miscExt->has_meta_size()));
        attributes->push_back(TAttributeInfo("compressed_data_size", chunk->IsConfirmed() && miscExt->has_compressed_data_size()));
        attributes->push_back(TAttributeInfo("uncompressed_data_size", chunk->IsConfirmed() && miscExt->has_uncompressed_data_size()));
        attributes->push_back(TAttributeInfo("data_weight", chunk->IsConfirmed() && miscExt->has_data_weight()));
        attributes->push_back(TAttributeInfo("compression_codec", chunk->IsConfirmed() && miscExt->has_compression_codec()));
        attributes->push_back(TAttributeInfo("row_count", chunk->IsConfirmed() && miscExt->has_row_count()));
        attributes->push_back(TAttributeInfo("value_count", chunk->IsConfirmed() && miscExt->has_value_count()));
        attributes->push_back(TAttributeInfo("sorted", chunk->IsConfirmed() && miscExt->has_sorted()));
        attributes->push_back(TAttributeInfo("staging_transaction_id", chunk->IsStaged()));
        attributes->push_back(TAttributeInfo("staging_account", chunk->IsStaged()));
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto* chunk = GetThisTypedImpl();
        auto status = chunkManager->ComputeChunkStatus(chunk);

        typedef std::function<void(TFluentList fluent, TNodePtrWithIndex replica)> TReplicaSerializer;

        auto serializeRegularReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .Value(replica.GetPtr()->GetAddress());
        };

        auto serializeErasureReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .BeginAttributes()
                    .Item("index").Value(replica.GetIndex())
                .EndAttributes()
                .Value(replica.GetPtr()->GetAddress());
        };

        auto serializeReplica = chunk->IsErasure()
            ? TReplicaSerializer(serializeErasureReplica)
            : TReplicaSerializer(serializeRegularReplica);

        auto serializeReplicas = [&] (IYsonConsumer* consumer, TNodePtrWithIndexList& replicas) {
            std::sort(
                replicas.begin(),
                replicas.end(),
                [] (TNodePtrWithIndex lhs, TNodePtrWithIndex rhs) {
                    return lhs.GetIndex() < rhs.GetIndex();
                });
            BuildYsonFluently(consumer)
                .DoListFor(replicas, serializeReplica);
        };

        if (key == "cached_replicas") {
            TNodePtrWithIndexList replicas;
            if (chunk->CachedReplicas()) {
                replicas = TNodePtrWithIndexList(chunk->CachedReplicas()->begin(), chunk->CachedReplicas()->end());
            }
            serializeReplicas(consumer, replicas);
            return true;
        }

        if (key == "stored_replicas") {
            auto replicas = chunk->StoredReplicas();
            serializeReplicas(consumer, replicas);
            return true;
        }

        if (chunk->IsErasure()) {
            if (key == "erasure_codec") {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetErasureCodec());
                return true;
            }
        } else {
            if (key == "replication_factor") {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetReplicationFactor());
                return true;
            }
        }

        if (key == "movable") {
            BuildYsonFluently(consumer)
                .Value(chunk->GetMovable());
            return true;
        }

        if (key == "vital") {
            BuildYsonFluently(consumer)
                .Value(chunk->GetVital());
            return true;
        }

        if (key == "underreplicated") {
            BuildYsonFluently(consumer)
                .Value((status & EChunkStatus::Underreplicated) != 0);
            return true;
        }

        if (key == "overreplicated") {
            BuildYsonFluently(consumer)
                .Value((status & EChunkStatus::Overreplicated) != 0);
            return true;
        }

        if (key == "lost") {
            BuildYsonFluently(consumer)
                .Value((status & EChunkStatus::Lost) != 0);
            return true;
        }

        if (key == "data_missing") {
            BuildYsonFluently(consumer)
                .Value((status & EChunkStatus::DataMissing) != 0);
            return true;
        }

        if (key == "parity_missing") {
            BuildYsonFluently(consumer)
                .Value((status & EChunkStatus::ParityMissing) != 0);
            return true;
        }

        if (key == "confirmed") {
            BuildYsonFluently(consumer)
                .Value(FormatBool(chunk->IsConfirmed()));
            return true;
        }

        if (key == "available") {
            BuildYsonFluently(consumer)
                .Value(chunk->IsAvailable());
            return true;
        }

        if (key == "master_meta_size") {
            BuildYsonFluently(consumer)
                .Value(chunk->ChunkMeta().ByteSize());
            return true;
        }

        if (key == "owning_nodes") {
            auto paths = chunkManager->GetOwningNodes(const_cast<TChunk*>(chunk));
            BuildYsonFluently(consumer)
                .Value(paths);
            return true;
        }

        if (chunk->IsConfirmed()) {
            auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());

            if (key == "disk_space") {
                BuildYsonFluently(consumer)
                    .Value(chunk->ChunkInfo().disk_space());
                return true;
            }

            if (key == "chunk_type") {
                auto type = EChunkType(chunk->ChunkMeta().type());
                BuildYsonFluently(consumer)
                    .Value(CamelCaseToUnderscoreCase(type.ToString()));
                return true;
            }

            if (key == "meta_size") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.meta_size());
                return true;
            }

            if (key == "compressed_data_size") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.compressed_data_size());
                return true;
            }

            if (key == "uncompressed_data_size") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.uncompressed_data_size());
                return true;
            }

            if (key == "data_weight") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.data_weight());
                return true;
            }

            if (key == "compression_codec") {
                BuildYsonFluently(consumer)
                    .Value(CamelCaseToUnderscoreCase(NCompression::ECodec(miscExt.compression_codec()).ToString()));
                return true;
            }

            if (key == "row_count") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.row_count());
                return true;
            }

            if (key == "value_count") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.value_count());
                return true;
            }

            if (key == "sorted") {
                BuildYsonFluently(consumer)
                    .Value(FormatBool(miscExt.sorted()));
                return true;
            }
        }

        if (chunk->IsStaged()) {
            if (key == "staging_transaction_id") {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetStagingTransaction()->GetId());
                return true;
            }

            if (key == "staging_account") {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetStagingAccount()->GetName());
                return true;
            }
        }

        return TBase::GetSystemAttribute(key, consumer);
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch)
    {
        UNUSED(request);

        DeclareNonMutating();

        context->SetRequestInfo("");

        auto chunkManager = Bootstrap->GetChunkManager();
        const auto* chunk = GetThisTypedImpl();

        auto replicas = chunk->GetReplicas();

        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());
        nodeDirectoryBuilder.Add(replicas);

        auto* chunkSpec = response->add_chunks();
        ToProto(chunkSpec->mutable_replicas(), replicas);
        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(chunk->GetErasureCodec());
        chunkSpec->mutable_chunk_meta()->set_type(chunk->ChunkMeta().type());
        chunkSpec->mutable_chunk_meta()->set_version(chunk->ChunkMeta().version());
        chunkSpec->mutable_chunk_meta()->mutable_extensions()->CopyFrom(chunk->ChunkMeta().extensions());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Confirm)
    {
        UNUSED(response);

        DeclareMutating();

        auto chunkManager = Bootstrap->GetChunkManager();

        auto replicas = FromProto<NChunkClient::TChunkReplica>(request->replicas());
        YCHECK(!replicas.empty());

        context->SetRequestInfo("DiskSpace: %" PRId64 ", Targets: [%s]",
            request->chunk_info().disk_space(),
            ~JoinToString(replicas));

        auto* chunk = GetThisTypedImpl();

        // Skip chunks that are already confirmed.
        if (chunk->IsConfirmed()) {
            context->SetResponseInfo("Chunk is already confirmed");
            context->Reply();
            return;
        }

        // Use the size reported by the client, but check it for consistency first.
        if (!chunk->ValidateChunkInfo(request->chunk_info())) {
            THROW_ERROR_EXCEPTION("Invalid chunk info reported by client (ChunkId: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s})",
                ~ToString(chunk->GetId()),
                ~chunk->ChunkInfo().DebugString(),
                ~request->chunk_info().DebugString());
        }

        chunkManager->ConfirmChunk(
            chunk,
            replicas,
            request->mutable_chunk_info(),
            request->mutable_chunk_meta());

        context->Reply();
    }
};

IObjectProxyPtr CreateChunkProxy(
    NCellMaster::TBootstrap* bootstrap,
    TChunk* chunk)
{
    return New<TChunkProxy>(bootstrap, chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
