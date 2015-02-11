#include "stdafx.h"
#include "chunk_proxy.h"
#include "private.h"
#include "chunk_manager.h"
#include "chunk.h"
#include "helpers.h"

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/fluent.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>
#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/node_directory_builder.h>

#include <server/object_server/object_detail.h>

#include <server/cypress_server/cypress_manager.h>

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
using namespace NVersionedTableClient;
using namespace NNodeTrackerServer;

using NChunkClient::NProto::TMiscExt;
using NVersionedTableClient::NProto::TBoundaryKeysExt;

////////////////////////////////////////////////////////////////////////////////

class TChunkProxy
    : public TNonversionedObjectProxyBase<TChunk>
{
public:
    TChunkProxy(NCellMaster::TBootstrap* bootstrap, TChunk* chunk)
        : TBase(bootstrap, chunk)
    { }

private:
    typedef TNonversionedObjectProxyBase<TChunk> TBase;

    virtual NLog::TLogger CreateLogger() const override
    {
        return ChunkServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* chunk = GetThisTypedImpl();
        auto miscExt = FindProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
        YCHECK(!chunk->IsConfirmed() || miscExt);

        bool hasBoundaryKeysExt = HasProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());

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
        attributes->push_back("unsafely_placed");
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
        attributes->push_back(TAttributeInfo("compression_codec", chunk->IsConfirmed() && miscExt->has_compression_codec(), false, true));
        attributes->push_back(TAttributeInfo("row_count", chunk->IsConfirmed() && miscExt->has_row_count()));
        attributes->push_back(TAttributeInfo("quorum_row_count", chunk->IsJournal(), true));
        attributes->push_back(TAttributeInfo("sealed", chunk->IsJournal()));
        attributes->push_back(TAttributeInfo("value_count", chunk->IsConfirmed() && miscExt->has_value_count()));
        attributes->push_back(TAttributeInfo("sorted", chunk->IsConfirmed() && miscExt->has_sorted()));
        attributes->push_back(TAttributeInfo("min_timestamp", chunk->IsConfirmed() && miscExt->has_min_timestamp()));
        attributes->push_back(TAttributeInfo("max_timestamp", chunk->IsConfirmed() && miscExt->has_max_timestamp()));
        attributes->push_back(TAttributeInfo("staging_transaction_id", chunk->IsStaged()));
        attributes->push_back(TAttributeInfo("staging_account", chunk->IsStaged()));
        attributes->push_back(TAttributeInfo("min_key", hasBoundaryKeysExt));
        attributes->push_back(TAttributeInfo("max_key", hasBoundaryKeysExt));        
        attributes->push_back(TAttributeInfo("read_quorum", chunk->IsJournal()));
        attributes->push_back(TAttributeInfo("write_quorum", chunk->IsJournal()));
        attributes->push_back(TAttributeInfo("eden", chunk->IsConfirmed()));
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto cypressManager = Bootstrap_->GetCypressManager();

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

        auto serializeJournalReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .BeginAttributes()
                    .Item("type").Value(EJournalReplicaType(replica.GetIndex()))
                .EndAttributes()
                .Value(replica.GetPtr()->GetAddress());
        };

        TReplicaSerializer serializeReplica;
        if (chunk->IsErasure()) {
            serializeReplica = TReplicaSerializer(serializeErasureReplica);
        } else if (chunk->IsJournal()) {
            serializeReplica = TReplicaSerializer(serializeJournalReplica);
        } else {
            serializeReplica = TReplicaSerializer(serializeRegularReplica);
        }

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
                .Value(Any(status & EChunkStatus::Underreplicated));
            return true;
        }

        if (key == "overreplicated") {
            BuildYsonFluently(consumer)
                .Value(Any(status & EChunkStatus::Overreplicated));
            return true;
        }

        if (key == "lost") {
            BuildYsonFluently(consumer)
                .Value(Any(status & EChunkStatus::Lost));
            return true;
        }

        if (key == "data_missing") {
            BuildYsonFluently(consumer)
                .Value(Any(status & EChunkStatus::DataMissing));
            return true;
        }

        if (key == "parity_missing") {
            BuildYsonFluently(consumer)
                .Value(Any(status & EChunkStatus::ParityMissing));
            return true;
        }

        if (key == "unsafely_placed") {
            BuildYsonFluently(consumer)
                .Value(Any(status & EChunkStatus::UnsafelyPlaced));
            return true;
        }

        if (key == "confirmed") {
            BuildYsonFluently(consumer)
                .Value(chunk->IsConfirmed());
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
            SerializeOwningNodesPaths(
                cypressManager,
                const_cast<TChunk*>(chunk),
                consumer);
            return true;
        }

        if (chunk->IsConfirmed()) {
            const auto& miscExt = chunk->MiscExt();

            if (key == "disk_space") {
                BuildYsonFluently(consumer)
                    .Value(chunk->ChunkInfo().disk_space());
                return true;
            }

            if (key == "chunk_type") {
                auto type = EChunkType(chunk->ChunkMeta().type());
                BuildYsonFluently(consumer)
                    .Value(type);
                return true;
            }

            if (key == "meta_size" && miscExt.has_meta_size()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.meta_size());
                return true;
            }

            if (key == "compressed_data_size" && miscExt.has_compressed_data_size()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.compressed_data_size());
                return true;
            }

            if (key == "uncompressed_data_size" && miscExt.has_uncompressed_data_size()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.uncompressed_data_size());
                return true;
            }

            if (key == "data_weight" && miscExt.has_data_weight()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.data_weight());
                return true;
            }

            if (key == "compression_codec" && miscExt.has_compression_codec()) {
                BuildYsonFluently(consumer)
                    .Value(NCompression::ECodec(miscExt.compression_codec()));
                return true;
            }

            if (key == "row_count" && miscExt.has_row_count()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.row_count());
                return true;
            }

            if (key == "sealed" && chunk->IsJournal()) {
                BuildYsonFluently(consumer)
                    .Value(chunk->IsSealed());
                return true;
            }

            if (key == "value_count" && miscExt.has_value_count()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.value_count());
                return true;
            }

            if (key == "sorted" && miscExt.has_sorted()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.sorted());
                return true;
            }

            if (key == "min_timestamp" && miscExt.has_min_timestamp()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.min_timestamp());
                return true;
            }

            if (key == "max_timestamp" && miscExt.has_max_timestamp()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.max_timestamp());
                return true;
            }

            if (key == "read_quorum" && chunk->IsJournal()) {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetReadQuorum());
                return true;
            }

            if (key == "write_quorum" && chunk->IsJournal()) {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetWriteQuorum());
                return true;
            }

            if (key == "eden") {
                BuildYsonFluently(consumer)
                    .Value(miscExt.eden());
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

        auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
        if (boundaryKeysExt) {
            if (key == "min_key") {
                BuildYsonFluently(consumer)
                    .Value(FromProto<TOwningKey>(boundaryKeysExt->min()));
                return true;
            }

            if (key == "max_key") {
                BuildYsonFluently(consumer)
                    .Value(FromProto<TOwningKey>(boundaryKeysExt->max()));
                return true;
            }
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<void> GetBuiltinAttributeAsync(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto* chunk = GetThisTypedImpl();
        if (chunk->IsJournal() && key == "quorum_row_count") {
            auto chunkManager = Bootstrap_->GetChunkManager();
            auto rowCountResult = chunkManager->GetChunkQuorumInfo(chunk);
            return rowCountResult.Apply(BIND([=] (const TMiscExt& miscExt) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.row_count());
            }));
        }
        return TBase::GetBuiltinAttributeAsync(key, consumer);
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        DISPATCH_YPATH_SERVICE_METHOD(Confirm);
        DISPATCH_YPATH_SERVICE_METHOD(Seal);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch)
    {
        UNUSED(request);

        DeclareNonMutating();

        context->SetRequestInfo();

        auto chunkManager = Bootstrap_->GetChunkManager();
        const auto* chunk = GetThisTypedImpl();

        auto replicas = chunk->GetReplicas();

        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());
        nodeDirectoryBuilder.Add(replicas);

        auto* chunkSpec = response->add_chunks();
        ToProto(chunkSpec->mutable_replicas(), replicas);
        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
        chunkSpec->mutable_chunk_meta()->set_type(chunk->ChunkMeta().type());
        chunkSpec->mutable_chunk_meta()->set_version(chunk->ChunkMeta().version());
        chunkSpec->mutable_chunk_meta()->mutable_extensions()->CopyFrom(chunk->ChunkMeta().extensions());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Confirm)
    {
        UNUSED(response);

        DeclareMutating();

        auto replicas = FromProto<NChunkClient::TChunkReplica>(request->replicas());

        context->SetRequestInfo("Targets: [%v]", JoinToString(replicas));

        auto* chunk = GetThisTypedImpl();

        // Skip chunks that are already confirmed.
        if (chunk->IsConfirmed()) {
            context->Reply();
            return;
        }

        auto chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->ConfirmChunk(
            chunk,
            replicas,
            request->mutable_chunk_info(),
            request->mutable_chunk_meta());

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Seal)
    {
        UNUSED(response);

        DeclareMutating();

        context->SetRequestInfo();

        const auto& info = request->info();
        YCHECK(info.sealed());

        auto* chunk = GetThisTypedImpl();
        auto chunkManager = Bootstrap_->GetChunkManager();
        chunkManager->SealChunk(chunk, info);

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
