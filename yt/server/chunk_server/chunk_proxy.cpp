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

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/unversioned_row.h>

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/node_directory_builder.h>

#include <server/object_server/object_detail.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/transaction_server/transaction.h>

#include <server/security_server/account.h>

#include <server/journal_server/journal_manager.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NNodeTrackerServer;

using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::TBoundaryKeysExt;

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

    virtual bool IsLeaderReadRequired() const override
    {
        // Needed due to TChunkManager::ComputeChunkStatus call below.
        return true;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* chunk = GetThisTypedImpl();
        auto miscExt = FindProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
        YCHECK(!chunk->IsConfirmed() || miscExt);

        bool hasBoundaryKeysExt = HasProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());

        auto objectManager = Bootstrap_->GetObjectManager();
        auto isForeign = objectManager->IsForeign(chunk);

        descriptors->push_back(TAttributeDescriptor("cached_replicas")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("stored_replicas")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("movable")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("vital")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("overreplicated")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("underreplicated")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("lost")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("data_missing")
            .SetPresent(chunk->IsErasure() && !isForeign));
        descriptors->push_back(TAttributeDescriptor("parity_missing")
            .SetPresent(chunk->IsErasure() && !isForeign));
        descriptors->push_back(TAttributeDescriptor("unsafely_placed")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("available")
            .SetPresent(!isForeign));
        descriptors->push_back("confirmed");
        descriptors->push_back(TAttributeDescriptor("replication_factor")
            .SetPresent(!chunk->IsErasure()));
        descriptors->push_back(TAttributeDescriptor("erasure_codec")
            .SetPresent(chunk->IsErasure()));
        descriptors->push_back("master_meta_size");
        descriptors->push_back(TAttributeDescriptor("owning_nodes")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("disk_space")
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor("chunk_type")
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor("meta_size")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_meta_size()));
        descriptors->push_back(TAttributeDescriptor("compressed_data_size")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_compressed_data_size()));
        descriptors->push_back(TAttributeDescriptor("uncompressed_data_size")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_uncompressed_data_size()));
        descriptors->push_back(TAttributeDescriptor("data_weight")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_data_weight()));
        descriptors->push_back(TAttributeDescriptor("compression_codec")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_compression_codec()));
        descriptors->push_back(TAttributeDescriptor("row_count")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_row_count()));
        descriptors->push_back(TAttributeDescriptor("quorum_row_count")
            .SetPresent(chunk->IsJournal())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("sealed")
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor("value_count")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_value_count()));
        descriptors->push_back(TAttributeDescriptor("sorted")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_sorted()));
        descriptors->push_back(TAttributeDescriptor("min_timestamp")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_min_timestamp()));
        descriptors->push_back(TAttributeDescriptor("max_timestamp")
            .SetPresent(chunk->IsConfirmed() && miscExt->has_max_timestamp()));
        descriptors->push_back(TAttributeDescriptor("staging_transaction_id")
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor("staging_account")
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor("min_key")
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor("max_key")
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor("read_quorum")
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor("write_quorum")
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor("eden")
            .SetPresent(chunk->IsConfirmed()));
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto chunkManager = Bootstrap_->GetChunkManager();
        auto cypressManager = Bootstrap_->GetCypressManager();

        auto* chunk = GetThisTypedImpl();
        auto status = chunkManager->ComputeChunkStatus(chunk);

        auto objectManager = Bootstrap_->GetObjectManager();
        auto isForeign = objectManager->IsForeign(chunk);

        typedef std::function<void(TFluentList fluent, TNodePtrWithIndex replica)> TReplicaSerializer;

        auto serializeRegularReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .Value(replica.GetPtr()->GetDefaultAddress());
        };

        auto serializeErasureReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .BeginAttributes()
                    .Item("index").Value(replica.GetIndex())
                .EndAttributes()
                .Value(replica.GetPtr()->GetDefaultAddress());
        };

        auto serializeJournalReplica = [] (TFluentList fluent, TNodePtrWithIndex replica) {
            fluent.Item()
                .BeginAttributes()
                    .Item("type").Value(EJournalReplicaType(replica.GetIndex()))
                .EndAttributes()
                .Value(replica.GetPtr()->GetDefaultAddress());
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

        if (!isForeign) {
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

            if (key == "available") {
                BuildYsonFluently(consumer)
                    .Value(chunk->IsAvailable());
                return true;
            }
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

        if (key == "confirmed") {
            BuildYsonFluently(consumer)
                .Value(chunk->IsConfirmed());
            return true;
        }

        if (key == "master_meta_size") {
            BuildYsonFluently(consumer)
                .Value(chunk->ChunkMeta().ByteSize());
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

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override
    {
        auto* chunk = GetThisTypedImpl();

        if (chunk->IsJournal() && key == "quorum_row_count") {
            auto chunkManager = Bootstrap_->GetChunkManager();
            auto rowCountResult = chunkManager->GetChunkQuorumInfo(chunk);
            return rowCountResult.Apply(BIND([=] (const TMiscExt& miscExt) {
                return MakeFuture(ConvertToYsonString(miscExt.row_count()));
            }));
        }

        if (key == "owning_nodes") {
            return GetMulticellOwningNodes(Bootstrap_, chunk);
        }

        return TBase::GetBuiltinAttributeAsync(key);
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

        auto journalManager = Bootstrap_->GetJournalManager();
        journalManager->SealChunk(chunk, info);

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
