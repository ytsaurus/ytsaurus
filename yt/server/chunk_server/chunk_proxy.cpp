#include "chunk_proxy.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "helpers.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/security_server/account.h>

#include <yt/server/transaction_server/transaction.h>

#include <yt/ytlib/chunk_client/chunk_meta.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_owner_ypath.pb.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/fluent.h>

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
    TChunkProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TChunk* chunk)
        : TBase(bootstrap, metadata, chunk)
    { }

private:
    typedef TNonversionedObjectProxyBase<TChunk> TBase;


    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* chunk = GetThisImpl();
        const auto& miscExt = chunk->MiscExt();

        bool hasBoundaryKeysExt = HasProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
        auto isForeign = chunk->IsForeign();

        descriptors->push_back(TAttributeDescriptor("cached_replicas")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("stored_replicas")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("last_seen_replicas")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("movable")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("media")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("vital")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("replication_status")
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor("available")
            .SetPresent(!isForeign));
        descriptors->push_back("confirmed");
        descriptors->push_back(TAttributeDescriptor("erasure_codec")
            .SetPresent(chunk->IsErasure()));
        descriptors->push_back("master_meta_size");
        descriptors->push_back(TAttributeDescriptor("parent_ids")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("owning_nodes")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("exports")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("disk_space")
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor("chunk_type")
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor("table_chunk_format")
            .SetPresent(chunk->IsConfirmed() && EChunkType(chunk->ChunkMeta().type()) == EChunkType::Table));
        descriptors->push_back(TAttributeDescriptor("meta_size")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_meta_size()));
        descriptors->push_back(TAttributeDescriptor("compressed_data_size")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_compressed_data_size()));
        descriptors->push_back(TAttributeDescriptor("uncompressed_data_size")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_uncompressed_data_size()));
        descriptors->push_back(TAttributeDescriptor("data_weight")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_data_weight()));
        descriptors->push_back(TAttributeDescriptor("compression_codec")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_compression_codec()));
        descriptors->push_back(TAttributeDescriptor("row_count")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_row_count()));
        descriptors->push_back(TAttributeDescriptor("max_block_size")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_max_block_size()));
        descriptors->push_back(TAttributeDescriptor("quorum_row_count")
            .SetPresent(chunk->IsJournal())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("sealed")
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor("value_count")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_value_count()));
        descriptors->push_back(TAttributeDescriptor("sorted")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_sorted()));
        descriptors->push_back(TAttributeDescriptor("min_timestamp")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_min_timestamp()));
        descriptors->push_back(TAttributeDescriptor("max_timestamp")
            .SetPresent(chunk->IsConfirmed() && miscExt.has_max_timestamp()));
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
        descriptors->push_back("scan_flags");
        descriptors->push_back(TAttributeDescriptor("creation_time")
            .SetPresent(miscExt.has_creation_time()));
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto* chunk = GetThisImpl();
        auto isForeign = chunk->IsForeign();

        auto serializePhysicalReplica = [&] (TFluentList fluent, TNodePtrWithIndexes replica) {
            auto* medium = chunkManager->GetMediumByIndex(replica.GetMediumIndex());
            fluent.Item()
                .BeginAttributes()
                    .Item("medium").Value(medium->GetName())
                    .DoIf(chunk->IsErasure(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("index").Value(replica.GetReplicaIndex());
                    })
                    .DoIf(chunk->IsJournal(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("type").Value(EJournalReplicaType(replica.GetReplicaIndex()));
                    })
                .EndAttributes()
                .Value(replica.GetPtr()->GetDefaultAddress());
        };

        auto serializePhysicalReplicas = [&] (IYsonConsumer* consumer, TNodePtrWithIndexesList& replicas) {
            std::sort(
                replicas.begin(),
                replicas.end(),
                [] (TNodePtrWithIndexes lhs, TNodePtrWithIndexes rhs) {
                    if (lhs.GetReplicaIndex() != rhs.GetReplicaIndex()) {
                        return lhs.GetReplicaIndex() < rhs.GetReplicaIndex();
                    }
                    return lhs.GetMediumIndex() < rhs.GetMediumIndex();
                });
            BuildYsonFluently(consumer)
                .DoListFor(replicas, serializePhysicalReplica);
        };

        auto serializeLastSeenReplica = [&] (TFluentList fluent, TNodePtrWithIndexes replica) {
            fluent.Item()
                .BeginAttributes()
                    .DoIf(chunk->IsErasure(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("index").Value(replica.GetReplicaIndex());
                    })
                .EndAttributes()
                .Value(replica.GetPtr()->GetDefaultAddress());
        };

        auto serializeLastSeenReplicas = [&] (IYsonConsumer* consumer, const TNodePtrWithIndexesList& replicas) {
            BuildYsonFluently(consumer)
                .DoListFor(replicas, serializeLastSeenReplica);
        };

        if (!isForeign) {
            if (key == "cached_replicas") {
                TNodePtrWithIndexesList replicas(chunk->CachedReplicas().begin(), chunk->CachedReplicas().end());
                serializePhysicalReplicas(consumer, replicas);
                return true;
            }

            if (key == "stored_replicas") {
                TNodePtrWithIndexesList replicas(chunk->StoredReplicas().begin(), chunk->StoredReplicas().end());
                serializePhysicalReplicas(consumer, replicas);
                return true;
            }

            if (key == "last_seen_replicas") {
                TNodePtrWithIndexesList replicas;
                const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                auto addReplica = [&] (TNodeId nodeId, int replicaIndex) {
                    auto* node = nodeTracker->FindNode(nodeId);
                    if (IsObjectAlive(node)) {
                        replicas.push_back(TNodePtrWithIndexes(node, replicaIndex, DefaultStoreMediumIndex));
                    }
                };
                if (chunk->IsErasure()) {
                    for (int index = 0; index < NErasure::MaxTotalPartCount; ++index) {
                        addReplica(chunk->LastSeenReplicas()[index], index);
                    }
                } else {
                    for (auto nodeId : chunk->LastSeenReplicas()) {
                        addReplica(nodeId, GenericChunkReplicaIndex);
                    }
                }
                std::sort(replicas.begin(), replicas.end());
                replicas.erase(std::unique(replicas.begin(), replicas.end()), replicas.end());
                serializeLastSeenReplicas(consumer, replicas);
                return true;
            }

            if (key == "movable") {
                BuildYsonFluently(consumer)
                    .Value(chunk->GetMovable());
                return true;
            }

            if (key == "vital") {
                BuildYsonFluently(consumer)
                    .Value(chunk->ComputeVital());
                return true;
            }

            if (key == "replication_status") {
                RequireLeader();
                auto statuses = chunkManager->ComputeChunkStatuses(chunk);

                BuildYsonFluently(consumer).DoMapFor(
                    0,
                    MaxMediumCount,
                    [&] (TFluentMap fluent, int mediumIndex) {
                        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                        if (!medium || medium->GetCache()) {
                            return;
                        }

                        auto status = statuses[mediumIndex];

                        fluent
                            .Item(medium->GetName())
                            .BeginMap()
                                .Item("underreplicated").Value(Any(status & EChunkStatus::Underreplicated))
                                .Item("overreplicated").Value(Any(status & EChunkStatus::Overreplicated))
                                .Item("lost").Value(Any(status & EChunkStatus::Lost))
                                .Item("data_missing").Value(Any(status & EChunkStatus::DataMissing))
                                .Item("parity_missing").Value(Any(status & EChunkStatus::ParityMissing))
                                .Item("unsafely_placed").Value(Any(status & EChunkStatus::UnsafelyPlaced))
                            .EndMap();
                    });

                return true;
            }

            if (key == "available") {
                BuildYsonFluently(consumer)
                    .Value(chunk->IsAvailable());
                return true;
            }
        }

        if (key == "media") {
            const auto& properties = chunk->ComputeProperties();
            BuildYsonFluently(consumer)
                .Value(TSerializableChunkProperties(properties, chunkManager));
            return true;
        }

        if (chunk->IsErasure() && key == "erasure_codec") {
            BuildYsonFluently(consumer)
                .Value(chunk->GetErasureCodec());
            return true;
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

        if (key == "exports") {
            const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();
            BuildYsonFluently(consumer)
                .DoMapFor(0, static_cast<int>(cellTags.size()), [&] (TFluentMap fluent, int index) {
                    auto cellTag = cellTags[index];
                    const auto& exportData = chunk->GetExportData(index);
                    if (exportData.RefCounter > 0) {
                        const auto& props = exportData.Properties;
                        fluent
                            .Item(ToString(cellTag)).BeginMap()
                                .Item("ref_counter").Value(exportData.RefCounter)
                                .Item("vital").Value(props.GetVital())
                                .Item("media").Value(TSerializableChunkProperties(props, chunkManager))
                            .EndMap();
                    }
                });
            return true;
        }

        if (key == "sealed") {
            BuildYsonFluently(consumer)
                .Value(chunk->IsSealed());
            return true;
        }

        if (key == "parent_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(chunk->Parents(), [] (TFluentList fluent, const TChunkList* parent) {
                    fluent
                        .Item().Value(parent->GetId());
                });
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

            if (key == "table_chunk_format" && EChunkType(chunk->ChunkMeta().type()) == EChunkType::Table) {
                auto format = ETableChunkFormat(chunk->ChunkMeta().version());
                BuildYsonFluently(consumer)
                    .Value(format);
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

            if (key == "max_block_size" && miscExt.has_max_block_size()) {
                BuildYsonFluently(consumer)
                    .Value(miscExt.max_block_size());
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

            if (key == "creation_time" && miscExt.has_creation_time()) {
                BuildYsonFluently(consumer)
                    .Value(TInstant(miscExt.creation_time()));
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

        if (key == "scan_flags") {
            RequireLeader();
            BuildYsonFluently(consumer)
                .DoMapFor(TEnumTraits<EChunkScanKind>::GetDomainValues(), [&] (TFluentMap fluent, EChunkScanKind kind) {
                    if (kind != EChunkScanKind::None) {
                        fluent
                            .Item(FormatEnum(kind)).Value(chunk->GetScanFlag(kind, objectManager->GetCurrentEpoch()));
                    }
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(const TString& key) override
    {
        auto* chunk = GetThisImpl();

        if (chunk->IsJournal() && key == "quorum_row_count") {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
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

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch)
    {
        Y_UNUSED(request);

        DeclareNonMutating();

        context->SetRequestInfo();

        const auto* chunk = GetThisImpl();

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
};

IObjectProxyPtr CreateChunkProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChunk* chunk)
{
    return New<TChunkProxy>(bootstrap, metadata, chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
