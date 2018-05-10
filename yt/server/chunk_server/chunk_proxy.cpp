#include "chunk_proxy.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "medium.h"
#include "helpers.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/multicell_manager.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/node_tracker_server/node.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>

#include <yt/server/object_server/interned_attributes.h>
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

using NYT::FromProto;
using NYT::ToProto;

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

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CachedReplicas)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoredReplicas)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastSeenReplicas)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Movable)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Media)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Vital)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Requisition)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalRequisition)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalRequisitionIndex)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExternalRequisitions)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExternalRequisitionIndexes)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationStatus)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Available)
            .SetPresent(!isForeign));
        descriptors->push_back(EInternedAttributeKey::Confirmed);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ErasureCodec)
            .SetPresent(chunk->IsErasure()));
        descriptors->push_back(EInternedAttributeKey::MasterMetaSize);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentIds)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OwningNodes)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Exports)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DiskSpace)
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkType)
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TableChunkFormat)
            .SetPresent(chunk->IsConfirmed() && EChunkType(chunk->ChunkMeta().type()) == EChunkType::Table));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MetaSize)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_meta_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressedDataSize)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_compressed_data_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UncompressedDataSize)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_uncompressed_data_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DataWeight)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_data_weight()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionCodec)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_compression_codec()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RowCount)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_row_count()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxBlockSize)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_max_block_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QuorumRowCount)
            .SetPresent(chunk->IsJournal())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Sealed)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ValueCount)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_value_count()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Sorted)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_sorted()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinTimestamp)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_min_timestamp()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxTimestamp)
            .SetPresent(chunk->IsConfirmed() && miscExt.has_max_timestamp()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagingTransactionId)
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagingAccount)
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinKey)
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxKey)
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadQuorum)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteQuorum)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Eden)
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(EInternedAttributeKey::ScanFlags);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CreationTime)
            .SetPresent(miscExt.has_creation_time()));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
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

        auto isConfirmed = chunk->IsConfirmed();
        const auto& miscExt = chunk->MiscExt();

        switch (key) {
            case EInternedAttributeKey::CachedReplicas: {
                if (isForeign) {
                    break;
                }
                TNodePtrWithIndexesList replicas(chunk->CachedReplicas().begin(), chunk->CachedReplicas().end());
                serializePhysicalReplicas(consumer, replicas);
                return true;
            }

            case EInternedAttributeKey::StoredReplicas: {
                if (isForeign) {
                    break;
                }
                TNodePtrWithIndexesList replicas(chunk->StoredReplicas().begin(), chunk->StoredReplicas().end());
                serializePhysicalReplicas(consumer, replicas);
                return true;
            }

            case EInternedAttributeKey::LastSeenReplicas: {
                if (isForeign) {
                    break;
                }

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

            case EInternedAttributeKey::Movable:
                if (isForeign) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetMovable());
                return true;

            case EInternedAttributeKey::ReplicationStatus: {
                if (isForeign) {
                    break;
                }

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

            case EInternedAttributeKey::Available:
                if (isForeign) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->IsAvailable());
                return true;

            case EInternedAttributeKey::Vital:
            case EInternedAttributeKey::Media: {
                if (isForeign) {
                    break;
                }

                auto replication = chunk->GetAggregatedReplication(chunkManager->GetChunkRequisitionRegistry());

                if (key == EInternedAttributeKey::Vital) {
                    BuildYsonFluently(consumer)
                        .Value(replication.GetVital());
                } else {
                    BuildYsonFluently(consumer)
                        .Value(TSerializableChunkReplication(replication, chunkManager));
                }
                return true;
            }

            case EInternedAttributeKey::Requisition: {
                if (isForeign) {
                    break;
                }

                auto requisition = chunk->GetAggregatedRequisition(chunkManager->GetChunkRequisitionRegistry());
                BuildYsonFluently(consumer)
                    .Value(TSerializableChunkRequisition(requisition, chunkManager));
                return true;
            }

            case EInternedAttributeKey::LocalRequisition: {
                if (isForeign) {
                    break;
                }

                const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
                const auto& requisition = requisitionRegistry->GetRequisition(chunk->GetLocalRequisitionIndex());
                BuildYsonFluently(consumer)
                    .Value(TSerializableChunkRequisition(requisition, chunkManager));
                return true;
            }

            case EInternedAttributeKey::LocalRequisitionIndex: {
                if (isForeign) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetLocalRequisitionIndex());
                return true;
            }

            case EInternedAttributeKey::ExternalRequisitions: {
                if (isForeign) {
                    break;
                }

                const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
                const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();
                BuildYsonFluently(consumer)
                    .DoMapFor(0, static_cast<int>(cellTags.size()), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(index);
                        if (exportData.RefCounter > 0) {
                            auto requisitionIndex = exportData.ChunkRequisitionIndex;
                            const auto& requisition = requisitionRegistry->GetRequisition(requisitionIndex);
                            fluent
                                .Item(ToString(cellTag)).Value(TSerializableChunkRequisition(requisition, chunkManager));
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::ExternalRequisitionIndexes: {
                if (isForeign) {
                    break;
                }

                const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();
                BuildYsonFluently(consumer)
                    .DoMapFor(0, static_cast<int>(cellTags.size()), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(index);
                        if (exportData.RefCounter > 0) {
                            fluent
                                .Item(ToString(cellTag)).Value(exportData.ChunkRequisitionIndex);
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::ErasureCodec:
                if (!chunk->IsErasure()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetErasureCodec());
                return true;

            case EInternedAttributeKey::Confirmed:
                BuildYsonFluently(consumer)
                    .Value(isConfirmed);
                return true;

            case EInternedAttributeKey::MasterMetaSize:
                BuildYsonFluently(consumer)
                    .Value(chunk->ChunkMeta().ByteSize());
                return true;

            case EInternedAttributeKey::Exports: {
                const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();
                const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
                BuildYsonFluently(consumer)
                    .DoMapFor(0, static_cast<int>(cellTags.size()), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(index);
                        if (exportData.RefCounter > 0) {
                            auto requisitionIndex = exportData.ChunkRequisitionIndex;
                            const auto& replication = requisitionRegistry->GetReplication(requisitionIndex);
                            fluent
                                .Item(ToString(cellTag)).BeginMap()
                                    .Item("ref_counter").Value(exportData.RefCounter)
                                    .Item("vital").Value(replication.GetVital())
                                    .Item("media").Value(TSerializableChunkReplication(replication, chunkManager))
                                .EndMap();
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::Sealed:
                BuildYsonFluently(consumer)
                    .Value(chunk->IsSealed());
                return true;

            case EInternedAttributeKey::ParentIds:
                BuildYsonFluently(consumer)
                    .DoListFor(chunk->Parents(), [] (TFluentList fluent, const TChunkList* parent) {
                        fluent
                            .Item().Value(parent->GetId());
                    });
                return true;

            case EInternedAttributeKey::DiskSpace:
                if (!isConfirmed) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->ChunkInfo().disk_space());
                return true;

            case EInternedAttributeKey::ChunkType: {
                if (!isConfirmed) {
                    break;
                }
                auto type = EChunkType(chunk->ChunkMeta().type());
                BuildYsonFluently(consumer)
                    .Value(type);
                return true;
            }

            case EInternedAttributeKey::TableChunkFormat: {
                if (!isConfirmed || EChunkType(chunk->ChunkMeta().type()) != EChunkType::Table) {
                    break;
                }
                auto format = ETableChunkFormat(chunk->ChunkMeta().version());
                BuildYsonFluently(consumer)
                    .Value(format);
                return true;
            }

            case EInternedAttributeKey::MetaSize:
                if (!isConfirmed || !miscExt.has_meta_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.meta_size());
                return true;

            case EInternedAttributeKey::CompressedDataSize:
                if (!isConfirmed || !miscExt.has_compressed_data_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.compressed_data_size());
                return true;

            case EInternedAttributeKey::UncompressedDataSize:
                if (!isConfirmed || !miscExt.has_uncompressed_data_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.uncompressed_data_size());
                return true;

            case EInternedAttributeKey::DataWeight:
                if (!isConfirmed || !miscExt.has_data_weight()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.data_weight());
                return true;

            case EInternedAttributeKey::CompressionCodec:
                if (!isConfirmed || !miscExt.has_compression_codec()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(NCompression::ECodec(miscExt.compression_codec()));
                return true;

            case EInternedAttributeKey::RowCount:
                if (!isConfirmed || !miscExt.has_row_count()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.row_count());
                return true;

            case EInternedAttributeKey::ValueCount:
                if (!isConfirmed || !miscExt.has_value_count()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.value_count());
                return true;

            case EInternedAttributeKey::Sorted:
                if (!isConfirmed || !miscExt.has_sorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.sorted());
                return true;

            case EInternedAttributeKey::MinTimestamp:
                if (!isConfirmed || !miscExt.has_min_timestamp()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.min_timestamp());
                return true;

            case EInternedAttributeKey::MaxTimestamp:
                if (!isConfirmed || !miscExt.has_max_timestamp()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.max_timestamp());
                return true;

            case EInternedAttributeKey::MaxBlockSize:
                if (!isConfirmed || !miscExt.has_max_block_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.max_block_size());
                return true;

            case EInternedAttributeKey::ReadQuorum:
                if (!isConfirmed || !chunk->IsJournal()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetReadQuorum());
                return true;

            case EInternedAttributeKey::WriteQuorum:
                if (!isConfirmed || !chunk->IsJournal()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetWriteQuorum());
                return true;

            case EInternedAttributeKey::Eden:
                if (!isConfirmed) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt.eden());
                return true;

            case EInternedAttributeKey::CreationTime:
                if (!isConfirmed || !miscExt.has_creation_time()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(TInstant::MicroSeconds(miscExt.creation_time()));
                return true;

            case EInternedAttributeKey::StagingTransactionId:
                if (!chunk->IsStaged()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetStagingTransaction()->GetId());
                return true;

            case EInternedAttributeKey::StagingAccount:
                if (!chunk->IsStaged()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetStagingAccount()->GetName());
                return true;

            case EInternedAttributeKey::MinKey: {
                auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
                if (boundaryKeysExt) {
                    BuildYsonFluently(consumer)
                        .Value(FromProto<TOwningKey>(boundaryKeysExt->min()));
                    return true;
                }
                break;
            }

            case EInternedAttributeKey::MaxKey: {
                auto boundaryKeysExt = FindProtoExtension<TBoundaryKeysExt>(chunk->ChunkMeta().extensions());
                if (boundaryKeysExt) {
                    BuildYsonFluently(consumer)
                        .Value(FromProto<TOwningKey>(boundaryKeysExt->max()));
                    return true;
                }
                break;
            }

            case EInternedAttributeKey::ScanFlags:
                RequireLeader();
                BuildYsonFluently(consumer)
                    .DoMapFor(TEnumTraits<EChunkScanKind>::GetDomainValues(), [&] (TFluentMap fluent, EChunkScanKind kind) {
                        if (kind != EChunkScanKind::None) {
                            fluent
                                .Item(FormatEnum(kind)).Value(chunk->GetScanFlag(kind, objectManager->GetCurrentEpoch()));
                        }
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* chunk = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::QuorumRowCount: {
                if (!chunk->IsJournal()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                auto rowCountResult = chunkManager->GetChunkQuorumInfo(chunk);
                return rowCountResult.Apply(BIND([=] (const TMiscExt& miscExt) {
                    return MakeFuture(ConvertToYsonString(miscExt.row_count()));
                }));
            }

            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, chunk);

            default:
                break;
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
