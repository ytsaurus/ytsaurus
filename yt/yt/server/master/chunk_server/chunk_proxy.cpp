#include "chunk_proxy.h"
#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "job.h"
#include "medium.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NProfiling;
using namespace NTableClient;
using namespace NJournalClient;
using namespace NNodeTrackerServer;

using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::NProto::THunkChunkRefsExt;
using NTableClient::NProto::THunkChunkMiscExt;

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
    using TBase = TNonversionedObjectProxyBase<TChunk>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* chunk = GetThisImpl();

        auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();

        bool hasBoundaryKeysExt = chunk->ChunkMeta()->HasExtension<TBoundaryKeysExt>();
        bool hasHunkChunkMiscExt = chunk->ChunkMeta()->HasExtension<THunkChunkMiscExt>();
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkFormat)
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TableChunkFormat)
            .SetPresent(chunk->IsConfirmed() && chunk->GetChunkType() == EChunkType::Table));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MetaSize)
            .SetPresent(miscExt && miscExt->has_meta_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressedDataSize)
            .SetPresent(miscExt && miscExt->has_compressed_data_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UncompressedDataSize)
            .SetPresent(miscExt && miscExt->has_uncompressed_data_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DataWeight)
            .SetPresent(miscExt && miscExt->has_data_weight()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionCodec)
            .SetPresent(miscExt && miscExt->has_compression_codec()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RowCount)
            .SetPresent(miscExt && miscExt->has_row_count()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FirstOverlayedRowIndex)
            .SetPresent(miscExt && miscExt->has_first_overlayed_row_index()));
        descriptors->push_back(EInternedAttributeKey::Overlayed);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxBlockSize)
            .SetPresent(miscExt && miscExt->has_max_block_size()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QuorumInfo)
            .SetPresent(chunk->IsJournal())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Sealed)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ValueCount)
            .SetPresent(miscExt && miscExt->has_value_count()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Sorted)
            .SetPresent(miscExt && miscExt->has_sorted()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinTimestamp)
            .SetPresent(miscExt && miscExt->has_min_timestamp()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxTimestamp)
            .SetPresent(miscExt && miscExt->has_max_timestamp()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagingTransactionId)
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StagingAccount)
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpirationTime)
            .SetPresent(chunk->IsStaged()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinKey)
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxKey)
            .SetPresent(hasBoundaryKeysExt));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReadQuorum)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::WriteQuorum)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicaLagLimit)
            .SetPresent(chunk->IsJournal()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Eden)
            .SetPresent(chunk->IsConfirmed()));
        descriptors->push_back(EInternedAttributeKey::ScanFlags);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CreationTime)
            .SetPresent(miscExt && miscExt->has_creation_time()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Jobs)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PartLossTime)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkChunkRefs)
            .SetPresent(chunk->IsConfirmed())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SharedToSkynet)
            .SetPresent(miscExt && miscExt->has_shared_to_skynet()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkCount)
            .SetPresent(hasHunkChunkMiscExt));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalHunkLength)
            .SetPresent(hasHunkChunkMiscExt));
        descriptors->push_back(EInternedAttributeKey::ApprovedReplicaCount);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EndorsementRequired)
            .SetPresent(chunk->IsBlob()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto* chunk = GetThisImpl();

        auto isForeign = chunk->IsForeign();
        auto isConfirmed = chunk->IsConfirmed();

        auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();

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
                            .Item("state").Value(replica.GetState());
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
                        // NB: Medium index is irrelevant.
                        replicas.push_back(TNodePtrWithIndexes(node, replicaIndex, DefaultStoreMediumIndex));
                    }
                };
                if (chunk->IsErasure()) {
                    for (int index = 0; index < ::NErasure::MaxTotalPartCount; ++index) {
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
                    statuses.begin(),
                    statuses.end(),
                    [&] (TFluentMap fluent, TMediumMap<EChunkStatus>::iterator it) {
                        auto mediumIndex = it->first;
                        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                        if (!medium || medium->GetCache()) {
                            return;
                        }

                        auto status = it->second;

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
                    .Value(chunk->ChunkMeta()->GetExtensionsByteSize());
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
                    .DoListFor(chunk->Parents(), [] (TFluentList fluent, const TChunk::TParents::value_type& pair) {
                        auto [parent, cardinality] = pair;
                        for (auto i = 0; i < cardinality; ++i) {
                            fluent
                                .Item().Value(parent->GetId());
                        }
                    });
                return true;

            case EInternedAttributeKey::DiskSpace:
                if (!isConfirmed) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetDiskSpace());
                return true;

            case EInternedAttributeKey::ChunkType: {
                if (!isConfirmed) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetChunkType());
                return true;
            }

            case EInternedAttributeKey::ChunkFormat: {
                if (!isConfirmed) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetChunkFormat());
                return true;
            }

            case EInternedAttributeKey::TableChunkFormat: {
                if (!isConfirmed || chunk->GetChunkType() != EChunkType::Table) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(static_cast<ETableChunkFormat>(chunk->GetChunkFormat()));
                return true;
            }

            case EInternedAttributeKey::MetaSize:
                if (!miscExt || !miscExt->has_meta_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->meta_size());
                return true;

            case EInternedAttributeKey::CompressedDataSize:
                if (!miscExt || !miscExt->has_compressed_data_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->compressed_data_size());
                return true;

            case EInternedAttributeKey::UncompressedDataSize:
                if (!miscExt || !miscExt->has_uncompressed_data_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->uncompressed_data_size());
                return true;

            case EInternedAttributeKey::DataWeight:
                if (!miscExt || !miscExt->has_data_weight()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->data_weight());
                return true;

            case EInternedAttributeKey::CompressionCodec:
                if (!miscExt || !miscExt->has_compression_codec()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(NCompression::ECodec(miscExt->compression_codec()));
                return true;

            case EInternedAttributeKey::RowCount:
                if (!miscExt || !miscExt->has_row_count()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->row_count());
                return true;

            case EInternedAttributeKey::FirstOverlayedRowIndex:
                if (!miscExt || !miscExt->has_first_overlayed_row_index()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->first_overlayed_row_index());
                return true;

            case EInternedAttributeKey::Overlayed:
                BuildYsonFluently(consumer)
                    .Value(chunk->GetOverlayed());
                return true;

            case EInternedAttributeKey::ValueCount:
                if (!miscExt || !miscExt->has_value_count()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->value_count());
                return true;

            case EInternedAttributeKey::Sorted:
                if (!miscExt || !miscExt->has_sorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->sorted());
                return true;

            case EInternedAttributeKey::MinTimestamp:
                if (!miscExt || !miscExt->has_min_timestamp()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->min_timestamp());
                return true;

            case EInternedAttributeKey::MaxTimestamp:
                if (!miscExt || !miscExt->has_max_timestamp()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->max_timestamp());
                return true;

            case EInternedAttributeKey::MaxBlockSize:
                if (!miscExt || !miscExt->has_max_block_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->max_block_size());
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

            case EInternedAttributeKey::ReplicaLagLimit:
                if (!chunk->IsJournal()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetReplicaLagLimit());
                return true;

            case EInternedAttributeKey::Eden:
                if (!miscExt) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->eden());
                return true;

            case EInternedAttributeKey::CreationTime:
                if (!miscExt || !miscExt->has_creation_time()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(TInstant::MicroSeconds(miscExt->creation_time()));
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

            case EInternedAttributeKey::ExpirationTime:
                if (!chunk->IsStaged()) {
                    break;
                }
                // COMPAT(shakurov)
                // Old staged chunks didn't have expiration time.
                if (!chunk->GetExpirationTime()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetExpirationTime());
                return true;

            case EInternedAttributeKey::MinKey: {
                if (auto boundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(FromProto<TLegacyOwningKey>(boundaryKeysExt->min()));
                    return true;
                }
                break;
            }

            case EInternedAttributeKey::MaxKey: {
                if (auto boundaryKeysExt = chunk->ChunkMeta()->FindExtension<TBoundaryKeysExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(FromProto<TLegacyOwningKey>(boundaryKeysExt->max()));
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

            case EInternedAttributeKey::Jobs: {
                RequireLeader();
                BuildYsonFluently(consumer)
                    .DoListFor(
                        chunk->GetJobs(),
                        [&] (TFluentList fluent, const TJobPtr& job) {
                            fluent
                                .Item()
                                .BeginMap()
                                    .Item("id").Value(job->GetJobId())
                                    .Item("type").Value(job->GetType())
                                    .Item("start_time").Value(job->GetStartTime())
                                    .Item("address").Value(job->GetNode()->GetDefaultAddress())
                                    .Item("state").Value(job->GetState())
                                .EndMap();
                        });
                return true;
            }

            case EInternedAttributeKey::PartLossTime: {
                RequireLeader();
                auto epoch = objectManager->GetCurrentEpoch();
                if (auto partLossTime = chunk->GetPartLossTime(epoch)) {
                    BuildYsonFluently(consumer)
                        .Value(CpuInstantToInstant(*partLossTime));
                } else {
                    BuildYsonFluently(consumer)
                        .Entity();
                }
                return true;
            }

            case EInternedAttributeKey::HunkChunkRefs: {
                if (!isConfirmed) {
                    break;
                }
                std::vector<THunkChunkRef> hunkChunkRefs;
                if (auto hunkChunkRefsExt = chunk->ChunkMeta()->FindExtension<THunkChunkRefsExt>()) {
                    hunkChunkRefs = FromProto<std::vector<THunkChunkRef>>(hunkChunkRefsExt->refs());
                }
                BuildYsonFluently(consumer)
                    .Value(hunkChunkRefs);
                return true;
            }

            case EInternedAttributeKey::SharedToSkynet: {
                if (!miscExt || !miscExt->has_shared_to_skynet()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->shared_to_skynet());
                return true;
            }

            case EInternedAttributeKey::HunkCount:
                if (auto hunkChunkmiscExt = chunk->ChunkMeta()->FindExtension<THunkChunkMiscExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(hunkChunkmiscExt->hunk_count());
                    return true;
                }
                break;

            case EInternedAttributeKey::TotalHunkLength:
                if (auto hunkChunkmiscExt = chunk->ChunkMeta()->FindExtension<THunkChunkMiscExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(hunkChunkmiscExt->total_hunk_length());
                    return true;
                }
                break;

            case EInternedAttributeKey::ApprovedReplicaCount:
                BuildYsonFluently(consumer)
                    .Value(chunk->GetApprovedReplicaCount());
                return true;

            case EInternedAttributeKey::EndorsementRequired:
                if (!chunk->IsBlob()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->GetEndorsementRequired());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* chunk = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::QuorumInfo: {
                if (!chunk->IsJournal()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                return chunkManager->GetChunkQuorumInfo(chunk)
                    .Apply(BIND([] (const TChunkQuorumInfo& info) {
                        return MakeFuture(BuildYsonStringFluently()
                            .BeginMap()
                                .DoIf(info.FirstOverlayedRowIndex.has_value(), [&] (auto fluent) {
                                    fluent
                                        .Item("first_overlayed_row_index").Value(info.FirstOverlayedRowIndex);
                                })
                                .Item("row_count").Value(info.RowCount)
                                .Item("uncompressed_data_size").Value(info.UncompressedDataSize)
                                .Item("compressed_data_size").Value(info.CompressedDataSize)
                            .EndMap());
                    }));
            }

            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, chunk);

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    bool DoInvoke(const NRpc::IServiceContextPtr& context) override
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
        ToProto(chunkSpec->mutable_chunk_meta(), chunk->ChunkMeta());

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

} // namespace NYT::NChunkServer
