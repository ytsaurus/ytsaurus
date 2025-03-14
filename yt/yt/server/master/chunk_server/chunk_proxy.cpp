#include "chunk_proxy.h"

#include "private.h"
#include "chunk.h"
#include "chunk_manager.h"
#include "chunk_reincarnator.h"
#include "chunk_replicator.h"
#include "job.h"
#include "job_registry.h"
#include "domestic_medium.h"
#include "helpers.h"
#include "chunk_replica_fetcher.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/incumbent_server/incumbent_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/account.h>
#include <yt/yt/server/master/security_server/helpers.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_owner_ypath.pb.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NObjectServer;
using namespace NChunkClient;
using namespace NIncumbentClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NTableClient;
using namespace NJournalClient;
using namespace NNodeTrackerServer;
using namespace NConcurrency;
using namespace NSecurityServer;
using namespace NCellMaster;
using namespace NServer;

using NChunkClient::NProto::TMiscExt;
using NTableClient::NProto::TBoundaryKeysExt;
using NTableClient::NProto::THunkChunkRefsExt;
using NTableClient::NProto::THunkChunkMiscExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr bool IsLeaderChunkScanKind(EChunkScanKind kind)
{
    return
        kind == EChunkScanKind::Seal ||
        kind == EChunkScanKind::Reincarnation ||
        kind == EChunkScanKind::GlobalStatisticsCollector;
}

constexpr bool IsReplicatorChunkScanKind(EChunkScanKind kind)
{
    return kind == EChunkScanKind::Refresh || kind == EChunkScanKind::RequisitionUpdate;
}

static_assert(TEnumTraits<EChunkScanKind>::GetDomainSize() == 6);
static_assert(static_cast<int>(EChunkScanKind::None) == 0);
static_assert(IsReplicatorChunkScanKind(EChunkScanKind::Refresh));
static_assert(IsReplicatorChunkScanKind(EChunkScanKind::RequisitionUpdate));
static_assert(IsLeaderChunkScanKind(EChunkScanKind::Seal));
static_assert(IsLeaderChunkScanKind(EChunkScanKind::Reincarnation));
static_assert(IsLeaderChunkScanKind(EChunkScanKind::GlobalStatisticsCollector));

////////////////////////////////////////////////////////////////////////////////

class TChunkProxy
    : public TNonversionedObjectProxyBase<TChunk>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

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
        const auto& chunkSchema = chunk->Schema();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoredReplicas)
            .SetPresent(!isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoredMasterReplicas)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoredSequoiaReplicas)
            .SetPresent(!isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastSeenReplicas)
            .SetPresent(!isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnapprovedSequoiaReplicas)
            .SetPresent(!isForeign)
            .SetOpaque(true));
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
            .SetPresent(!isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalReplicationStatus)
            .SetPresent(!isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkReplicatorAddress)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ShardIndex)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Available)
            .SetPresent(!isForeign));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HistoricallyNonVital)
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CompressionDictionaryId)
            .SetPresent(miscExt && miscExt->has_compression_dictionary_id()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RowCount)
            .SetPresent(miscExt && miscExt->has_row_count()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FirstOverlayedRowIndex)
            .SetPresent(miscExt && miscExt->has_first_overlayed_row_index()));
        descriptors->push_back(EInternedAttributeKey::Overlayed);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxBlockSize)
            .SetPresent(miscExt && miscExt->has_max_data_block_size()));
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ScanFlags)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalScanFlags)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CreationTime)
            .SetPresent(miscExt && miscExt->has_creation_time()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Jobs)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalJobs)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PartLossTime)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LocalPartLossTime)
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ConsistentReplicaPlacementHash)
            .SetPresent(chunk->HasConsistentReplicaPlacementHash()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ConsistentReplicaPlacement)
            .SetPresent(chunk->HasConsistentReplicaPlacementHash() && !isForeign)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StripedErasure)
            .SetPresent(chunk->IsBlob() && chunk->IsErasure() && chunk->IsConfirmed()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Schema)
            .SetPresent(chunkSchema.operator bool())
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SchemaId)
            .SetPresent(chunkSchema.operator bool()));
        descriptors->emplace_back(EInternedAttributeKey::ScheduleReincarnation)
            .SetWritable(!isForeign)
            .SetPresent(false);
    }

    void SerializeReplica(
        TFluentList fluent,
        const IChunkManagerPtr& chunkManager,
        TChunkId chunkId,
        const TNode* node,
        const TChunkLocation* location,
        int replicaIndex,
        EChunkReplicaState replicaState,
        int mediumIndex)
    {
        auto* medium = chunkManager->GetMediumByIndex(mediumIndex);
        fluent.Item()
            .BeginAttributes()
                .Item("medium").Value(medium->GetName())
                .DoIf(location, [&] (TFluentMap fluent) {
                    fluent
                        .Item("location_uuid").Value(location->GetUuid());
                })
                .DoIf(node->IsDecommissioned(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("decommissioned").Value(true);
                })
                .DoIf(IsErasureChunkId(chunkId), [&] (TFluentMap fluent) {
                    fluent
                        .Item("index").Value(replicaIndex);
                })
                .DoIf(IsJournalChunkId(chunkId), [&] (TFluentMap fluent) {
                    fluent
                        .Item("state").Value(replicaState);
                })
            .EndAttributes()
            .Value(node->GetDefaultAddress());
    };

    void BuildYsonReplicas(
        IYsonConsumer* consumer,
        const IChunkManagerPtr& chunkManager,
        TChunkId chunkId,
        TChunkLocationPtrWithReplicaInfoList replicas)
    {
        SortBy(replicas, [] (TChunkLocationPtrWithReplicaInfo replica) {
            return std::tuple(replica.GetReplicaIndex(), replica.GetPtr()->GetEffectiveMediumIndex());
        });

        BuildYsonFluently(consumer)
            .DoListFor(replicas, [&] (TFluentList fluent, TChunkLocationPtrWithReplicaInfo replica) {
                const auto* location = replica.GetPtr();
                SerializeReplica(
                    fluent,
                    chunkManager,
                    chunkId,
                    location->GetNode(),
                    location,
                    replica.GetReplicaIndex(),
                    replica.GetReplicaState(),
                    location->GetEffectiveMediumIndex());
            });
    };

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto* chunk = GetThisImpl();

        auto isForeign = chunk->IsForeign();
        auto isConfirmed = chunk->IsConfirmed();

        auto miscExt = chunk->ChunkMeta()->FindExtension<TMiscExt>();

        switch (key) {
            case EInternedAttributeKey::StoredMasterReplicas: {
                if (isForeign) {
                    break;
                }

                auto masterReplicas = chunk->StoredReplicas();
                TChunkLocationPtrWithReplicaInfoList replicaList(masterReplicas.begin(), masterReplicas.end());
                BuildYsonReplicas(consumer, chunkManager, chunk->GetId(), replicaList);
                return true;
            }

            case EInternedAttributeKey::Movable:
                if (isForeign) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetMovable());
                return true;

            case EInternedAttributeKey::ChunkReplicatorAddress: {
                if (isForeign) {
                    break;
                }

                RequireLeader();

                const auto& incumbentManager = Bootstrap_->GetIncumbentManager();
                auto address = incumbentManager->GetIncumbentAddress(
                    EIncumbentType::ChunkReplicator,
                    chunk->GetShardIndex());
                BuildYsonFluently(consumer)
                    .Value(address);
                return true;
            }

            case EInternedAttributeKey::ShardIndex: {
                if (isForeign) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetShardIndex());
                return true;
            }

            case EInternedAttributeKey::Available:
                if (isForeign) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(chunk->IsAvailable());
                return true;

            case EInternedAttributeKey::HistoricallyNonVital: {
                if (isForeign) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetHistoricallyNonVital());
                return true;
            }

            case EInternedAttributeKey::Vital:
            case EInternedAttributeKey::Media: {
                if (isForeign) {
                    break;
                }

                const auto& replication = chunk->GetAggregatedReplication(chunkManager->GetChunkRequisitionRegistry());

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
                    .DoMapFor(0, std::ssize(cellTags), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(cellTag);
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
                    .DoMapFor(0, std::ssize(cellTags), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(cellTag);
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
                    .DoMapFor(0, std::ssize(cellTags), [&] (TFluentMap fluent, int index) {
                        auto cellTag = cellTags[index];
                        const auto exportData = chunk->GetExportData(cellTag);
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

            case EInternedAttributeKey::CompressionDictionaryId:
                if (!miscExt || !miscExt->has_compression_dictionary_id()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(FromProto<TChunkId>(miscExt->compression_dictionary_id()));
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
                if (!miscExt || !miscExt->has_max_data_block_size()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(miscExt->max_data_block_size());
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
                    .Value(chunk->StagingAccount()->GetName());
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

            case EInternedAttributeKey::LocalScanFlags: {
                BuildYsonFluently(consumer)
                    .DoMapFor(TEnumTraits<EChunkScanKind>::GetDomainValues(), [&] (TFluentMap fluent, EChunkScanKind kind) {
                        if (kind == EChunkScanKind::None) {
                            return;
                        }

                        YT_ASSERT(IsLeaderChunkScanKind(kind) || IsReplicatorChunkScanKind(kind));
                        if (IsLeaderChunkScanKind(kind) && !IsLeader()) {
                            return;
                        }

                        if (IsReplicatorChunkScanKind(kind) && !chunkReplicator->ShouldProcessChunk(chunk)) {
                            return;
                        }

                        fluent
                            .Item(FormatEnum(kind)).Value(chunk->GetScanFlag(kind));
                    });
                return true;
            }

            case EInternedAttributeKey::LocalJobs: {
                // NB: This attribute is requested from all peers.

                auto list = BuildYsonFluently(consumer).BeginList();
                auto addJob = [&] (const TJobPtr& job) {
                    list.Item().BeginMap()
                        .Item("id").Value(job->GetJobId())
                        .Item("type").Value(job->GetType())
                        .Item("start_time").Value(job->GetStartTime())
                        .Item("address").Value(job->NodeAddress())
                        .Item("state").Value(job->GetState())
                        .Item("epoch").Value(job->GetJobEpoch())
                        .Item("origin").Value(NNet::GetLocalHostName())
                    .EndMap();
                };
                for (const auto& job : chunk->GetJobs()) {
                    addJob(job);
                }
                const auto& jobRegistry = chunkManager->GetJobRegistry();
                if (auto lastFinishedJob = jobRegistry->FindLastFinishedJob(chunk->GetId())) {
                    addJob(lastFinishedJob);
                }
                list.EndList();
                return true;
            }

            case EInternedAttributeKey::LocalPartLossTime: {
                if (auto partLossTime = chunk->GetPartLossTime()) {
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
                if (auto hunkChunkMiscExt = chunk->ChunkMeta()->FindExtension<THunkChunkMiscExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(hunkChunkMiscExt->hunk_count());
                    return true;
                }
                break;

            case EInternedAttributeKey::TotalHunkLength:
                if (auto hunkChunkMiscExt = chunk->ChunkMeta()->FindExtension<THunkChunkMiscExt>()) {
                    BuildYsonFluently(consumer)
                        .Value(hunkChunkMiscExt->total_hunk_length());
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

            case EInternedAttributeKey::ConsistentReplicaPlacementHash:
                if (!chunk->HasConsistentReplicaPlacementHash()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetConsistentReplicaPlacementHash());
                return true;

            case EInternedAttributeKey::ConsistentReplicaPlacement: {
                if (isForeign) {
                    break;
                }
                if (!chunk->HasConsistentReplicaPlacementHash()) {
                    break;
                }

                auto replicas = chunkManager->GetConsistentChunkReplicas(chunk);
                SortBy(replicas, [] (TNodePtrWithReplicaInfoAndMediumIndex replica) {
                    return std::tuple(replica.GetReplicaIndex(), replica.GetMediumIndex());
                });
                BuildYsonFluently(consumer)
                    .DoListFor(replicas, [&] (TFluentList fluent, TNodePtrWithReplicaInfoAndMediumIndex replica) {
                        SerializeReplica(
                            fluent,
                            chunkManager,
                            chunk->GetId(),
                            replica.GetPtr(),
                            /*location*/ nullptr,
                            replica.GetReplicaIndex(),
                            replica.GetReplicaState(),
                            replica.GetMediumIndex());
                    });
                return true;
            }

            case EInternedAttributeKey::StripedErasure: {
                if (!chunk->IsBlob() || !chunk->IsErasure() || !chunk->IsConfirmed()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunk->GetStripedErasure());
                return true;
            }

            case EInternedAttributeKey::SchemaId: {
                const auto& chunkSchema = chunk->Schema();
                if (!chunkSchema) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(chunkSchema->GetId());
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& cellManager = Bootstrap_->GetCellManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();
        const auto& chunkReplicator = chunkManager->GetChunkReplicator();

        auto* chunk = GetThisImpl();

        auto isForeign = chunk->IsForeign();

        auto requestAttributeFromAllPeers = [&] (const TString& attributeSuffix) {
            std::vector<TFuture<TIntrusivePtr<TObjectYPathProxy::TRspGet>>> responseFutures;
            responseFutures.reserve(cellManager->GetTotalPeerCount());

            for (int peerIndex = 0; peerIndex < cellManager->GetTotalPeerCount(); ++peerIndex) {
                auto peerChannel = cellManager->GetPeerChannel(peerIndex);
                auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(peerChannel));
                auto req = TYPathProxy::Get(FromObjectId(chunk->GetId()) + attributeSuffix);
                responseFutures.push_back(proxy.Execute(req));
            }

            return responseFutures;
        };

        auto requestAttributeFromChunkReplicator = [&] (const TString& attributeSuffix) {
            auto replicatorChannel = chunkManager->GetChunkReplicatorChannelOrThrow(chunk);
            auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(replicatorChannel));
            auto req = TYPathProxy::Get(FromObjectId(chunk->GetId()) + attributeSuffix);
            return proxy.Execute(req)
                .Apply(BIND([] (const TIntrusivePtr<TObjectYPathProxy::TRspGet>& rsp) {
                    return MakeFuture(TYsonString{rsp->value()});
                }));
        };

        switch (key) {
            case EInternedAttributeKey::QuorumInfo: {
                if (!chunk->IsJournal()) {
                    break;
                }
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

            case EInternedAttributeKey::LocalReplicationStatus: {
                if (isForeign) {
                    break;
                }

                return chunkReplicaFetcher->GetChunkReplicasAsync({TEphemeralObjectPtr<TChunk>(chunk)})
                    .Apply(BIND([=, this_ = MakeStrong(this)] (const TChunkLocationPtrWithReplicaInfoList& replicas) {
                        auto statuses = chunkReplicator->ComputeChunkStatuses(chunk, replicas);

                        return BuildYsonStringFluently().DoMapFor(
                            statuses.begin(),
                            statuses.end(),
                            [&] (auto fluent, auto it) {
                                auto mediumIndex = it->first;
                                auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                                if (!IsObjectAlive(medium)) {
                                    return;
                                }

                                auto status = it->second;

                                fluent
                                    .Item(medium->GetName())
                                    .BeginMap()
                                        .Item("underreplicated").Value(Any(status & EChunkStatus::Underreplicated))
                                        .Item("overreplicated").Value(Any(status & EChunkStatus::Overreplicated))
                                        .Item("unexpected_overreplicated").Value(Any(status & EChunkStatus::UnexpectedOverreplicated))
                                        .Item("lost").Value(Any(status & EChunkStatus::Lost))
                                        .Item("data_missing").Value(Any(status & EChunkStatus::DataMissing))
                                        .Item("parity_missing").Value(Any(status & EChunkStatus::ParityMissing))
                                        .Item("unsafely_placed").Value(Any(status & EChunkStatus::UnsafelyPlaced))
                                        .Item("temporarily_unavailable").Value(Any(status & EChunkStatus::TemporarilyUnavailable))
                                    .EndMap();
                            });
                    }));

            }

            case EInternedAttributeKey::OwningNodes:
                return GetMulticellOwningNodes(Bootstrap_, chunk);

            case EInternedAttributeKey::ScanFlags: {
                return AllSet(requestAttributeFromAllPeers("/@local_scan_flags"))
                    .Apply(BIND([] (const std::vector<TErrorOr<TIntrusivePtr<TObjectYPathProxy::TRspGet>>>& rspOrErrors) {
                        auto builder = BuildYsonStringFluently().BeginMap();
                        THashSet<EChunkScanKind> seenScanKinds;
                        for (const auto& rspOrError : rspOrErrors) {
                            if (!rspOrError.IsOK()) {
                                YT_LOG_DEBUG(rspOrError, "Failed to request chunk scan flags from peer");
                                continue;
                            }

                            auto chunkScanFlags = ConvertTo<THashMap<EChunkScanKind, bool>>(TYsonString{rspOrError.Value()->value()});
                            for (auto [scanKind, flag] : chunkScanFlags) {
                                if (seenScanKinds.insert(scanKind).second) {
                                    builder.Item(FormatEnum(scanKind)).Value(flag);
                                }
                            }
                        }
                        return MakeFuture(builder.EndMap());
                    }));
            }

            case EInternedAttributeKey::Jobs: {
                return AllSet(requestAttributeFromAllPeers("/@local_jobs"))
                    .Apply(BIND([] (const std::vector<TErrorOr<TIntrusivePtr<TObjectYPathProxy::TRspGet>>>& rspOrErrors) {
                        auto builder = BuildYsonStringFluently().BeginList();
                        for (const auto& rspOrError : rspOrErrors) {
                            if (!rspOrError.IsOK()) {
                                YT_LOG_DEBUG(rspOrError, "Failed to request chunk jobs from peer");
                                continue;
                            }

                            auto jobs = ConvertTo<IListNodePtr>(TYsonString{rspOrError.Value()->value()});
                            for (const auto& job : jobs->GetChildren()) {
                                builder.Item().Value(job);
                            }
                        }
                        return MakeFuture(builder.EndList());
                    }));
            }

            case EInternedAttributeKey::PartLossTime:
                return requestAttributeFromChunkReplicator("/@local_part_loss_time");

            case EInternedAttributeKey::ReplicationStatus:
                if (isForeign) {
                    break;
                }
                return requestAttributeFromChunkReplicator("/@local_replication_status");

            case EInternedAttributeKey::Schema: {
                const auto& chunkSchema = chunk->Schema();
                if (!chunkSchema) {
                    break;
                }

                return chunkSchema->AsYsonAsync();
            }

            case EInternedAttributeKey::StoredReplicas: {
                if (isForeign) {
                    break;
                }

                auto chunkId = chunk->GetId();
                return chunkReplicaFetcher->GetChunkReplicasAsync({TEphemeralObjectPtr<TChunk>(chunk)})
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TChunkLocationPtrWithReplicaInfoList& replicas) {
                        return BuildYsonStringFluently()
                            .Do([&] (auto fluent) {
                                BuildYsonReplicas(fluent.GetConsumer(), chunkManager, chunkId, replicas);
                            });
                    }));
            }

            case EInternedAttributeKey::StoredSequoiaReplicas: {
                if (isForeign) {
                    break;
                }

                auto chunkId = chunk->GetId();
                return chunkReplicaFetcher->GetOnlySequoiaChunkReplicas({chunk->GetId()})
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const THashMap<TChunkId, TChunkLocationPtrWithReplicaInfoList>& replicas) {
                        auto it = replicas.find(chunkId);
                        const auto& chunkReplicas = it != replicas.end() ? it->second : TChunkLocationPtrWithReplicaInfoList();
                        return BuildYsonStringFluently()
                            .Do([&] (auto fluent) {
                                BuildYsonReplicas(fluent.GetConsumer(), chunkManager, chunkId, chunkReplicas);
                            });
                    }));
            }

            case EInternedAttributeKey::LastSeenReplicas: {
                if (isForeign) {
                    break;
                }

                auto isErasure = chunk->IsErasure();
                auto chunkId = chunk->GetId();
                return chunkReplicaFetcher->GetLastSeenReplicas(TEphemeralObjectPtr<TChunk>(chunk)).Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TNodeId>& lastSeenReplicas) {
                    TNodePtrWithReplicaIndexList replicas;
                    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                    auto addReplica = [&] (TNodeId nodeId, int replicaIndex) {
                        auto* node = nodeTracker->FindNode(nodeId);
                        if (IsObjectAlive(node)) {
                            replicas.emplace_back(node, replicaIndex);
                        }
                    };

                    if (isErasure) {
                        if (std::ssize(lastSeenReplicas) != ::NErasure::MaxTotalPartCount) {
                            auto error = TError("Unexpected last seen replicas size %v for erasure chunk %v",
                                std::ssize(lastSeenReplicas),
                                chunkId);
                            YT_LOG_ALERT(error);
                            THROW_ERROR_EXCEPTION(error);
                        }
                        for (int index = 0; index < ::NErasure::MaxTotalPartCount; ++index) {
                            addReplica(lastSeenReplicas[index], index);
                        }
                    } else {
                        for (auto nodeId : lastSeenReplicas) {
                            addReplica(nodeId, GenericChunkReplicaIndex);
                        }
                    }

                    SortUnique(replicas);
                    return BuildYsonStringFluently()
                        .DoListFor(replicas, [&] (TFluentList fluent, TNodePtrWithReplicaIndex replica) {
                            fluent.Item()
                                .BeginAttributes()
                                    .DoIf(isErasure, [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("index").Value(replica.GetReplicaIndex());
                                    })
                                    .DoIf(replica.GetPtr()->IsDecommissioned(), [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("decommissioned").Value(true);
                                    })
                                .EndAttributes()
                                .Value(replica.GetPtr()->GetDefaultAddress());
                        });
                }));
            }

            case EInternedAttributeKey::UnapprovedSequoiaReplicas: {
                if (isForeign) {
                    break;
                }

                return chunkReplicaFetcher->GetUnapprovedSequoiaChunkReplicas({chunk->GetId()})
                    .Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TSequoiaChunkReplica>& rawReplicas) {
                        TNodePtrWithReplicaIndexList replicas;
                        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
                        for (const auto& rawReplica : rawReplicas) {
                            auto* node = nodeTracker->FindNode(rawReplica.NodeId);
                            if (IsObjectAlive(node)) {
                                replicas.emplace_back(node, rawReplica.ReplicaIndex);
                            }
                        }

                        return BuildYsonStringFluently()
                            .DoListFor(replicas, [&] (TFluentList fluent, TNodePtrWithReplicaIndex replica) {
                                fluent.Item()
                                    .Value(replica.GetPtr()->GetDefaultAddress());
                            });
                        }));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        switch (key) {
            case EInternedAttributeKey::ScheduleReincarnation: {
                auto* chunk = GetThisImpl<TChunk>();
                if (chunk->IsForeign()) {
                    THROW_ERROR_EXCEPTION("Reincarnation can be scheduled for native chunks only");
                }

                ValidateSuperuserOnAttributeModification(Bootstrap_->GetSecurityManager(), key.Unintern());

                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& chunkReincarnator = chunkManager->GetChunkReincarnator();
                chunkReincarnator->ScheduleReincarnation(chunk, ConvertTo<TChunkReincarnationOptions>(value));

                return true;
            }
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Fetch);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NChunkClient::NProto, Fetch)
    {
        Y_UNUSED(request);

        DeclareNonMutating();

        context->SetRequestInfo();

        auto chunk = TEphemeralObjectPtr<TChunk>(GetThisImpl());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();

        // This is context switch, chunk may die.
        auto replicas = chunkReplicaFetcher->GetChunkReplicas(chunk)
            .ValueOrThrow();

        TNodeDirectoryBuilder nodeDirectoryBuilder(response->mutable_node_directory());
        nodeDirectoryBuilder.Add(replicas);

        auto* chunkSpec = response->add_chunks();
        ToProto(chunkSpec->mutable_replicas(), replicas);
        ToProto(chunkSpec->mutable_chunk_id(), chunk->GetId());
        chunkSpec->set_erasure_codec(ToProto(chunk->GetErasureCodec()));
        chunkSpec->set_striped_erasure(chunk->GetStripedErasure());
        ToProto(chunkSpec->mutable_chunk_meta(), chunk->ChunkMeta());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateChunkProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChunk* chunk)
{
    return New<TChunkProxy>(bootstrap, metadata, chunk);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
