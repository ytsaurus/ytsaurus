#include "table_node_proxy_detail.h"

#include "helpers.h"
#include "master_table_schema.h"
#include "mount_config_attributes.h"
#include "private.h"
#include "replicated_table_node.h"
#include "secondary_index.h"
#include "table_collocation.h"
#include "table_manager.h"
#include "table_node.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_visitor.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/tablet_server/backup_manager.h>
#include <yt/yt/server/master/tablet_server/chaos_helpers.h>
#include <yt/yt/server/master/tablet_server/config.h>
#include <yt/yt/server/master/tablet_server/mount_config_storage.h>
#include <yt/yt/server/master/tablet_server/tablet.h>
#include <yt/yt/server/master/tablet_server/tablet_cell.h>
#include <yt/yt/server/master/tablet_server/table_replica.h>
#include <yt/yt/server/master/tablet_server/tablet_manager.h>
#include <yt/yt/server/master/tablet_server/hunk_storage_node.h>

#include <yt/yt/server/master/security_server/access_log.h>
#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/table_settings.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/tablet_balancer/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/tablet_client/backup.h>
#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/async_consumer.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NTableServer {

using namespace NApi;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NQueueClient;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TTableNodeProxy::GetBasicAttributes(TGetBasicAttributesContext* context)
{
    if (context->Permission == EPermission::Read) {
        // We shall take care of reads ourselves.
        TPermissionCheckOptions checkOptions;
        auto* table = GetThisImpl();
        if (context->Columns) {
            checkOptions.Columns = std::move(context->Columns);
        } else {
            const auto& tableSchema = *table->GetSchema()->AsTableSchema();
            checkOptions.Columns.emplace();
            checkOptions.Columns->reserve(tableSchema.Columns().size());
            for (const auto& columnSchema : tableSchema.Columns()) {
                checkOptions.Columns->push_back(columnSchema.Name());
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        auto checkResponse = securityManager->CheckPermission(
            Object_,
            user,
            EPermission::Read,
            checkOptions);

        if (checkResponse.Action == ESecurityAction::Deny) {
            TPermissionCheckTarget target;
            target.Object = Object_;
            securityManager->LogAndThrowAuthorizationError(
                target,
                user,
                EPermission::Read,
                checkResponse);
        }

        if (checkOptions.Columns) {
            for (size_t index = 0; index < checkOptions.Columns->size(); ++index) {
                const auto& column = (*checkOptions.Columns)[index];
                const auto& result = (*checkResponse.Columns)[index];
                if (result.Action == ESecurityAction::Deny) {
                    if (context->OmitInaccessibleColumns) {
                        if (!context->OmittedInaccessibleColumns) {
                            context->OmittedInaccessibleColumns.emplace();
                        }
                        context->OmittedInaccessibleColumns->push_back(column);
                    } else {
                        TPermissionCheckTarget target;
                        target.Object = Object_;
                        target.Column = column;
                        securityManager->LogAndThrowAuthorizationError(
                            target,
                            user,
                            EPermission::Read,
                            result);
                    }
                }
            }
        }

        // No need for an extra check below.
        context->Permission = std::nullopt;
    }

    TBase::GetBasicAttributes(context);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();
    bool isDynamic = table->IsDynamic();
    bool isSorted = table->IsSorted();
    bool isExternal = table->IsExternal();
    bool isQueue = table->IsQueue();
    bool isConsumer = table->IsConsumer();

    TBase::DoListSystemAttributes(descriptors, /*showTabletAttributes*/ isDynamic);

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkRowCount));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RowCount)
        .SetPresent(!isDynamic));
    // TODO(savrus) remove "unmerged_row_count" in 20.0
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnmergedRowCount)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(EInternedAttributeKey::Sorted);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::KeyColumns)
        .SetReplicated(true));
    // TODO(shakurov): make @schema opaque (in favor of @schema_id)?
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Schema)
        .SetReplicated(true));
    descriptors->push_back(EInternedAttributeKey::SchemaId);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SchemaDuplicateCount));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SortedBy)
        .SetPresent(isSorted));
    descriptors->push_back(EInternedAttributeKey::Dynamic);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastCommitTimestamp)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tablets)
        .SetExternal(isExternal)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletPerformanceCounters)
        .SetExternal(isExternal)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKeys)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && isSorted)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RetainedTimestamp)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnflushedTimestamp)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Atomicity)
        .SetReplicated(true)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CommitOrdering)
        .SetWritable(true)
        .SetPresent(!isSorted)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OptimizeFor)
        .SetReplicated(true)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkFormat)
        .SetPresent(table->TryGetChunkFormat().has_value())
        .SetReplicated(true)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OptimizeForStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SchemaMode));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkWriter)
        .SetCustom(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UpstreamReplicaId)
        .SetExternal(isExternal)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationCardId)
        .SetWritable(true)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && trunkTable->GetReplicationCardId()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationProgress)
        .SetExternal(isExternal)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TableChunkFormatStatistics)
        .SetExternal(isExternal)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkStatistics)
        .SetExternal(isExternal && isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableTabletBalancer)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DisableTabletBalancer)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetMinTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetMaxTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DesiredTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DesiredTabletCount)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletCount())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ForcedCompactionRevision)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetForcedCompactionRevision())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ForcedStoreCompactionRevision)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetForcedStoreCompactionRevision())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ForcedHunkCompactionRevision)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetForcedHunkCompactionRevision())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ForcedChunkViewCompactionRevision)
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true)
        .SetPresent(static_cast<bool>(table->GetForcedChunkViewCompactionRevision())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FlushLagTime)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletBalancerConfig)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BoundaryKeys)
        .SetExternal(isExternal)
        .SetOpaque(true)
        .SetPresent(isSorted && !isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableDynamicStoreRead)
        .SetWritable(true)
        .SetRemovable(true)
        .SetExternal(isExternal)
        .SetPresent(isDynamic || trunkTable->GetEnableDynamicStoreRead().has_value()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PreloadState)
        .SetExternal(isExternal)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ProfilingMode)
        .SetWritable(true)
        .SetReplicated(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetProfilingMode())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ProfilingTag)
        .SetWritable(true)
        .SetReplicated(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetProfilingTag())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableDetailedProfiling)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationCollocationId)
        .SetPresent(table->IsReplicated() && trunkTable->GetReplicationCollocation())
        .SetWritable(true)
        .SetRemovable(true)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationCollocationTablePaths)
        .SetPresent(table->IsReplicated() && trunkTable->GetReplicationCollocation())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BackupState)
        .SetExternal(isExternal)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BackupCheckpointTimestamp)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && table->GetBackupState() == ETableBackupState::BackupCompleted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletBackupState)
        .SetExternal(isExternal)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::BackupError)
        .SetExternal(isExternal)
        .SetPresent(isDynamic && !trunkTable->BackupError().IsOK()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableConsistentChunkReplicaPlacement)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DataWeight)
        .SetPresent(table->HasDataWeight()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueAgentStage)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueStatus)
        .SetPresent(isQueue)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueuePartitions)
        .SetPresent(isQueue)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TreatAsQueueConsumer)
        .SetWritable(true)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueConsumerStatus)
        .SetPresent(isConsumer)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::QueueConsumerPartitions)
        .SetPresent(isConsumer)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::VitalQueueConsumer)
        .SetWritable(true)
        .SetPresent(isConsumer));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MountConfig)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EffectiveMountConfig)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::HunkStorageNode)
        .SetWritable(true)
        .SetReplicated(true)
        .SetRemovable(true)
        .SetPresent(table->GetHunkStorageNode()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AssignedMountConfigExperiments)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SecondaryIndices)
        .SetPresent(isDynamic && !table->SecondaryIndices().empty()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::IndexTo)
        .SetPresent(isDynamic && table->GetIndexTo()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableSharedWriteLocks)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(isDynamic && isSorted));
}

bool TTableNodeProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();
    auto statistics = table->ComputeTotalStatistics();
    bool isDynamic = table->IsDynamic();
    bool isSorted = table->IsSorted();
    bool isExternal = table->IsExternal();
    bool isConsumer = table->IsConsumer();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    switch (key) {
        case EInternedAttributeKey::DataWeight: {
            if (!table->HasDataWeight()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->ComputeTotalStatistics().data_weight());
            return true;
        }

        case EInternedAttributeKey::ChunkRowCount:
            BuildYsonFluently(consumer)
                .Value(statistics.row_count());
            return true;

        case EInternedAttributeKey::RowCount:
            if (isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(statistics.row_count());
            return true;

        case EInternedAttributeKey::UnmergedRowCount:
            if (!isDynamic || !isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(statistics.row_count());
            return true;

        case EInternedAttributeKey::Sorted:
            BuildYsonFluently(consumer)
                .Value(table->GetSchema()->AsTableSchema()->IsSorted());
            return true;

        case EInternedAttributeKey::KeyColumns:
            BuildYsonFluently(consumer)
                .Value(table->GetSchema()->AsTableSchema()->GetKeyColumns());
            return true;

        case EInternedAttributeKey::SchemaId: {
            auto* schema = table->GetSchema();
            BuildYsonFluently(consumer)
                .Value(schema->GetId());
            return true;
        }

        case EInternedAttributeKey::SchemaDuplicateCount: {
            auto* schema = table->GetSchema();
            BuildYsonFluently(consumer)
                .Value(schema->GetObjectRefCounter());
            return true;
        }

        case EInternedAttributeKey::SchemaMode:
            BuildYsonFluently(consumer)
                .Value(table->GetSchemaMode());
            return true;

        case EInternedAttributeKey::SortedBy:
            if (!isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->GetSchema()->AsTableSchema()->GetKeyColumns());
            return true;

        case EInternedAttributeKey::Dynamic:
            BuildYsonFluently(consumer)
                .Value(trunkTable->IsDynamic());
            return true;

        case EInternedAttributeKey::LastCommitTimestamp:
            if (!isDynamic || !isSorted || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetLastCommitTimestamp());
            return true;

        case EInternedAttributeKey::TabletPerformanceCounters:
            if (!isDynamic || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTabletBase* tabletBase) {
                    auto* tablet = tabletBase->As<TTablet>();
                    fluent
                        .Item().BeginMap()
                            .Item("tablet_id").Value(tablet->GetId())
                            .Item("performance_counters").Value(tablet->PerformanceCounters())
                        .EndMap();
                });
            return true;

        case EInternedAttributeKey::Tablets: {
            if (!isDynamic || isExternal) {
                break;
            }
            auto addPerfCounters = Bootstrap_->GetConfigManager()->GetConfig()
                ->TabletManager->AddPerfCountersToTabletsAttribute;
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTabletBase* tabletBase) {
                    auto* tablet = tabletBase->As<TTablet>();
                    auto* cell = tablet->GetCell();
                    auto* node = tabletManager->FindTabletLeaderNode(tablet);
                    fluent
                        .Item().BeginMap()
                            .Item("index").Value(tablet->GetIndex())
                            .DoIf(addPerfCounters, [&] (TFluentMap fluent) {
                                fluent
                                    .Item("performance_counters").Value(tablet->PerformanceCounters());
                            })
                            .DoIf(table->IsSorted(), [&] (TFluentMap fluent) {
                                fluent
                                    .Item("pivot_key").Value(tablet->GetPivotKey());
                            })
                            .DoIf(!table->IsPhysicallySorted(), [&] (TFluentMap fluent) {
                                const auto* chunkList = tablet->GetChunkList();
                                fluent
                                    .Item("trimmed_row_count").Value(tablet->GetTrimmedRowCount())
                                    .Item("flushed_row_count").Value(chunkList->Statistics().LogicalRowCount);
                            })
                            .Item("state").Value(tablet->GetState())
                            .Item("last_commit_timestamp").Value(tablet->NodeStatistics().last_commit_timestamp())
                            .Item("statistics").Value(New<TSerializableTabletStatistics>(
                                tablet->GetTabletStatistics(),
                                chunkManager))
                            .Item("tablet_id").Value(tablet->GetId())
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("cell_id").Value(cell->GetId());
                            })
                            .DoIf(node, [&] (TFluentMap fluent) {
                                fluent.Item("cell_leader_address").Value(node->GetDefaultAddress());
                            })
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("mount_time").Value(tablet->Servant().GetMountTime());
                            })
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("mount_revision").Value(tablet->Servant().GetMountRevision());
                            })
                            .Item("error_count").Value(tablet->GetTabletErrorCount())
                            .Item("replication_error_count").Value(tablet->GetReplicationErrorCount())
                        .EndMap();
                });
            return true;
        }

        case EInternedAttributeKey::PivotKeys:
            if (!isDynamic || !isSorted || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTabletBase* tablet) {
                    fluent
                        .Item().Value(tablet->As<TTablet>()->GetPivotKey());
                });
            return true;

        case EInternedAttributeKey::RetainedTimestamp:
            if (!isDynamic || !isSorted || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->GetCurrentRetainedTimestamp());
            return true;

        case EInternedAttributeKey::UnflushedTimestamp:
            if (!isDynamic || !isSorted || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->GetCurrentUnflushedTimestamp(timestampProvider->GetLatestTimestamp()));
            return true;

        case EInternedAttributeKey::Atomicity:
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetAtomicity());
            return true;

        case EInternedAttributeKey::CommitOrdering:
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetCommitOrdering());
            return true;

        case EInternedAttributeKey::OptimizeFor:
            BuildYsonFluently(consumer)
                .Value(table->GetOptimizeFor());
            return true;

        case EInternedAttributeKey::ChunkFormat:
            if (auto optionalChunkFormat = table->TryGetChunkFormat()) {
                BuildYsonFluently(consumer)
                    .Value(*optionalChunkFormat);
                return true;
            }
            break;

        case EInternedAttributeKey::HunkErasureCodec:
            BuildYsonFluently(consumer)
                .Value(table->GetHunkErasureCodec());
            return true;

        case EInternedAttributeKey::UpstreamReplicaId:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetUpstreamReplicaId());
            return true;

        case EInternedAttributeKey::ReplicationCardId:
            if (!isDynamic || !trunkTable->GetReplicationCardId()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetReplicationCardId());
            return true;

        case EInternedAttributeKey::ReplicationProgress:
            if (!isDynamic || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(GatherReplicationProgress(trunkTable));
            return true;

        case EInternedAttributeKey::EnableTabletBalancer:
            if (!static_cast<bool>(trunkTable->GetEnableTabletBalancer())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetEnableTabletBalancer());
            return true;

        case EInternedAttributeKey::DisableTabletBalancer:
            if (!static_cast<bool>(trunkTable->GetEnableTabletBalancer())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(!*trunkTable->GetEnableTabletBalancer());
            return true;

        case EInternedAttributeKey::MinTabletSize:
            if (!static_cast<bool>(trunkTable->GetMinTabletSize())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetMinTabletSize());
            return true;

        case EInternedAttributeKey::MaxTabletSize:
            if (!static_cast<bool>(trunkTable->GetMaxTabletSize())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetMaxTabletSize());
            return true;

        case EInternedAttributeKey::DesiredTabletSize:
            if (!static_cast<bool>(trunkTable->GetDesiredTabletSize())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetDesiredTabletSize());
            return true;

        case EInternedAttributeKey::DesiredTabletCount:
            if (!static_cast<bool>(trunkTable->GetDesiredTabletCount())) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetDesiredTabletCount());
            return true;

        case EInternedAttributeKey::ForcedCompactionRevision:
            if (!trunkTable->GetForcedCompactionRevision()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetForcedCompactionRevision());
            return true;

        case EInternedAttributeKey::ForcedStoreCompactionRevision:
            if (!trunkTable->GetForcedStoreCompactionRevision()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetForcedStoreCompactionRevision());
            return true;

        case EInternedAttributeKey::ForcedHunkCompactionRevision:
            if (!trunkTable->GetForcedHunkCompactionRevision()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetForcedHunkCompactionRevision());
            return true;

        case EInternedAttributeKey::ForcedChunkViewCompactionRevision:
            if (!trunkTable->GetForcedChunkViewCompactionRevision()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*trunkTable->GetForcedChunkViewCompactionRevision());
            return true;

        case EInternedAttributeKey::FlushLagTime: {
            if (!isSorted || !isDynamic || isExternal) {
                break;
            }
            auto unflushedTimestamp = table->GetCurrentUnflushedTimestamp(
                timestampProvider->GetLatestTimestamp());
            auto lastCommitTimestamp = trunkTable->GetLastCommitTimestamp();

            // NB: Proper order is not guaranteed.
            auto duration = TDuration::Zero();
            if (unflushedTimestamp <= lastCommitTimestamp) {
                duration = NTransactionClient::TimestampDiffToDuration(
                    unflushedTimestamp,
                    lastCommitTimestamp)
                    .second;
            }

            BuildYsonFluently(consumer)
                .Value(duration);
            return true;
        }

        case EInternedAttributeKey::TabletBalancerConfig:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->TabletBalancerConfig());
            return true;

        case EInternedAttributeKey::BoundaryKeys: {
            if (!isSorted || isDynamic || isExternal) {
                break;
            }

            const auto* table = GetThisImpl();
            const auto* chunkList = table->GetChunkList();

            BuildYsonFluently(consumer)
                .BeginMap()
                    .DoIf(!IsEmpty(chunkList), [&] (TFluentMap fluent) {
                        fluent
                            .Item("min_key").Value(GetMinKeyOrThrow(chunkList))
                            .Item("max_key").Value(GetMaxKeyOrThrow(chunkList));
                    })
                .EndMap();

            return true;
        }

        case EInternedAttributeKey::EnableDynamicStoreRead:
            if (isExternal) {
                break;
            }

            if (isDynamic) {
                bool value;

                if (auto explicitValue = trunkTable->GetEnableDynamicStoreRead()) {
                    value = *explicitValue;
                } else if (trunkTable->GetTabletState() == NTabletClient::ETabletState::Unmounted) {
                    value = Bootstrap_->GetConfigManager()->GetConfig()
                        ->TabletManager->EnableDynamicStoreReadByDefault;
                } else {
                    value = trunkTable->GetMountedWithEnabledDynamicStoreRead();
                }

                BuildYsonFluently(consumer)
                    .Value(value);
                return true;
            } else {
                auto explicitValue = trunkTable->GetEnableDynamicStoreRead();
                if (explicitValue) {
                    BuildYsonFluently(consumer)
                        .Value(*explicitValue);
                    return true;
                }

                break;
            }

        case EInternedAttributeKey::PreloadState: {
            if (!isDynamic || isExternal) {
                break;
            }

            auto statistics = trunkTable->GetTabletStatistics();

            EStorePreloadState preloadState;

            if (statistics.PreloadFailedStoreCount > 0) {
                preloadState = EStorePreloadState::Failed;
            } else if (statistics.PreloadPendingStoreCount > 0) {
                preloadState = EStorePreloadState::Running;
            } else {
                preloadState = EStorePreloadState::Complete;
            }

            BuildYsonFluently(consumer)
                .Value(preloadState);

            return true;
        }

        case EInternedAttributeKey::ProfilingMode:
            if (!table->GetProfilingMode()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*table->GetProfilingMode());
            return true;

        case EInternedAttributeKey::ProfilingTag:
            if (!table->GetProfilingTag()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(*table->GetProfilingTag());
            return true;

        case EInternedAttributeKey::EnableDetailedProfiling:
            if (!isDynamic) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetEnableDetailedProfiling());
            return true;

        case EInternedAttributeKey::ReplicationCollocationTablePaths: {
            if (!isDynamic || !table->IsReplicated() || !trunkTable->GetReplicationCollocation()) {
                break;
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();
            auto* collocation = trunkTable->GetReplicationCollocation();

            BuildYsonFluently(consumer)
                .DoListFor(collocation->Tables(), [&] (TFluentList fluent, TTableNode* table) {
                    if (!IsObjectAlive(table)) {
                        return;
                    }
                    fluent
                        .Item().Value(cypressManager->GetNodePath(table, nullptr));
                });

            return true;
        }

        case EInternedAttributeKey::ReplicationCollocationId: {
            if (!isDynamic || !table->IsReplicated() || !trunkTable->GetReplicationCollocation()) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(trunkTable->GetReplicationCollocation()->GetId());

            return true;
        }

        case EInternedAttributeKey::BackupState:
            if (!isDynamic || isExternal) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(trunkTable->GetBackupState());
            return true;

        case EInternedAttributeKey::BackupCheckpointTimestamp:
            if (!isDynamic || isExternal || table->GetBackupState() != ETableBackupState::BackupCompleted) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetBackupCheckpointTimestamp());
            return true;

        case EInternedAttributeKey::TabletBackupState:
            if (!isDynamic || isExternal) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(trunkTable->GetAggregatedTabletBackupState());
            return true;

        case EInternedAttributeKey::BackupError:
            if (!isDynamic || isExternal || trunkTable->BackupError().IsOK()) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(trunkTable->BackupError());
            return true;

        case EInternedAttributeKey::EnableConsistentChunkReplicaPlacement:
            if (!isDynamic) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetEnableConsistentChunkReplicaPlacement());
            return true;

        case EInternedAttributeKey::QueueAgentStage:
            if (!isDynamic) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(GetEffectiveQueueAgentStage(Bootstrap_, table->GetQueueAgentStage()));
            return true;

        case EInternedAttributeKey::TreatAsQueueConsumer:
            if (!isDynamic || !isSorted) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetTreatAsConsumer());
            return true;

        case EInternedAttributeKey::VitalQueueConsumer:
            if (!isConsumer) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetIsVitalConsumer());
            return true;

        case EInternedAttributeKey::MountConfig:
            if (const auto* storage = table->FindMountConfigStorage()) {
                Serialize(storage, consumer);
            } else {
                BuildYsonFluently(consumer)
                    .BeginMap()
                    .EndMap();
            }

            return true;

        case EInternedAttributeKey::EffectiveMountConfig: {
            const auto* storage = table->FindMountConfigStorage();
            auto config = storage
                ? storage->GetEffectiveConfig()
                : New<TCustomTableMountConfig>();
            Serialize(config, consumer);

            return true;
        }

        case EInternedAttributeKey::HunkStorageNode: {
            const auto& hunkStorageNode = table->GetHunkStorageNode();
            if (!hunkStorageNode) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(hunkStorageNode->GetId());
            return true;
        }

        case EInternedAttributeKey::AssignedMountConfigExperiments: {
            if (!table->IsDynamic()) {
                return false;
            }

            const auto& configManager = Bootstrap_->GetConfigManager();
            const auto& experiments = configManager->GetConfig()->TabletManager->TableConfigExperiments;
            NTabletNode::TTableConfigExperiment::TTableDescriptor descriptor{
                .TableId = table->GetId(),
                .TablePath = GetPath(),
                .TabletCellBundle = table->TabletCellBundle()->GetName(),
                .InMemoryMode = table->GetInMemoryMode(),
                .Sorted = table->IsSorted(),
                .Replicated = table->IsReplicated(),
            };

            BuildYsonFluently(consumer)
                .DoMapFor(experiments, [&] (auto fluent, const auto& pair) {
                    if (pair.second->Matches(descriptor)) {
                        fluent
                            .Item(pair.first).Value(pair.second);
                    }
                });

            return true;
        }

        case EInternedAttributeKey::SecondaryIndices: {
            if (!table->IsDynamic() || table->SecondaryIndices().empty()) {
                return false;
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();

            BuildYsonFluently(consumer)
                .DoMapFor(
                    table->SecondaryIndices(),
                    [&] (auto fluent, TSecondaryIndex* secondaryIndex) {
                        auto indexPath = cypressManager->GetNodePath(
                            secondaryIndex->GetIndexTable(),
                            /*transaction*/ nullptr);
                        auto kind = secondaryIndex->GetKind();

                        fluent
                            .Item(ToString(secondaryIndex->GetId()))
                            .BeginMap()
                                .Item("index_path").Value(indexPath)
                                .Item("kind").Value(kind)
                            .EndMap();
                    });

            return true;
        }

        case EInternedAttributeKey::IndexTo: {
            if (!table->IsDynamic() || !table->GetIndexTo()) {
                return false;
            }

            auto* secondaryIndex = table->GetIndexTo();
            auto tablePath = Bootstrap_->GetCypressManager()->GetNodePath(
                secondaryIndex->GetTable(),
                /*transaction*/ nullptr);
            auto kind = secondaryIndex->GetKind();

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("index_id").Value(secondaryIndex->GetId())
                    .Item("table_path").Value(tablePath)
                    .Item("kind").Value(kind)
                .EndMap();

            return true;
        }

        case EInternedAttributeKey::EnableSharedWriteLocks:
            if (!isDynamic || !isSorted) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(table->GetEnableSharedWriteLocks());
            return true;

        default:
            break;
    }

    return TBase::DoGetBuiltinAttribute(key, consumer, /*showTabletAttributes*/ isDynamic);
}

TFuture<TYsonString> TTableNodeProxy::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    const auto* table = GetThisImpl();
    auto chunkLists = table->GetChunkLists();
    bool isExternal = table->IsExternal();
    bool isQueue = table->IsQueue();
    bool isConsumer = table->IsConsumer();

    switch (key) {
        case EInternedAttributeKey::TableChunkFormatStatistics:
            if (isExternal) {
                break;
            }
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) -> std::optional<ETableChunkFormat> {
                    if (chunk->GetChunkType() != EChunkType::Table) {
                        return std::nullopt;
                    }
                    return static_cast<ETableChunkFormat>(chunk->GetChunkFormat());
                });

        case EInternedAttributeKey::OptimizeForStatistics: {
            if (isExternal) {
                break;
            }

            return ComputeChunkStatistics(
                Bootstrap_,
                chunkLists,
                [] (const TChunk* chunk) -> std::optional<EOptimizeFor> {
                    if (chunk->GetChunkType() != EChunkType::Table) {
                        return std::nullopt;
                    }
                    return OptimizeForFromFormat(chunk->GetChunkFormat());
                });
        }

        case EInternedAttributeKey::HunkStatistics: {
            if (isExternal) {
                break;
            }
            if (!table->IsDynamic()) {
                break;
            }
            return ComputeHunkStatistics(Bootstrap_, chunkLists);
        }

        case EInternedAttributeKey::Schema:
            return table->GetSchema()->AsYsonAsync();

        case EInternedAttributeKey::QueueStatus:
        case EInternedAttributeKey::QueuePartitions: {
            if (!isQueue) {
                break;
            }
            return GetQueueAgentAttributeAsync(Bootstrap_, table->GetQueueAgentStage(), GetPath(), key);
        }

        case EInternedAttributeKey::QueueConsumerStatus:
        case EInternedAttributeKey::QueueConsumerPartitions: {
            if (!isConsumer) {
                break;
            }
            return GetQueueAgentAttributeAsync(Bootstrap_, table->GetQueueAgentStage(), GetPath(), key);
        }

        default:
            break;
    }

    return TBase::GetBuiltinAttributeAsync(key);
}

bool TTableNodeProxy::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::EnableTabletBalancer: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::DisableTabletBalancer: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::MinTabletSize: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMinTabletSize(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::MaxTabletSize: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMaxTabletSize(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::DesiredTabletSize: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletSize(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::DesiredTabletCount: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletCount(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ForcedCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedCompactionRevision(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ForcedStoreCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedStoreCompactionRevision(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ForcedHunkCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedHunkCompactionRevision(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ForcedChunkViewCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedChunkViewCompactionRevision(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::EnableDynamicStoreRead: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            if (lockedTable->IsPhysicallyLog() && !lockedTable->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Dynamic store read is not supported for table type %Qlv",
                    lockedTable->GetType());
            }
            if (lockedTable->IsDynamic()) {
                lockedTable->ValidateAllTabletsUnmounted("Cannot change dynamic stores readability");
                lockedTable->ValidateNotBackup("Cannot change dynamic stores readability");
            }

            lockedTable->SetEnableDynamicStoreRead(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ProfilingMode: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetProfilingMode(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ProfilingTag: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetProfilingTag(std::nullopt);
            return true;
        }

        case EInternedAttributeKey::ReplicationCollocationId: {
            ValidateNoTransaction();

            const auto& cypressManager = Bootstrap_->GetCypressManager();

            auto* lockedTable = LockThisImpl();
            if (auto* collocation = lockedTable->GetReplicationCollocation()) {
                YT_VERIFY(lockedTable->IsDynamic() && lockedTable->IsReplicated());
                const auto& tableManager = Bootstrap_->GetTableManager();

                if (lockedTable->GetIndexTo()) {
                    THROW_ERROR_EXCEPTION("Cannot remove table %v from collocation due to it being a secondary index",
                        cypressManager->GetNodePath(lockedTable, nullptr));
                } else if (!lockedTable->SecondaryIndices().empty()) {
                    THROW_ERROR_EXCEPTION("Cannot remove table %v from collocation due to it having a secondary index",
                        cypressManager->GetNodePath(lockedTable, nullptr));
                }

                tableManager->RemoveTableFromCollocation(
                    lockedTable,
                    collocation);
            }

            return true;
        }

        case EInternedAttributeKey::HunkStorageNode: {
            auto* lockedTable = LockThisImpl();
            lockedTable->ResetHunkStorageNode();
            return true;
        }

        case EInternedAttributeKey::QueueAgentStage: {
            ValidateNoTransaction();
            auto* lockedTable = LockThisImpl();
            lockedTable->SetQueueAgentStage(std::nullopt);
            return true;
        }

        default:
            break;
    }

    return TBase::RemoveBuiltinAttribute(key);
}

bool TTableNodeProxy::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    auto* table = GetThisImpl();

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    const auto& tableManager = Bootstrap_->GetTableManager();
    auto revision = hydraManager->GetAutomatonVersion().ToRevision();

    switch (key) {
        case EInternedAttributeKey::Atomicity: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->ValidateAllTabletsUnmounted("Cannot change table atomicity mode");

            auto atomicity = ConvertTo<NTransactionClient::EAtomicity>(value);
            if (table->IsPhysicallyLog() && atomicity != NTransactionClient::EAtomicity::Full) {
                THROW_ERROR_EXCEPTION("Table of type %Qlv only support %Qlv atomicity, cannot set it to %Qlv",
                    table->GetType(),
                    NTransactionClient::EAtomicity::Full,
                    atomicity);
            }
            lockedTable->SetAtomicity(atomicity);

            return true;
        }

        case EInternedAttributeKey::CommitOrdering: {
            if (table->IsSorted()) {
                break;
            }
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->ValidateAllTabletsUnmounted("Cannot change table commit ordering mode");

            auto ordering = ConvertTo<NTransactionClient::ECommitOrdering>(value);
            if (table->IsPhysicallyLog() && ordering != NTransactionClient::ECommitOrdering::Strong) {
                THROW_ERROR_EXCEPTION("Table of type %Qlv only support %Qlv commit ordering, cannot set it to %Qlv",
                    table->GetType(),
                    NTransactionClient::ECommitOrdering::Strong,
                    ordering);
            }

            lockedTable->SetCommitOrdering(ordering);

            return true;
        }

        case EInternedAttributeKey::OptimizeFor: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto* lockedTable = LockThisImpl(TLockRequest::MakeSharedAttribute(uninternedKey));
            lockedTable->SetOptimizeFor(ConvertTo<EOptimizeFor>(value));
            lockedTable->OnRemountNeeded();

            return true;
        }

        case EInternedAttributeKey::ChunkFormat: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = key.Unintern();
            auto* lockedTable = LockThisImpl(TLockRequest::MakeSharedAttribute(uninternedKey));
            auto chunkFormat = ConvertTo<EChunkFormat>(value);
            ValidateTableChunkFormat(chunkFormat);
            lockedTable->SetChunkFormat(chunkFormat);

            return true;
        }

        case EInternedAttributeKey::HunkErasureCodec: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            auto codecId = ConvertTo<NErasure::ECodec>(value);
            if (codecId != NErasure::ECodec::None) {
                auto* codec = NErasure::GetCodec(codecId);
                if (!codec->IsBytewise()) {
                    THROW_ERROR_EXCEPTION("%Qlv codec is not suitable for erasure hunks",
                        codecId);
                }
            }

            const auto& uninternedKey = key.Unintern();
            auto* lockedTable = LockThisImpl(TLockRequest::MakeSharedAttribute(uninternedKey));
            lockedTable->SetHunkErasureCodec(codecId);
            lockedTable->OnRemountNeeded();

            return true;
        }

        case EInternedAttributeKey::EnableTabletBalancer: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(ConvertTo<bool>(value));
            return true;
        }

        case EInternedAttributeKey::DisableTabletBalancer: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(!ConvertTo<bool>(value));
            return true;
        }

        case EInternedAttributeKey::MinTabletSize: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetMinTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::MaxTabletSize: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetMaxTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::DesiredTabletSize: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::DesiredTabletCount: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletCount(ConvertTo<int>(value));
            return true;
        }

        case EInternedAttributeKey::ForcedCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedCompactionRevision(revision);
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::ForcedStoreCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedStoreCompactionRevision(revision);
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::ForcedHunkCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedHunkCompactionRevision(revision);
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::ForcedChunkViewCompactionRevision: {
            ValidateNoTransaction();
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetForcedChunkViewCompactionRevision(revision);
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::TabletBalancerConfig: {
            if (!table->IsDynamic()) {
                break;
            }
            ValidateNoTransaction();

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->MutableTabletBalancerConfig() = ConvertTo<NTabletBalancer::TMasterTableTabletBalancerConfigPtr>(value);
            return true;
        }

        case EInternedAttributeKey::EnableDynamicStoreRead: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            if (lockedTable->IsPhysicallyLog() && !lockedTable->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Dynamic store read is not supported for table type %Qlv",
                    lockedTable->GetType());
            }
            if (lockedTable->IsDynamic()) {
                lockedTable->ValidateAllTabletsUnmounted("Cannot change dynamic stores readability");
                lockedTable->ValidateNotBackup("Cannot change dynamic stores readability");
            }

            lockedTable->SetEnableDynamicStoreRead(ConvertTo<bool>(value));
            return true;
        }

        case EInternedAttributeKey::ProfilingMode: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetProfilingMode(ConvertTo<EDynamicTableProfilingMode>(value));
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::ProfilingTag: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetProfilingTag(ConvertTo<TString>(value));
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::EnableDetailedProfiling: {
            if (!table->IsDynamic()) {
                break;
            }

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetEnableDetailedProfiling(ConvertTo<bool>(value));
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::ReplicationCollocationId: {
            ValidateNoTransaction();

            auto collocationId = ConvertTo<TTableCollocationId>(value);

            auto* lockedTable = LockThisImpl();
            if (!lockedTable->IsDynamic() || !lockedTable->IsReplicated()) {
                break;
            }

            if (!Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->ReplicateTableCollocations) {
                auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
                if (primaryCellTag != table->GetNativeCellTag()) {
                    // TODO(akozhikhov): Support portals with collocation.
                    THROW_ERROR_EXCEPTION("Unexpected native cell tag for table %v: found %v, expected %v",
                        table->GetId(),
                        table->GetNativeCellTag(),
                        primaryCellTag);
                }

                if (!Bootstrap_->GetMulticellManager()->IsPrimaryMaster()) {
                    return true;
                }
            }

            const auto& tableManager = Bootstrap_->GetTableManager();
            auto* collocation = tableManager->GetTableCollocationOrThrow(collocationId);
            tableManager->AddTableToCollocation(
                lockedTable,
                collocation);
            return true;
        }

        case EInternedAttributeKey::EnableConsistentChunkReplicaPlacement: {
            ValidateNoTransaction();

            if (!table->IsDynamic()) {
                break;
            }

            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableConsistentChunkReplicaPlacement(ConvertTo<bool>(value));
            lockedTable->OnRemountNeeded();
            return true;
        }

        case EInternedAttributeKey::QueueAgentStage: {
            ValidateNoTransaction();

            if (!table->IsDynamic()) {
                break;
            }

            auto* lockedTable = LockThisImpl();
            lockedTable->SetQueueAgentStage(ConvertTo<TString>(value));

            SetModified(EModificationType::Attributes);

            return true;
        }

        case EInternedAttributeKey::TreatAsQueueConsumer: {
            ValidateNoTransaction();

            if (!table->IsDynamic() || !table->IsSorted()) {
                break;
            }

            auto* lockedTable = LockThisImpl();
            auto isConsumerObjectBefore = lockedTable->IsTrackedConsumerObject();
            lockedTable->SetTreatAsConsumer(ConvertTo<bool>(value));
            auto isConsumerObjectAfter = lockedTable->IsTrackedConsumerObject();

            if (isConsumerObjectAfter != isConsumerObjectBefore) {
                if (isConsumerObjectAfter) {
                    tableManager->RegisterConsumer(table);
                } else {
                    tableManager->UnregisterConsumer(table);
                }
            }

            SetModified(EModificationType::Attributes);

            return true;
        }

        case EInternedAttributeKey::VitalQueueConsumer: {
            ValidateNoTransaction();

            if (!table->IsConsumer()) {
                break;
            }

            auto* lockedTable = LockThisImpl();
            lockedTable->SetIsVitalConsumer(ConvertTo<bool>(value));

            SetModified(EModificationType::Attributes);

            return true;
        }

        case EInternedAttributeKey::MountConfig: {
            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            auto* storage = lockedTable->GetMutableMountConfigStorage();
            storage->SetSelf(value);
            lockedTable->OnRemountNeeded();

            return true;
        }

        case EInternedAttributeKey::HunkStorageNode: {
            if (!table->IsDynamic()) {
                break;
            }

            auto* lockedTable = LockThisImpl();

            auto path = ConvertTo<TYPath>(value);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto* node = objectManager->ResolvePathToObject(path, /*transaction*/ nullptr, /*options*/ {});
            if (node->GetType() != EObjectType::HunkStorage) {
                THROW_ERROR_EXCEPTION("Unexpected node type: expected %Qlv, got %Qlv",
                    EObjectType::HunkStorage,
                    node->GetType())
                    << TErrorAttribute("path", path);
            }

            auto* hunkStorageNode = node->As<THunkStorageNode>();
            lockedTable->SetHunkStorageNode(hunkStorageNode);

            return true;
        }

        case EInternedAttributeKey::EnableSharedWriteLocks: {
            if (!table->IsDynamic()) {
                break;
            }

            auto lockRequest = TLockRequest::MakeSharedAttribute(key.Unintern());
            auto* lockedTable = LockThisImpl(lockRequest);
            lockedTable->SetEnableSharedWriteLocks(ConvertTo<bool>(value));
            return true;
        }

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value, force);
}

void TTableNodeProxy::ValidateCustomAttributeUpdate(
    const TString& key,
    const TYsonString& oldValue,
    const TYsonString& newValue)
{
    auto internedKey = TInternedAttributeKey::Lookup(key);

    switch (internedKey) {
        case EInternedAttributeKey::ChunkWriter:
            if (!newValue) {
                break;
            }
            ConvertTo<NTabletNode::TTabletStoreWriterConfigPtr>(newValue);
            return;

        case EInternedAttributeKey::HunkChunkWriter:
            if (!newValue) {
                break;
            }
            ConvertTo<NTabletNode::TTabletHunkWriterConfigPtr>(newValue);
            return;

        case EInternedAttributeKey::ChunkReader:
            if (!newValue) {
                break;
            }
            ConvertTo<NTabletNode::TTabletStoreReaderConfigPtr>(newValue);
            return;

        case EInternedAttributeKey::HunkChunkReader:
            if (!newValue) {
                break;
            }
            ConvertTo<NTabletNode::TTabletHunkReaderConfigPtr>(newValue);
            return;

        default:
            break;
    }

    TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
}

void TTableNodeProxy::ValidateReadLimit(const NChunkClient::NProto::TReadLimit& readLimit) const
{
    auto* table = GetThisImpl();
    if ((readLimit.has_key_bound_prefix() || readLimit.has_legacy_key()) && !table->IsSorted()) {
        THROW_ERROR_EXCEPTION("Key selectors are not supported for unsorted tables");
    }
    if (readLimit.has_tablet_index()) {
        if (!table->IsDynamic() || table->IsSorted()) {
            THROW_ERROR_EXCEPTION("Tablet index selectors are only supported for ordered dynamic tables");
        }
    }
    if (table->IsDynamic() && !table->IsSorted()) {
        if (readLimit.has_row_index() && !readLimit.has_tablet_index()) {
            THROW_ERROR_EXCEPTION("In ordered dynamic tables row index selector can only be specified when tablet index selector is also specified");
        }
    }
    if (readLimit.has_row_index() && table->IsDynamic() && table->IsSorted()) {
        THROW_ERROR_EXCEPTION("Row index selectors are not supported for sorted dynamic tables");
    }
    if (readLimit.has_offset()) {
        THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
    }
}

TComparator TTableNodeProxy::GetComparator() const
{
    auto* schema = GetThisImpl()->GetSchema();
    return schema->AsTableSchema()->ToComparator();
}

bool TTableNodeProxy::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(ReshardAutomatic);
    DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
    DISPATCH_YPATH_SERVICE_METHOD(Alter);
    DISPATCH_YPATH_SERVICE_METHOD(LockDynamicTable);
    DISPATCH_YPATH_SERVICE_METHOD(CheckDynamicTableLock);
    DISPATCH_YPATH_SERVICE_METHOD(StartBackup);
    DISPATCH_YPATH_SERVICE_METHOD(StartRestore);
    DISPATCH_YPATH_SERVICE_METHOD(CheckBackup);
    DISPATCH_YPATH_SERVICE_METHOD(FinishBackup);
    DISPATCH_YPATH_SERVICE_METHOD(FinishRestore);
    return TBase::DoInvoke(context);
}

void TTableNodeProxy::ValidateBeginUpload()
{
    TBase::ValidateBeginUpload();
    const auto* table = GetThisImpl();

    if (table->IsDynamic() && !table->GetSchema()->AsTableSchema()->IsSorted()) {
        THROW_ERROR_EXCEPTION("Cannot upload into ordered dynamic table");
    }

    if (table->IsDynamic() && !Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->EnableBulkInsert) {
        THROW_ERROR_EXCEPTION("Bulk insert is disabled");
    }
}

void TTableNodeProxy::ValidateStorageParametersUpdate()
{
    TChunkOwnerNodeProxy::ValidateStorageParametersUpdate();

    const auto* table = GetThisImpl();
    table->ValidateAllTabletsUnmounted("Cannot change storage parameters");
}

void TTableNodeProxy::ValidateLockPossible()
{
    TChunkOwnerNodeProxy::ValidateLockPossible();

    const auto* table = GetThisImpl();
    table->ValidateTabletStateFixed("Cannot lock table");
}

TTableNode* TTableNodeProxy::GetThisImpl()
{
    return TBase::GetThisImpl<TTableNode>();
}

const TTableNode* TTableNodeProxy::GetThisImpl() const
{
    return TBase::GetThisImpl<TTableNode>();
}

TTableNode* TTableNodeProxy::LockThisImpl(
    const TLockRequest& request,
    bool recursive)
{
    return TBase::LockThisImpl<TTableNode>(request, recursive);
}

IAttributeDictionary* TTableNodeProxy::GetCustomAttributes()
{
    if (!WrappedAttributes_) {
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->TabletManager;
        WrappedAttributes_ = New<TMountConfigAttributeDictionary>(
            Bootstrap_,
            GetThisImpl(),
            Transaction_,
            TBase::GetCustomAttributes(),
            config->IncludeMountConfigAttributesInUserAttributes);
    }
    return WrappedAttributes_.Get();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, ReshardAutomatic)
{
    DeclareMutating();

    bool keepActions = request->keep_actions();

    context->SetRequestInfo("KeepActions: %v", keepActions);

    ValidateNoTransaction();

    auto* trunkTable = GetThisImpl();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    auto tabletActions = tabletManager->SyncBalanceTablets(trunkTable, keepActions);
    ToProto(response->mutable_tablet_actions(), tabletActions);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, GetMountInfo)
{
    DeclareNonMutating();
    SuppressAccessTracking();

    context->SetRequestInfo();

    ValidateNotExternal();
    ValidateNoTransaction();

    const auto* trunkTable = GetThisImpl();

    ToProto(response->mutable_table_id(), trunkTable->GetId());
    response->set_dynamic(trunkTable->IsDynamic());
    ToProto(response->mutable_upstream_replica_id(), trunkTable->GetUpstreamReplicaId());
    ToProto(response->mutable_schema(), *trunkTable->GetSchema()->AsTableSchema());
    response->set_enable_detailed_profiling(trunkTable->GetEnableDetailedProfiling());
    response->set_enable_shared_write_locks(trunkTable->GetEnableSharedWriteLocks());

    THashSet<TTabletCell*> cells;
    for (const auto* tabletBase : trunkTable->Tablets()) {
        auto* tablet = tabletBase->As<TTablet>();
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->add_tablets();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        protoTablet->set_mount_revision(tablet->Servant().GetMountRevision());
        protoTablet->set_state(ToProto<int>(tablet->GetState()));
        protoTablet->set_in_memory_mode(ToProto<int>(tablet->GetInMemoryMode()));
        ToProto(protoTablet->mutable_pivot_key(), tablet->GetPivotKey());
        if (cell) {
            ToProto(protoTablet->mutable_cell_id(), cell->GetId());
            cells.insert(cell);
        }
    }

    for (const auto* cell : cells) {
        ToProto(response->add_tablet_cells(), cell->GetDescriptor());
    }

    for (const auto* index : trunkTable->SecondaryIndices()) {
        auto* protoIndexInfo = response->add_indices();
        ToProto(protoIndexInfo->mutable_index_table_id(), index->GetIndexTable()->GetId());
        protoIndexInfo->set_index_kind(ToProto<i32>(index->GetKind()));
    }

    if (trunkTable->IsReplicated()) {
        const auto* replicatedTable = trunkTable->As<TReplicatedTableNode>();
        for (const auto* replica : replicatedTable->Replicas()) {
            auto* protoReplica = response->add_replicas();
            ToProto(protoReplica->mutable_replica_id(), replica->GetId());
            protoReplica->set_cluster_name(replica->GetClusterName());
            protoReplica->set_replica_path(replica->GetReplicaPath());
            protoReplica->set_mode(static_cast<int>(replica->GetMode()));
        }
    }

    if (trunkTable->GetReplicationCardId()) {
        ToProto(response->mutable_replication_card_id(), trunkTable->GetReplicationCardId());
    }

    if (const auto& hunkStorageNode = trunkTable->GetHunkStorageNode()) {
        ToProto(response->mutable_hunk_storage_node_id(), hunkStorageNode->GetId());
    }

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Alter)
{
    DeclareMutating();

    struct TAlterTableOptions
    {
        NTableClient::TTableSchemaPtr Schema;
        std::optional<bool> Dynamic;
        std::optional<TTableReplicaId> UpstreamReplicaId;
        std::optional<ETableSchemaModification> SchemaModification;
        std::optional<TReplicationProgress> ReplicationProgress;
        TObjectId SchemaId;
    } options;

    if (request->has_schema()) {
        options.Schema = New<TTableSchema>(FromProto<TTableSchema>(request->schema()));
    }
    if (request->has_dynamic()) {
        options.Dynamic = request->dynamic();
    }
    if (request->has_upstream_replica_id()) {
        options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
    }
    if (request->has_schema_modification()) {
        options.SchemaModification = FromProto<ETableSchemaModification>(request->schema_modification());
    }
    if (request->has_replication_progress()) {
        options.ReplicationProgress = FromProto<TReplicationProgress>(request->replication_progress());
    }
    if (request->has_schema_id()) {
        options.SchemaId = FromProto<TObjectId>(request->schema_id());
    }

    context->SetRequestInfo("Schema: %v, SchemaId: %v, Dynamic: %v, UpstreamReplicaId: %v, SchemaModification: %v, ReplicationProgress: %v",
        options.Schema,
        options.SchemaId,
        options.Dynamic,
        options.UpstreamReplicaId,
        options.SchemaModification,
        options.ReplicationProgress);

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    const auto& tableManager = Bootstrap_->GetTableManager();
    auto* table = LockThisImpl();
    auto dynamic = options.Dynamic.value_or(table->IsDynamic());
    auto schemaReceived = options.SchemaId || options.Schema;

    // If nothing was received on the native cell - nothing will be received on the external.
    if (schemaReceived) {
        tableManager->ValidateTableSchemaCorrespondence(
            table->GetVersionedId(),
            options.Schema,
            options.SchemaId);
    }

    TTableSchemaPtr schema;
    if (options.Schema) {
        schema = options.Schema;
    } else if (options.SchemaId) {
        schema = tableManager->GetMasterTableSchemaOrThrow(options.SchemaId)->AsTableSchema();
    } else {
        schema = table->GetSchema()->AsTableSchema();
    }

    bool isQueueObjectBefore = table->IsTrackedQueueObject();

    // NB: Sorted dynamic tables contain unique keys, set this for user.
    if (dynamic && schemaReceived  && schema->IsSorted() && !schema->GetUniqueKeys()) {
        // COMPAT(h0pless): Change this to YT_VERIFY after schema migration is complete.
        YT_LOG_ALERT_IF(
            !schema->IsUniqueKeys() && table->IsForeign(),
            "Schema doesn't have UniqueKeys set to true on the external cell (TableId: %v, Schema: %v)",
            table->GetId(),
            schema);

        schema = schema->ToUniqueKeys();
    }

    if (table->IsNative()) {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        if (table->GetSchema()->AsTableSchema()->HasNontrivialSchemaModification()) {
            THROW_ERROR_EXCEPTION("Cannot alter table with nontrivial schema modification");
        }

        if (schemaReceived && schema->HasNontrivialSchemaModification()) {
            THROW_ERROR_EXCEPTION("Schema modification cannot be specified as schema attribute");
        }

        if (table->IsPhysicallyLog()) {
            if (schemaReceived && table->GetType() != EObjectType::ReplicatedTable) {
                THROW_ERROR_EXCEPTION("Cannot change schema of a table of type %Qlv",
                    table->GetType());
            }
            if (options.SchemaModification || options.Dynamic) {
                THROW_ERROR_EXCEPTION("Cannot alter table of type %Qlv",
                    table->GetType());
            }
        }

        if (options.Dynamic) {
            ValidateNoTransaction();
        }

        if (table->IsDynamic() && !dynamic && table->GetHunkStorageNode()) {
            THROW_ERROR_EXCEPTION("Cannot alter table with a hunk storage node to static");
        }

        if (schemaReceived && table->IsDynamic()) {
            table->ValidateAllTabletsUnmounted("Cannot change table schema");
        }

        if (options.UpstreamReplicaId) {
            if (!dynamic) {
                THROW_ERROR_EXCEPTION("Upstream replica can only be set for dynamic tables");
            }
            if (table->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Upstream replica cannot be explicitly set for replicated tables");
            }

            table->ValidateAllTabletsUnmounted("Cannot change upstream replica");
        }

        if (options.SchemaModification) {
            if (dynamic) {
                THROW_ERROR_EXCEPTION("Schema modification cannot be applied to a dynamic table");
            }
            if (!table->IsEmpty()) {
                THROW_ERROR_EXCEPTION("Schema modification can only be applied to an empty table");
            }
            if (!schema->IsSorted()) {
                THROW_ERROR_EXCEPTION("Schema modification can only be applied to sorted schema");
            }
            if (!schema->GetStrict()) {
                THROW_ERROR_EXCEPTION("Schema modification can only be applied to strict schema");
            }
        }

        if (options.ReplicationProgress) {
            if (!dynamic) {
                THROW_ERROR_EXCEPTION("Replication progress can only be set for dynamic tables");
            }
            if (table->IsReplicated()) {
                THROW_ERROR_EXCEPTION("Replication progress cannot be set for replicated tables");
            }

            if (!table->GetReplicationCardId() &&
                !(options.UpstreamReplicaId && IsChaosTableReplicaType(TypeFromId(*options.UpstreamReplicaId))))
            {
                THROW_ERROR_EXCEPTION("Replication progress can only be set for tables bound for chaos replication");
            }

            table->ValidateAllTabletsUnmounted("Cannot change replication progress");
        }

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();

        ValidateTableSchemaUpdateInternal(
            *table->GetSchema()->AsTableSchema(),
            *schema,
            GetSchemaUpdateEnabledFeatures(config),
            dynamic,
            table->IsEmpty() && !table->IsDynamic());

        if (!config->EnableDescendingSortOrder ||
            dynamic && !config->EnableDescendingSortOrderDynamic)
        {
            ValidateNoDescendingSortOrder(*schema);
        }

        if (!config->EnableTableColumnRenaming ||
            dynamic && !config->EnableDynamicTableColumnRenaming)
        {
            ValidateNoRenamedColumns(*schema);
        }

        if (options.Dynamic) {
            if (*options.Dynamic) {
                tabletManager->ValidateMakeTableDynamic(table);
            } else {
                tabletManager->ValidateMakeTableStatic(table);
            }
        }
    }

    YT_LOG_ACCESS(
        context,
        GetId(),
        GetPath(),
        Transaction_);

    TMasterTableSchema* resultingSchema = nullptr;
    if (options.SchemaModification) {
        YT_LOG_ALERT_IF(table->IsForeign(), "Alter request with schema modification present was received by an external cell (TableId: %v)",
            table->GetId());
        schema = schema->ToModifiedSchema(*options.SchemaModification);
    }

    if (schemaReceived || options.SchemaModification) {
        auto setCorrespondingTableSchema = [] (
            TTableNode* table,
            const TTableSchemaPtr& schema,
            const TAlterTableOptions& options,
            const auto& tableManager)
        {
            if (table->IsNative()) {
                return tableManager->GetOrCreateNativeMasterTableSchema(*schema, table);
            }

            // COMPAT(h0pless): RefactorSchemaExport
            if (options.Schema) {
                // COMPAT(h0pless): Remove this after schema migration is complete.
                if (!options.SchemaId) {
                    YT_LOG_ALERT("Created native schema on an external cell tag (TableId: %v)",
                        table->GetId());
                    return tableManager->GetOrCreateNativeMasterTableSchema(*schema, table);
                }

                YT_LOG_ALERT("Created imported schema on an external cell outside of designated mutation "
                    "(TableId: %v)",
                    table->GetId());
                return tableManager->CreateImportedMasterTableSchema(*schema, table, options.SchemaId);
            }

            auto* existingSchema = tableManager->GetMasterTableSchema(options.SchemaId);
            tableManager->SetTableSchema(table, existingSchema);
            return existingSchema;
        };

        resultingSchema = setCorrespondingTableSchema(table, schema, options, tableManager);

        table->SetSchemaMode(ETableSchemaMode::Strong);
    }

    if (options.Dynamic) {
        if (*options.Dynamic) {
            tabletManager->MakeTableDynamic(table);
        } else {
            tabletManager->MakeTableStatic(table);
        }
    }

    if (options.UpstreamReplicaId) {
        table->SetUpstreamReplicaId(*options.UpstreamReplicaId);
    }

    if (options.ReplicationProgress) {
        ScatterReplicationProgress(table, *options.ReplicationProgress);
    }

    if (table->IsExternal()) {
        auto replicationRequest = TTableYPathProxy::Alter(FromObjectId(GetId()));
        if (request->has_dynamic()) {
            replicationRequest->set_dynamic(request->dynamic());
        }
        if (request->has_upstream_replica_id()) {
            replicationRequest->mutable_upstream_replica_id()->CopyFrom(request->upstream_replica_id());
        }
        if (request->has_replication_progress()) {
            replicationRequest->mutable_replication_progress()->CopyFrom(request->replication_progress());
        }

        auto externalCellTag = table->GetExternalCellTag();
        if (resultingSchema) {
            auto schemaId = table->GetSchema()->GetId();
            ToProto(replicationRequest->mutable_schema_id(), schemaId);
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        TCellTagList cellTag = {externalCellTag};
        auto* transaction = GetTransaction();
        auto externalizedTransactionId = transactionManager->ExternalizeTransaction(transaction, cellTag);
        SetTransactionId(replicationRequest, externalizedTransactionId);

        multicellManager->PostToMasters(replicationRequest, cellTag);
    }

    bool isQueueObjectAfter = table->IsTrackedQueueObject();
    if (isQueueObjectAfter != isQueueObjectBefore) {
        if (isQueueObjectAfter) {
            tableManager->RegisterQueue(table);
        } else {
            tableManager->UnregisterQueue(table);
        }
    }

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, LockDynamicTable)
{
    DeclareMutating();
    ValidateTransaction();

    auto timestamp = request->timestamp();

    context->SetRequestInfo("Timestamp: %v",
        timestamp);

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->LockDynamicTable(
        GetThisImpl()->GetTrunkNode(),
        GetTransaction(),
        timestamp);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, CheckDynamicTableLock)
{
    ValidateTransaction();

    context->SetRequestInfo();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->CheckDynamicTableLock(
        GetThisImpl()->GetTrunkNode(),
        GetTransaction(),
        response);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, StartBackup)
{
    DeclareMutating();
    ValidateTransaction();

    auto timestamp = request->timestamp();
    auto backupMode = FromProto<EBackupMode>(request->backup_mode());

    auto upstreamReplicaId = request->has_upstream_replica_id()
        ? FromProto<TTableReplicaId>(request->upstream_replica_id())
        : TTableReplicaId{};
    auto clockClusterTag = request->has_clock_cluster_tag()
        ? std::optional(FromProto<TClusterTag>(request->clock_cluster_tag()))
        : std::nullopt;

    auto replicaDescriptors = FromProto<std::vector<TTableReplicaBackupDescriptor>>(
        request->replicas());

    context->SetRequestInfo("Timestamp: %v, BackupMode: %v, ClockClusterTag: %v",
        timestamp,
        backupMode,
        clockClusterTag);

    const auto& backupManager = Bootstrap_->GetBackupManager();
    backupManager->StartBackup(
        GetThisImpl()->GetTrunkNode(),
        timestamp,
        GetTransaction(),
        backupMode,
        upstreamReplicaId,
        clockClusterTag,
        std::move(replicaDescriptors));

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, StartRestore)
{
    DeclareMutating();
    ValidateTransaction();

    auto replicaDescriptors = FromProto<std::vector<TTableReplicaBackupDescriptor>>(
        request->replicas());

    context->SetRequestInfo();

    const auto& backupManager = Bootstrap_->GetBackupManager();
    backupManager->StartRestore(
        GetThisImpl()->GetTrunkNode(),
        GetTransaction(),
        std::move(replicaDescriptors));

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, CheckBackup)
{
    ValidateTransaction();

    context->SetRequestInfo();

    const auto& backupManager = Bootstrap_->GetBackupManager();
    backupManager->CheckBackup(
        GetThisImpl()->GetTrunkNode(),
        response);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, FinishBackup)
{
    ValidateTransaction();

    context->SetRequestInfo();

    const auto& backupManager = Bootstrap_->GetBackupManager();
    context->ReplyFrom(backupManager->FinishBackup(GetThisImpl()));
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, FinishRestore)
{
    ValidateTransaction();

    context->SetRequestInfo();

    const auto& backupManager = Bootstrap_->GetBackupManager();
    context->ReplyFrom(backupManager->FinishRestore(GetThisImpl()));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicatedTableNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TTableNodeProxy::ListSystemAttributes(descriptors);

    const auto* table = GetThisImpl();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Replicas)
        .SetExternal(table->IsExternal())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicatedTableOptions)
        .SetReplicated(true)
        .SetWritable(true));
}

bool TReplicatedTableNodeProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* table = TBase::GetThisImpl<TReplicatedTableNode>();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();
    auto isExternal = table->IsExternal();

    switch (key) {
        case EInternedAttributeKey::Replicas: {
            if (isExternal) {
                break;
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();
            BuildYsonFluently(consumer)
                .DoMapFor(table->Replicas(), [&] (TFluentMap fluent, TTableReplica* replica) {
                    auto replicaProxy = objectManager->GetProxy(replica);
                    fluent
                        .Item(ToString(replica->GetId()))
                        .BeginMap()
                            .Item("cluster_name").Value(replica->GetClusterName())
                            .Item("replica_path").Value(replica->GetReplicaPath())
                            .Item("state").Value(replica->GetState())
                            .Item("mode").Value(replica->GetMode())
                            .Item("replication_lag_time").Value(replica->ComputeReplicationLagTime(
                                timestampProvider->GetLatestTimestamp()))
                            .Item("error_count").Value(replica->GetErrorCount())
                            .Item("replicated_table_tracker_enabled").Value(replica->GetEnableReplicatedTableTracker())
                        .EndMap();
                });
            return true;
        }

        case EInternedAttributeKey::ReplicatedTableOptions: {
            BuildYsonFluently(consumer)
                .Value(table->GetReplicatedTableOptions());
            return true;
        }

        default:
            break;
    }

    return TTableNodeProxy::GetBuiltinAttribute(key, consumer);
}

bool TReplicatedTableNodeProxy::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force)
{
    auto* table = TBase::GetThisImpl<TReplicatedTableNode>();

    switch (key) {
        case EInternedAttributeKey::ReplicatedTableOptions: {
            auto options = ConvertTo<TReplicatedTableOptionsPtr>(value);
            table->SetReplicatedTableOptions(options);
            Bootstrap_->GetTabletManager()->GetReplicatedTableCreatedSignal()->Fire(TReplicatedTableData{
                .Id = table->GetTrunkNode()->GetId(),
                .Options = options,
            });
            return true;
        }

        default:
            break;
    }

    return TTableNodeProxy::SetBuiltinAttribute(key, value, force);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
