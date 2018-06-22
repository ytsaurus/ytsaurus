#include "table_node_proxy_detail.h"
#include "private.h"
#include "table_node.h"
#include "replicated_table_node.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_visitor.h>

#include <yt/server/node_tracker_server/node_directory_builder.h>

#include <yt/server/table_server/shared_table_schema.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_cell.h>
#include <yt/server/tablet_server/table_replica.h>
#include <yt/server/tablet_server/tablet_manager.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/string.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/tree_builder.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/async_consumer.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TTableNode* trunkNode)
    : TBase(
        bootstrap,
        metadata,
        transaction,
        trunkNode)
{ }

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TBase::ListSystemAttributes(descriptors);

    const auto* table = GetThisImpl();
    bool isDynamic = table->IsDynamic();
    bool isSorted = table->IsSorted();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkRowCount));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RowCount)
        .SetPresent(!isDynamic));
    // TODO(savrus) remove "unmerged_row_count" in 20.0
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnmergedRowCount)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(EInternedAttributeKey::Sorted);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::KeyColumns)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Schema)
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SchemaDuplicateCount));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SortedBy)
        .SetPresent(isSorted));
    descriptors->push_back(EInternedAttributeKey::Dynamic);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCount)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletState)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastCommitTimestamp)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Tablets)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCountByState)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKeys)
        .SetPresent(isDynamic && isSorted)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RetainedTimestamp)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnflushedTimestamp)
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletStatistics)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletErrors)
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletErrorCount)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetWritable(true)
        .SetPresent(table->GetTrunkNode()->GetTabletCellBundle()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Atomicity)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CommitOrdering)
        .SetWritable(true)
        .SetPresent(!isSorted));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::InMemoryMode)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OptimizeFor)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::OptimizeForStatistics)
        .SetExternal(table->IsExternal())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::SchemaMode));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkWriter)
        .SetCustom(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UpstreamReplicaId)
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TableChunkFormatStatistics)
        .SetExternal(table->IsExternal())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::EnableTabletBalancer)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DisableTabletBalancer)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MinTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetMinTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaxTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetMaxTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DesiredTabletSize)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletSize())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DesiredTabletCount)
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletCount())));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FlushLagTime)
        .SetPresent(isDynamic && isSorted));
}

bool TTableNodeProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();
    auto statistics = table->ComputeTotalStatistics();
    bool isDynamic = table->IsDynamic();
    bool isSorted = table->IsSorted();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    switch (key) {
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
                .Value(table->GetTableSchema().IsSorted());
            return true;

        case EInternedAttributeKey::KeyColumns:
            BuildYsonFluently(consumer)
                .Value(table->GetTableSchema().GetKeyColumns());
            return true;

        case EInternedAttributeKey::Schema:
            BuildYsonFluently(consumer)
                .Value(table->GetTableSchema());
            return true;

        case EInternedAttributeKey::SchemaDuplicateCount: {
            const auto& sharedSchema = table->SharedTableSchema();
            i64 duplicateCount = sharedSchema ? sharedSchema->GetRefCount() : 0;
            BuildYsonFluently(consumer)
                .Value(duplicateCount);
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
                .Value(table->GetTableSchema().GetKeyColumns());
            return true;

        case EInternedAttributeKey::Dynamic:
            BuildYsonFluently(consumer)
                .Value(table->IsDynamic());
            return true;

        case EInternedAttributeKey::TabletCount:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->Tablets().size());
            return true;

        case EInternedAttributeKey::TabletCountByState:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->TabletCountByState());
            return true;

        case EInternedAttributeKey::TabletState:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetTabletState());
            return true;

        case EInternedAttributeKey::LastCommitTimestamp:
            if (!isDynamic || !isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetLastCommitTimestamp());
            return true;

        case EInternedAttributeKey::Tablets:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTablet* tablet) {
                    auto* cell = tablet->GetCell();
                    fluent
                        .Item().BeginMap()
                            .Item("index").Value(tablet->GetIndex())
                            .Item("performance_counters").Value(tablet->PerformanceCounters())
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
                                tabletManager->GetTabletStatistics(tablet),
                                chunkManager))
                            .Item("tablet_id").Value(tablet->GetId())
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("cell_id").Value(cell->GetId());
                            })
                            .Item("error_count").Value(tablet->GetErrorCount())
                        .EndMap();
                });
            return true;

        case EInternedAttributeKey::PivotKeys:
            if (!isDynamic || !isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTablet* tablet) {
                    fluent
                        .Item().Value(tablet->GetPivotKey());
                });
            return true;

        case EInternedAttributeKey::RetainedTimestamp:
            if (!isDynamic || !isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->GetCurrentRetainedTimestamp());
            return true;

        case EInternedAttributeKey::UnflushedTimestamp:
            if (!isDynamic || !isSorted) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(table->GetCurrentUnflushedTimestamp(timestampProvider->GetLatestTimestamp()));
            return true;

        case EInternedAttributeKey::TabletStatistics: {
            if (!isDynamic) {
                break;
            }
            TTabletStatistics tabletStatistics;
            for (const auto& tablet : trunkTable->Tablets()) {
                tabletStatistics += tabletManager->GetTabletStatistics(tablet);
            }
            BuildYsonFluently(consumer)
                .Value(New<TSerializableTabletStatistics>(
                    tabletStatistics,
                    chunkManager));
            return true;
        }

        case EInternedAttributeKey::TabletErrors: {
            if (!isDynamic) {
                break;
            }
            std::vector<TError> errors;
            for (const auto& tablet : trunkTable->Tablets()) {
                const auto& tabletErrors = tablet->GetErrors();
                errors.insert(errors.end(), tabletErrors.begin(), tabletErrors.end());
            }
            BuildYsonFluently(consumer)
                .Value(errors);
            return true;
        }

        case EInternedAttributeKey::TabletErrorCount:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetTabletErrorCount());
            return true;

        case EInternedAttributeKey::TabletCellBundle:
            if (!trunkTable->GetTabletCellBundle()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetTabletCellBundle()->GetName());
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

        case EInternedAttributeKey::InMemoryMode:
            BuildYsonFluently(consumer)
                .Value(table->GetInMemoryMode());
            return true;

        case EInternedAttributeKey::UpstreamReplicaId:
            if (!isDynamic) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetUpstreamReplicaId());
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

        case EInternedAttributeKey::FlushLagTime: {
            if (!isSorted || !isDynamic) {
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

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TTableNodeProxy::GetBuiltinAttributeAsync(TInternedAttributeKey key)
{
    const auto* table = GetThisImpl();
    auto* chunkList = table->GetChunkList();
    auto isExternal = table->IsExternal();

    if (!isExternal) {
        switch (key) {
            case EInternedAttributeKey::TableChunkFormatStatistics:
                return ComputeChunkStatistics(
                    Bootstrap_,
                    chunkList,
                    [] (const TChunk* chunk) { return ETableChunkFormat(chunk->ChunkMeta().version()); });

            case EInternedAttributeKey::OptimizeForStatistics: {
                auto optimizeForExtractor = [] (const TChunk* chunk) {
                    switch (static_cast<ETableChunkFormat>(chunk->ChunkMeta().version())) {
                        case ETableChunkFormat::Old:
                        case ETableChunkFormat::VersionedSimple:
                        case ETableChunkFormat::Schemaful:
                        case ETableChunkFormat::SchemalessHorizontal:
                            return NTableClient::EOptimizeFor::Lookup;
                        case ETableChunkFormat::VersionedColumnar:
                        case ETableChunkFormat::UnversionedColumnar:
                            return NTableClient::EOptimizeFor::Scan;
                        default:
                            Y_UNREACHABLE();
                    }
                };

                return ComputeChunkStatistics(Bootstrap_, chunkList, optimizeForExtractor);
            }

            default:
                break;
        }
    }

    return TBase::GetBuiltinAttributeAsync(key);
}

bool TTableNodeProxy::RemoveBuiltinAttribute(TInternedAttributeKey key)
{
    switch (key) {
        case EInternedAttributeKey::EnableTabletBalancer: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(Null);
            return true;
        }

        case EInternedAttributeKey::DisableTabletBalancer: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(Null);
            return true;
        }

        case EInternedAttributeKey::MinTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMinTabletSize(Null);
            return true;
        }

        case EInternedAttributeKey::MaxTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMaxTabletSize(Null);
            return true;
        }

        case EInternedAttributeKey::DesiredTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletSize(Null);
            return true;
        }

        case EInternedAttributeKey::DesiredTabletCount: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletCount(Null);
            return true;
        }

        default:
            break;
    }

    return TBase::RemoveBuiltinAttribute(key);
}

bool TTableNodeProxy::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value)
{
    const auto* table = GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);
            const auto& tabletManager = Bootstrap_->GetTabletManager();
            auto* cellBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name);

            auto* lockedTable = LockThisImpl();
            tabletManager->SetTabletCellBundle(lockedTable, cellBundle);

            return true;
        }

        case EInternedAttributeKey::Atomicity: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            auto tabletState = table->GetTabletState();
            if (tabletState != ETabletState::Unmounted && tabletState != ETabletState::None) {
                THROW_ERROR_EXCEPTION("Cannot change table atomicity mode since not all of its tablets are in %Qlv state",
                    ETabletState::Unmounted);
            }

            auto atomicity = ConvertTo<NTransactionClient::EAtomicity>(value);
            lockedTable->SetAtomicity(atomicity);

            return true;
        }

        case EInternedAttributeKey::CommitOrdering: {
            if (table->IsSorted()) {
                break;
            }
            ValidateNoTransaction();

            auto tabletState = table->GetTabletState();
            if (tabletState != ETabletState::Unmounted && tabletState != ETabletState::None) {
                THROW_ERROR_EXCEPTION("Cannot change table commit ordering mode since not all of its tablets are in %Qlv state",
                    ETabletState::Unmounted);
            }

            auto* lockedTable = LockThisImpl();
            auto ordering = ConvertTo<NTransactionClient::ECommitOrdering>(value);
            lockedTable->SetCommitOrdering(ordering);

            return true;
        }

        case EInternedAttributeKey::OptimizeFor: {
            ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

            const auto& uninternedKey = GetUninternedAttributeKey(key);
            auto* lockedTable = LockThisImpl<TTableNode>(TLockRequest::MakeSharedAttribute(uninternedKey));
            lockedTable->SetOptimizeFor(ConvertTo<EOptimizeFor>(value));

            return true;
        }

        case EInternedAttributeKey::InMemoryMode: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            auto tabletState = table->GetTabletState();
            if (tabletState != ETabletState::Unmounted && tabletState != ETabletState::None) {
                THROW_ERROR_EXCEPTION("Cannot change table memory mode since not all of its tablets are in %Qlv state",
                    ETabletState::Unmounted);
            }

            auto inMemoryMode = ConvertTo<EInMemoryMode>(value);
            lockedTable->SetInMemoryMode(inMemoryMode);

            return true;
        }

        case EInternedAttributeKey::EnableTabletBalancer: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(ConvertTo<bool>(value));
            return true;
        }

        case EInternedAttributeKey::DisableTabletBalancer: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetEnableTabletBalancer(!ConvertTo<bool>(value));
            return true;
        }

        case EInternedAttributeKey::MinTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMinTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::MaxTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetMaxTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::DesiredTabletSize: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletSize(ConvertTo<i64>(value));
            return true;
        }

        case EInternedAttributeKey::DesiredTabletCount: {
            auto* lockedTable = LockThisImpl();
            lockedTable->SetDesiredTabletCount(ConvertTo<int>(value));
            return true;
        }

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value);
}

void TTableNodeProxy::ValidateCustomAttributeUpdate(
    const TString& key,
    const TYsonString& oldValue,
    const TYsonString& newValue)
{
    auto internedKey = GetInternedAttributeKey(key);

    switch (internedKey) {
        case EInternedAttributeKey::ChunkWriter:
            if (!newValue) {
                break;
            }
            ConvertTo<TTableWriterConfigPtr>(newValue);
            return;

        default:
            break;
    }

    TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
}

void TTableNodeProxy::ValidateFetchParameters(
    const std::vector<NChunkClient::TReadRange>& ranges)
{
    TChunkOwnerNodeProxy::ValidateFetchParameters(ranges);

    const auto* table = GetThisImpl();
    for (const auto& range : ranges) {
        const auto& lowerLimit = range.LowerLimit();
        const auto& upperLimit = range.UpperLimit();
        if ((upperLimit.HasKey() || lowerLimit.HasKey()) && !table->IsSorted()) {
            THROW_ERROR_EXCEPTION("Key selectors are not supported for unsorted tables");
        }
        if ((upperLimit.HasRowIndex() || lowerLimit.HasRowIndex()) && table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Row index selectors are not supported for dynamic tables");
        }
        if (upperLimit.HasOffset() || lowerLimit.HasOffset()) {
            THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
        }
    }
}

bool TTableNodeProxy::DoInvoke(const IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Mount);
    DISPATCH_YPATH_SERVICE_METHOD(Unmount);
    DISPATCH_YPATH_SERVICE_METHOD(Remount);
    DISPATCH_YPATH_SERVICE_METHOD(Freeze);
    DISPATCH_YPATH_SERVICE_METHOD(Unfreeze);
    DISPATCH_YPATH_SERVICE_METHOD(Reshard);
    DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
    DISPATCH_YPATH_SERVICE_METHOD(Alter);
    return TBase::DoInvoke(context);
}

void TTableNodeProxy::ValidateBeginUpload()
{
    TBase::ValidateBeginUpload();

    const auto* table = GetThisImpl();
    if (table->IsDynamic()) {
        THROW_ERROR_EXCEPTION("Cannot upload into a dynamic table");
    }
}

void TTableNodeProxy::ValidateStorageParametersUpdate()
{
    TChunkOwnerNodeProxy::ValidateStorageParametersUpdate();

    const auto* node = GetThisImpl();
    auto state = node->GetTabletState();
    if (state != ETabletState::None && state != ETabletState::Unmounted) {
        THROW_ERROR_EXCEPTION("Cannot change storage parameters since not all tablets are unmounted");
    }
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Mount)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->last_tablet_index();
    auto cellId = FromProto<TTabletCellId>(request->cell_id());
    bool freeze = request->freeze();
    auto mountTimestamp = static_cast<TTimestamp>(request->mount_timestamp());

    context->SetRequestInfo(
        "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, Freeze: %v, MountTimestamp: %v",
        firstTabletIndex,
        lastTabletIndex,
        cellId,
        freeze,
        mountTimestamp);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    const auto& tabletManager = Bootstrap_->GetTabletManager();

    TTabletCell* cell = nullptr;
    if (cellId) {
        cell = tabletManager->GetTabletCellOrThrow(cellId);
    }

    auto* table = LockThisImpl();

    tabletManager->MountTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        cell,
        freeze,
        mountTimestamp);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Unmount)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->last_tablet_index();
    bool force = request->force();
    context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, Force: %v",
        firstTabletIndex,
        lastTabletIndex,
        force);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    auto* table = LockThisImpl();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->UnmountTable(
        table,
        force,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Freeze)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->last_tablet_index();

    context->SetRequestInfo(
        "FirstTabletIndex: %v, LastTabletIndex: %v",
        firstTabletIndex,
        lastTabletIndex);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    auto* table = LockThisImpl();

    tabletManager->FreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Unfreeze)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->last_tablet_index();
    context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v",
        firstTabletIndex,
        lastTabletIndex);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    auto* table = LockThisImpl();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->UnfreezeTable(
        table,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Remount)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->first_tablet_index();
    context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v",
        firstTabletIndex,
        lastTabletIndex);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    auto* table = LockThisImpl();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->RemountTable(
        table,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Reshard)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->last_tablet_index();
    int tabletCount = request->tablet_count();
    auto pivotKeys = FromProto<std::vector<TOwningKey>>(request->pivot_keys());
    context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, TabletCount: %v",
        firstTabletIndex,
        lastTabletIndex,
        tabletCount);

    ValidateNotExternal();
    ValidateNoTransaction();
    ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

    auto* table = LockThisImpl();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    tabletManager->ReshardTable(
        table,
        firstTabletIndex,
        lastTabletIndex,
        tabletCount,
        pivotKeys);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, GetMountInfo)
{
    DeclareNonMutating();

    context->SetRequestInfo();

    ValidateNotExternal();
    ValidateNoTransaction();

    const auto* trunkTable = GetThisImpl();

    ToProto(response->mutable_table_id(), trunkTable->GetId());
    response->set_dynamic(trunkTable->IsDynamic());
    ToProto(response->mutable_upstream_replica_id(), trunkTable->GetUpstreamReplicaId());
    ToProto(response->mutable_schema(), trunkTable->GetTableSchema());

    THashSet<TTabletCell*> cells;
    for (auto* tablet : trunkTable->Tablets()) {
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->add_tablets();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        protoTablet->set_mount_revision(tablet->GetMountRevision());
        protoTablet->set_state(static_cast<int>(tablet->GetState()));
        protoTablet->set_in_memory_mode(static_cast<int>(tablet->GetInMemoryMode()));
        ToProto(protoTablet->mutable_pivot_key(), tablet->GetPivotKey());
        if (cell) {
            ToProto(protoTablet->mutable_cell_id(), cell->GetId());
            cells.insert(cell);
        }
    }

    for (const auto* cell : cells) {
        ToProto(response->add_tablet_cells(), cell->GetDescriptor());
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

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Alter)
{
    DeclareMutating();

    struct TAlterTableOptions
    {
        TNullable<NTableClient::TTableSchema> Schema;
        TNullable<bool> Dynamic;
        TNullable<NTabletClient::TTableReplicaId> UpstreamReplicaId;
    } options;

    if (request->has_schema()) {
        options.Schema = FromProto<TTableSchema>(request->schema());
    }
    if (request->has_dynamic()) {
        options.Dynamic = request->dynamic();
    }
    if (request->has_upstream_replica_id()) {
        options.UpstreamReplicaId = FromProto<TTableReplicaId>(request->upstream_replica_id());
    }

    context->SetRequestInfo("Schema: %v, Dynamic: %v, UpstreamReplicaId: %v",
        options.Schema,
        options.Dynamic,
        options.UpstreamReplicaId);

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* table = LockThisImpl();

    if (table->IsReplicated()) {
        THROW_ERROR_EXCEPTION("Cannot alter a replicated table");
    }

    if (options.Dynamic) {
        ValidateNoTransaction();

        if (*options.Dynamic && table->IsExternal()) {
            THROW_ERROR_EXCEPTION("External node cannot be a dynamic table");
        }
    }
    if (options.Schema && table->IsDynamic() && table->GetTabletState() != ETabletState::Unmounted) {
        THROW_ERROR_EXCEPTION("Cannot change table schema since not all of its tablets are in %Qlv state",
            ETabletState::Unmounted);
    }

    auto dynamic = options.Dynamic.Get(table->IsDynamic());
    auto schema = options.Schema.Get(table->GetTableSchema());

    if (options.UpstreamReplicaId) {
        ValidateNoTransaction();

        if (table->GetTabletState() != ETabletState::Unmounted) {
            THROW_ERROR_EXCEPTION("Cannot change upstream replica since not all of its tablets are in %Qlv state",
                ETabletState::Unmounted);
        }
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for dynamic tables");
        }
        if (table->IsReplicated()) {
            THROW_ERROR_EXCEPTION("Upstream replica cannot be explicitly set for replicated tables");
        }
    }

    // NB: Sorted dynamic tables contain unique keys, set this for user.
    if (dynamic && options.Schema && options.Schema->IsSorted() && !options.Schema->GetUniqueKeys()) {
        schema = schema.ToUniqueKeys();
    }

    ValidateTableSchemaUpdate(
        table->GetTableSchema(),
        schema,
        dynamic,
        table->IsEmpty() && !table->IsDynamic());

    auto oldSharedSchema = table->SharedTableSchema();
    auto oldSchemaMode = table->GetSchemaMode();

    if (options.Schema) {
        table->SharedTableSchema() = Bootstrap_->GetCypressManager()->GetSharedTableSchemaRegistry()->GetSchema(
            std::move(schema));
        table->SetSchemaMode(ETableSchemaMode::Strong);
    }

    try {
        if (options.Dynamic) {
            const auto& tabletManager = Bootstrap_->GetTabletManager();
            if (*options.Dynamic) {
                tabletManager->MakeTableDynamic(table);
            } else {
                tabletManager->MakeTableStatic(table);
            }
        }
    } catch (const std::exception&) {
        table->SharedTableSchema() = std::move(oldSharedSchema);
        table->SetSchemaMode(oldSchemaMode);
        throw;
    }

    if (options.UpstreamReplicaId) {
        table->SetUpstreamReplicaId(*options.UpstreamReplicaId);
    }

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

TReplicatedTableNodeProxy::TReplicatedTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TReplicatedTableNode* trunkNode)
    : TTableNodeProxy(
        bootstrap,
        metadata,
        transaction,
        trunkNode)
{ }

void TReplicatedTableNodeProxy::ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors)
{
    TTableNodeProxy::ListSystemAttributes(descriptors);

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Replicas)
        .SetOpaque(true));
}

bool TReplicatedTableNodeProxy::GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer)
{
    const auto* table = GetThisImpl<TReplicatedTableNode>();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();

    switch (key) {
        case EInternedAttributeKey::Replicas: {
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
                            .Item("errors").Value(replica->GetErrors())
                        .EndMap();
                });
            return true;
        }

        default:
            break;
    }

    return TTableNodeProxy::GetBuiltinAttribute(key, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT


