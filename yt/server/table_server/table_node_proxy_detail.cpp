#include "table_node_proxy_detail.h"
#include "private.h"
#include "table_node.h"
#include "replicated_table_node.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_visitor.h>

#include <yt/server/node_tracker_server/node_directory_builder.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_cell.h>
#include <yt/server/tablet_server/table_replica.h>
#include <yt/server/tablet_server/tablet_manager.h>

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

    descriptors->push_back(TAttributeDescriptor("chunk_row_count"));
    descriptors->push_back(TAttributeDescriptor("row_count")
        .SetPresent(!isDynamic));
    // TODO(savrus) remove "unmerged_row_count" in 20.0
    descriptors->push_back(TAttributeDescriptor("unmerged_row_count")
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back("sorted");
    descriptors->push_back(TAttributeDescriptor("key_columns")
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("schema")
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor("sorted_by")
        .SetPresent(isSorted));
    descriptors->push_back("dynamic");
    descriptors->push_back(TAttributeDescriptor("tablet_count")
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor("tablet_state")
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor("last_commit_timestamp")
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor("tablets")
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("tablet_count_by_state")
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("pivot_keys")
        .SetPresent(isDynamic && isSorted)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("retained_timestamp")
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor("unflushed_timestamp")
        .SetPresent(isDynamic && isSorted));
    descriptors->push_back(TAttributeDescriptor("tablet_statistics")
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("tablet_errors")
        .SetPresent(isDynamic)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("tablet_error_count")
        .SetPresent(isDynamic));
    descriptors->push_back(TAttributeDescriptor("tablet_cell_bundle")
        .SetWritable(true)
        .SetPresent(table->GetTrunkNode()->GetTabletCellBundle()));
    descriptors->push_back(TAttributeDescriptor("atomicity")
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor("commit_ordering")
        .SetWritable(true)
        .SetPresent(!isSorted));
    descriptors->push_back(TAttributeDescriptor("in_memory_mode")
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor("optimize_for")
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor("optimize_for_statistics")
        .SetExternal(table->IsExternal())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("schema_mode"));
    descriptors->push_back(TAttributeDescriptor("chunk_writer")
        .SetCustom(true));
    descriptors->push_back(TAttributeDescriptor("upstream_replica_id")
        .SetPresent(isSorted && isDynamic));
    descriptors->push_back(TAttributeDescriptor("table_chunk_format_statistics")
        .SetExternal(table->IsExternal())
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor("enable_tablet_balancer")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor("disable_tablet_balancer")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetEnableTabletBalancer())));
    descriptors->push_back(TAttributeDescriptor("min_tablet_size")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetMinTabletSize())));
    descriptors->push_back(TAttributeDescriptor("max_tablet_size")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetMaxTabletSize())));
    descriptors->push_back(TAttributeDescriptor("desired_tablet_size")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletSize())));
    descriptors->push_back(TAttributeDescriptor("desired_tablet_count")
        .SetWritable(true)
        .SetRemovable(true)
        .SetPresent(static_cast<bool>(table->GetDesiredTabletCount())));
    descriptors->push_back(TAttributeDescriptor("flush_lag_time")
        .SetPresent(isDynamic && isSorted));
}

bool TTableNodeProxy::GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer)
{
    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();
    auto statistics = table->ComputeTotalStatistics();
    bool isDynamic = table->IsDynamic();
    bool isSorted = table->IsSorted();

    const auto& tabletManager = Bootstrap_->GetTabletManager();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();
    const auto& chunkManager = Bootstrap_->GetChunkManager();

    if (key == "chunk_row_count") {
        BuildYsonFluently(consumer)
            .Value(statistics.row_count());
        return true;
    }

    if (key == "row_count" && !isDynamic) {
        BuildYsonFluently(consumer)
            .Value(statistics.row_count());
        return true;
    }

    if (key == "unmerged_row_count" && isDynamic && isSorted) {
        BuildYsonFluently(consumer)
            .Value(statistics.row_count());
        return true;
    }

    if (key == "sorted") {
        BuildYsonFluently(consumer)
            .Value(table->TableSchema().IsSorted());
        return true;
    }

    if (key == "key_columns") {
        BuildYsonFluently(consumer)
            .Value(table->TableSchema().GetKeyColumns());
        return true;
    }

    if (key == "schema") {
        BuildYsonFluently(consumer)
            .Value(table->TableSchema());
        return true;
    }

    if (key == "schema_mode") {
        BuildYsonFluently(consumer)
            .Value(table->GetSchemaMode());
        return true;
    }

    if (key == "sorted_by" && isSorted) {
        BuildYsonFluently(consumer)
            .Value(table->TableSchema().GetKeyColumns());
        return true;
    }

    if (key == "dynamic") {
        BuildYsonFluently(consumer)
            .Value(table->IsDynamic());
        return true;
    }

    if (key == "tablet_count" && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->Tablets().size());
        return true;
    }

    if (key == "tablet_count_by_state" && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->TabletCountByState());
        return true;
    }

    if (key == "tablet_state" && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetTabletState());
        return true;
    }

    if (key == "last_commit_timestamp" && isDynamic && isSorted) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetLastCommitTimestamp());
        return true;
    }

    if (key == "tablets" && isDynamic) {
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
    }

    if (key == "pivot_keys" && isDynamic && isSorted) {
        BuildYsonFluently(consumer)
            .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTablet* tablet) {
                fluent
                    .Item().Value(tablet->GetPivotKey());
            });
        return true;
    }

    if (key == "retained_timestamp" && isDynamic && isSorted) {
        BuildYsonFluently(consumer)
            .Value(table->GetCurrentRetainedTimestamp());
        return true;
    }

    if (key == "unflushed_timestamp" && isDynamic && isSorted) {
        BuildYsonFluently(consumer)
            .Value(table->GetCurrentUnflushedTimestamp(timestampProvider->GetLatestTimestamp()));
        return true;
    }

    if (key == "tablet_statistics" && isDynamic) {
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

    if (key == "tablet_errors" && isDynamic) {
        std::vector<TError> errors;
        for (const auto& tablet : trunkTable->Tablets()) {
            const auto& tabletErrors = tablet->GetErrors();
            errors.insert(errors.end(), tabletErrors.begin(), tabletErrors.end());
        }
        BuildYsonFluently(consumer)
            .Value(errors);
        return true;
    }

    if (key == "tablet_error_count" && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetTabletErrorCount());
        return true;
    }

    if (key == "tablet_cell_bundle" && trunkTable->GetTabletCellBundle()) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetTabletCellBundle()->GetName());
        return true;
    }

    if (key == "atomicity") {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetAtomicity());
        return true;
    }

    if (key == "commit_ordering") {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetCommitOrdering());
        return true;
    }

    if (key == "optimize_for") {
        BuildYsonFluently(consumer)
            .Value(table->GetOptimizeFor());
        return true;
    }

    if (key == "in_memory_mode") {
        BuildYsonFluently(consumer)
            .Value(table->GetInMemoryMode());
        return true;
    }

    if (key == "upstream_replica_id" && isSorted && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->GetUpstreamReplicaId());
        return true;
    }

    if (key == "tablet_count" && isDynamic) {
        BuildYsonFluently(consumer)
            .Value(trunkTable->Tablets().size());
        return true;
    }

    if (key == "enable_tablet_balancer" && static_cast<bool>(trunkTable->GetEnableTabletBalancer())) {
        BuildYsonFluently(consumer)
            .Value(*trunkTable->GetEnableTabletBalancer());
        return true;
    }

    if (key == "disable_tablet_balancer" && static_cast<bool>(trunkTable->GetEnableTabletBalancer())) {
        BuildYsonFluently(consumer)
            .Value(!*trunkTable->GetEnableTabletBalancer());
        return true;
    }

    if (key == "min_tablet_size" && static_cast<bool>(trunkTable->GetMinTabletSize())) {
        BuildYsonFluently(consumer)
            .Value(*trunkTable->GetMinTabletSize());
        return true;
    }

    if (key == "max_tablet_size" && static_cast<bool>(trunkTable->GetMaxTabletSize())) {
        BuildYsonFluently(consumer)
            .Value(*trunkTable->GetMaxTabletSize());
        return true;
    }

    if (key == "desired_tablet_size" && static_cast<bool>(trunkTable->GetDesiredTabletSize())) {
        BuildYsonFluently(consumer)
            .Value(*trunkTable->GetDesiredTabletSize());
        return true;
    }

    if (key == "desired_tablet_count" && static_cast<bool>(trunkTable->GetDesiredTabletCount())) {
        BuildYsonFluently(consumer)
            .Value(*trunkTable->GetDesiredTabletCount());
        return true;
    }

    if (key == "flush_lag_time" && isSorted && isDynamic) {
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

    return TBase::GetBuiltinAttribute(key, consumer);
}

TFuture<TYsonString> TTableNodeProxy::GetBuiltinAttributeAsync(const TString& key)
{
    const auto* table = GetThisImpl();
    auto* chunkList = table->GetChunkList();
    auto isExternal = table->IsExternal();

    if (!isExternal) {
        if (key == "table_chunk_format_statistics") {
            return ComputeChunkStatistics(
                Bootstrap_,
                chunkList,
                [] (const TChunk* chunk) { return ETableChunkFormat(chunk->ChunkMeta().version()); });
        }
        if (key == "optimize_for_statistics") {
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
    }

    return TBase::GetBuiltinAttributeAsync(key);
}

bool TTableNodeProxy::RemoveBuiltinAttribute(const TString& key)
{
    if (key == "enable_tablet_balancer") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetEnableTabletBalancer(Null);
        return true;
    }

    if (key == "disable_tablet_balancer") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetEnableTabletBalancer(Null);
        return true;
    }

    if (key == "min_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetMinTabletSize(Null);
        return true;
    }

    if (key == "max_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetMaxTabletSize(Null);
        return true;
    }

    if (key == "desired_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetDesiredTabletSize(Null);
        return true;
    }

    if (key == "desired_tablet_count") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetDesiredTabletCount(Null);
        return true;
    }

    return TBase::RemoveBuiltinAttribute(key);
}

bool TTableNodeProxy::SetBuiltinAttribute(const TString& key, const TYsonString& value)
{
    const auto* table = GetThisImpl();

    if (key == "tablet_cell_bundle") {
        ValidateNoTransaction();

        auto name = ConvertTo<TString>(value);
        const auto& tabletManager = Bootstrap_->GetTabletManager();
        auto* cellBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name);

        auto* lockedTable = LockThisImpl();
        tabletManager->SetTabletCellBundle(lockedTable, cellBundle);

        return true;
    }

    if (key == "atomicity") {
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

    if (key == "commit_ordering" && !table->IsSorted()) {
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

    if (key == "optimize_for") {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* lockedTable = LockThisImpl<TTableNode>(TLockRequest::MakeSharedAttribute(key));
        lockedTable->SetOptimizeFor(ConvertTo<EOptimizeFor>(value));

        return true;
    }

    if (key == "in_memory_mode") {
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

    if (key == "enable_tablet_balancer") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetEnableTabletBalancer(ConvertTo<bool>(value));
        return true;
    }

    if (key == "disable_tablet_balancer") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetEnableTabletBalancer(!ConvertTo<bool>(value));
        return true;
    }

    if (key == "min_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetMinTabletSize(ConvertTo<i64>(value));
        return true;
    }

    if (key == "max_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetMaxTabletSize(ConvertTo<i64>(value));
        return true;
    }

    if (key == "desired_tablet_size") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetDesiredTabletSize(ConvertTo<i64>(value));
        return true;
    }

    if (key == "desired_tablet_count") {
        auto* lockedTable = LockThisImpl();
        lockedTable->SetDesiredTabletCount(ConvertTo<int>(value));
        return true;
    }

    return TBase::SetBuiltinAttribute(key, value);
}

void TTableNodeProxy::ValidateCustomAttributeUpdate(
    const TString& key,
    const TYsonString& oldValue,
    const TYsonString& newValue)
{
    if (key == "chunk_writer" && newValue) {
        ConvertTo<TTableWriterConfigPtr>(newValue);
        return;
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
    ToProto(response->mutable_schema(), trunkTable->TableSchema());

    THashSet<TTabletCell*> cells;
    for (auto* tablet : trunkTable->Tablets()) {
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->add_tablets();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        protoTablet->set_mount_revision(tablet->GetMountRevision());
        protoTablet->set_state(static_cast<int>(tablet->GetState()));
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
    auto schema = options.Schema.Get(table->TableSchema());

    if (options.UpstreamReplicaId) {
        ValidateNoTransaction();

        if (table->GetTabletState() != ETabletState::Unmounted) {
            THROW_ERROR_EXCEPTION("Cannot change upstream replica since not all of its tablets are in %Qlv state",
                ETabletState::Unmounted);
        }
        if (!dynamic) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for dynamic tables");
        }
        if (!schema.IsSorted()) {
            THROW_ERROR_EXCEPTION("Upstream replica can only be set for sorted tables");
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
        table->TableSchema(),
        schema,
        dynamic,
        table->IsEmpty());

    auto oldSchema = table->TableSchema();
    auto oldSchemaMode = table->GetSchemaMode();

    if (options.Schema) {
        table->TableSchema() = std::move(schema);
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
        table->TableSchema() = std::move(oldSchema);
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

    descriptors->push_back(TAttributeDescriptor("replicas")
        .SetOpaque(true));
}

bool TReplicatedTableNodeProxy::GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer)
{
    const auto* table = GetThisImpl<TReplicatedTableNode>();
    const auto& timestampProvider = Bootstrap_->GetTimestampProvider();

    if (key == "replicas") {
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

    return TTableNodeProxy::GetBuiltinAttribute(key, consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT


