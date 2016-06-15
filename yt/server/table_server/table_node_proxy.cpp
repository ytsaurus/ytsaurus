#include "table_node_proxy.h"
#include "private.h"
#include "table_node.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_list.h>
#include <yt/server/chunk_server/chunk_owner_node_proxy.h>

#include <yt/server/node_tracker_server/node_directory_builder.h>

#include <yt/server/tablet_server/tablet.h>
#include <yt/server/tablet_server/tablet_cell.h>
#include <yt/server/tablet_server/tablet_manager.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/string.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/tree_builder.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NObjectServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NTransactionServer;
using namespace NTabletServer;
using namespace NNodeTrackerServer;

using NChunkClient::TChannel;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTableNode>
{
public:
    TTableNodeProxy(
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

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTableNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* table = GetThisTypedImpl();
        bool isDynamic = table->IsDynamic();

        descriptors->push_back(TAttributeDescriptor("row_count")
            .SetPresent(!isDynamic));
        descriptors->push_back(TAttributeDescriptor("unmerged_row_count")
            .SetPresent(isDynamic));
        descriptors->push_back("sorted");
        descriptors->push_back(TAttributeDescriptor("key_columns")
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("schema")
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor("sorted_by")
            .SetPresent(table->TableSchema().IsSorted()));
        descriptors->push_back("dynamic");
        descriptors->push_back(TAttributeDescriptor("tablet_count")
            .SetPresent(isDynamic));
        descriptors->push_back(TAttributeDescriptor("tablets")
            .SetPresent(isDynamic)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("tablet_statistics")
            .SetPresent(isDynamic)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("tablet_cell_bundle")
            .SetPresent(table->GetTabletCellBundle() != nullptr));
        descriptors->push_back("atomicity");
        descriptors->push_back(TAttributeDescriptor("optimize_for")
            .SetCustom(true));
        descriptors->push_back(TAttributeDescriptor("preserve_schema_on_write"));
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* table = GetThisTypedImpl();
        const auto* trunkTable = table->GetTrunkNode();
        auto statistics = table->ComputeTotalStatistics();

        auto tabletManager = Bootstrap_->GetTabletManager();

        if (key == "row_count" && !table->IsDynamic()) {
            BuildYsonFluently(consumer)
                .Value(statistics.row_count());
            return true;
        }

        if (key == "unmerged_row_count" && table->IsDynamic()) {
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

        if (key == "preserve_schema_on_write") {
            BuildYsonFluently(consumer)
                .Value(table->GetPreserveSchemaOnWrite());
            return true;
        }

        if (key == "sorted_by" && table->TableSchema().IsSorted()) {
            BuildYsonFluently(consumer)
                .Value(table->TableSchema().GetKeyColumns());
            return true;
        }

        if (key == "dynamic") {
            BuildYsonFluently(consumer)
                .Value(trunkTable->IsDynamic());
            return true;
        }

        if (key == "tablet_count" && trunkTable->IsDynamic()) {
            BuildYsonFluently(consumer)
                .Value(trunkTable->Tablets().size());
            return true;
        }

        if (key == "tablets" && trunkTable->IsDynamic()) {
            BuildYsonFluently(consumer)
                .DoListFor(trunkTable->Tablets(), [&] (TFluentList fluent, TTablet* tablet) {
                    auto* cell = tablet->GetCell();
                    fluent
                        .Item().BeginMap()
                            .Item("index").Value(tablet->GetIndex())
                            .Item("performance_counters").Value(tablet->PerformanceCounters())
                            .DoIf(table->IsSorted(), [&] (TFluentMap fluent) {
                                fluent.Item("pivot_key").Value(tablet->GetPivotKey());
                            })
                            .Item("state").Value(tablet->GetState())
                            .Item("statistics").Value(tabletManager->GetTabletStatistics(tablet))
                            .Item("tablet_id").Value(tablet->GetId())
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("cell_id").Value(cell->GetId());
                            })
                        .EndMap();
                });
            return true;
        }

        if (key == "tablet_statistics" && trunkTable->IsDynamic()) {
            TTabletStatistics tabletStatistics;
            for (const auto& tablet : trunkTable->Tablets()) {
                tabletStatistics += tabletManager->GetTabletStatistics(tablet);
            }
            BuildYsonFluently(consumer)
                .Value(tabletStatistics);
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

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    void AlterTable(const TNullable<TTableSchema>& newSchema, const TNullable<bool>& newDynamic)
    {
        auto* table = LockThisTypedImpl();

        if (newDynamic) {
            ValidateNoTransaction();
        }

        if (newSchema && table->HasMountedTablets()) {
            THROW_ERROR_EXCEPTION("Cannot change schema of a table with mounted tablets");
        }

        if (newSchema) {
            table->SetCustomSchema(*newSchema, newDynamic.Get(table->IsDynamic()));
        }

        if (newDynamic) {
            auto tabletManager = Bootstrap_->GetTabletManager();
            if (*newDynamic) {
                tabletManager->MakeTableDynamic(table);
            } else {
                tabletManager->MakeTableStatic(table);
            }
        }
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        if (key == "tablet_cell_bundle") {
            ValidateNoTransaction();

            auto name = ConvertTo<Stroka>(value);
            auto tabletManager = Bootstrap_->GetTabletManager();
            auto* cellBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name);

            auto* table = LockThisTypedImpl();
            tabletManager->SetTabletCellBundle(table, cellBundle);
            return true;
        }

        if (key == "atomicity") {
            ValidateNoTransaction();

            auto* table = LockThisTypedImpl();
            if (table->HasMountedTablets()) {
                THROW_ERROR_EXCEPTION("Cannot atomicity mode of a dynamic table with mounted tablets");
            }

            auto atomicity = ConvertTo<NTransactionClient::EAtomicity>(value);
            table->SetAtomicity(atomicity);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override
    {
        if (key == "optimize_for") {
            if (!newValue) {
                ThrowCannotRemoveAttribute(key);
            }
            ConvertTo<NTableClient::EOptimizeFor>(*newValue);
            return;
        }

        TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
    }

    virtual void ValidateFetchParameters(
        const TChannel& channel,
        const std::vector<TReadRange>& ranges) override
    {
        TChunkOwnerNodeProxy::ValidateFetchParameters(channel, ranges);

        const auto* table = GetThisTypedImpl();
        for (const auto& range : ranges) {
            const auto& lowerLimit = range.LowerLimit();
            const auto& upperLimit = range.UpperLimit();
            if ((upperLimit.HasKey() || lowerLimit.HasKey()) && !table->IsSorted()) {
                THROW_ERROR_EXCEPTION("Key selectors are not supported for unsorted tables");
            }
            if ((upperLimit.HasRowIndex() || lowerLimit.HasRowIndex()) && table->IsDynamic()) {
                THROW_ERROR_EXCEPTION("Row index selectors are not supported for dynamic tables");
            }
            if ((upperLimit.HasChunkIndex() || lowerLimit.HasChunkIndex()) && table->IsDynamic()) {
                THROW_ERROR_EXCEPTION("Chunk index selectors are not supported for dynamic tables");
            }
            if (upperLimit.HasOffset() || lowerLimit.HasOffset()) {
                THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
            }
        }
    }


    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Mount);
        DISPATCH_YPATH_SERVICE_METHOD(Unmount);
        DISPATCH_YPATH_SERVICE_METHOD(Remount);
        DISPATCH_YPATH_SERVICE_METHOD(Reshard);
        DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
        DISPATCH_YPATH_SERVICE_METHOD(Alter);
        return TBase::DoInvoke(context);
    }

    virtual void ValidateBeginUpload() override
    {
        TBase::ValidateBeginUpload();

        const auto* table = GetThisTypedImpl();
        if (table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot upload into a dynamic table");
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Mount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto cellId = FromProto<TTabletCellId>(request->cell_id());

        context->SetRequestInfo(
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v",
            firstTabletIndex,
            lastTabletIndex,
            cellId);

        ValidateNotExternal();
        ValidateNoTransaction();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Mount);

        auto tabletManager = Bootstrap_->GetTabletManager();

        TTabletCell* cell = nullptr;
        if (cellId) {
            cell = tabletManager->GetTabletCellOrThrow(cellId);
        }

        auto* table = LockThisTypedImpl();

        tabletManager->MountTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            cell);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unmount)
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

        auto* table = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnmountTable(
            table,
            force,
            firstTabletIndex,
            lastTabletIndex);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Remount)
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

        auto* table = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->RemountTable(
            table,
            firstTabletIndex,
            lastTabletIndex);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Reshard)
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

        auto* table = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->ReshardTable(
            table,
            firstTabletIndex,
            lastTabletIndex,
            tabletCount,
            pivotKeys);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo)
    {
        DeclareNonMutating();

        context->SetRequestInfo();

        ValidateNotExternal();
        ValidateNoTransaction();

        auto* table = GetThisTypedImpl();

        ToProto(response->mutable_table_id(), table->GetId());
        response->set_dynamic(table->IsDynamic());
        ToProto(response->mutable_schema(), table->TableSchema());

        yhash_set<TTabletCell*> cells;
        for (auto* tablet : table->Tablets()) {
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

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Alter)
    {
        DeclareMutating();
        auto newSchema = request->has_schema()
            ? MakeNullable(FromProto<TTableSchema>(request->schema()))
            : Null;
        auto newDynamic = request->has_dynamic()
            ? MakeNullable(request->dynamic())
            : Null;

        context->SetRequestInfo("Schema: %v, Dynamic: %v",
            newSchema,
            newDynamic);

        AlterTable(newSchema, newDynamic);

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateTableNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TTableNode* trunkNode)
{
    return New<TTableNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT


