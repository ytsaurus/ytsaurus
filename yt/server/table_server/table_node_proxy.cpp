#include "stdafx.h"
#include "stdafx.h"
#include "table_node_proxy.h"
#include "table_node.h"
#include "private.h"

#include <core/misc/string.h>
#include <core/misc/serialize.h>

#include <core/erasure/codec.h>

#include <core/ytree/tree_builder.h>
#include <core/ytree/ephemeral_node_factory.h>

#include <core/ypath/token.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/new_table_client/table_ypath_proxy.h>
#include <ytlib/new_table_client/schema.h>

#include <ytlib/chunk_client/read_limit.h>

#include <server/node_tracker_server/node_directory_builder.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_owner_node_proxy.h>

#include <server/tablet_server/tablet_manager.h>
#include <server/tablet_server/tablet.h>
#include <server/tablet_server/tablet_cell.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;
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
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        TTransaction* transaction,
        TTableNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTableNode> TBase;

    virtual NLogging::TLogger CreateLogger() const override
    {
        return TableServerLogger;
    }


    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* table = GetThisTypedImpl();

        attributes->push_back(TAttributeInfo("row_count", !table->IsDynamic()));
        attributes->push_back(TAttributeInfo("unmerged_row_count", table->IsDynamic()));
        attributes->push_back("sorted");
        attributes->push_back("key_columns");
        attributes->push_back(TAttributeInfo("sorted_by", table->GetSorted()));
        attributes->push_back("dynamic");
        attributes->push_back(TAttributeInfo("tablets", table->IsDynamic(), true));
        attributes->push_back(TAttributeInfo("channels", true, false, true));
        attributes->push_back(TAttributeInfo("schema", true, false, true));
        attributes->push_back("atomicity");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* table = GetThisTypedImpl();
        auto tabletManager = Bootstrap_->GetTabletManager();

        const auto* chunkList = table->GetChunkList();
        const auto& statistics = chunkList->Statistics();

        if (key == "row_count" && !table->IsDynamic()) {
            BuildYsonFluently(consumer)
                .Value(statistics.RowCount);
            return true;
        }

        if (key == "unmerged_row_count" && table->IsDynamic()) {
            BuildYsonFluently(consumer)
                .Value(statistics.RowCount);
            return true;
        }

        if (key == "sorted") {
            BuildYsonFluently(consumer)
                .Value(table->GetSorted());
            return true;
        }

        if (key == "key_columns") {
            BuildYsonFluently(consumer)
                .Value(table->KeyColumns());
            return true;
        }

        if (key == "sorted_by" && table->GetSorted()) {
            BuildYsonFluently(consumer)
                .Value(table->KeyColumns());
            return true;
        }

        if (key == "dynamic") {
            BuildYsonFluently(consumer)
                .Value(table->IsDynamic());
            return true;
        }

        if (key == "tablets" && table->IsDynamic()) {
            BuildYsonFluently(consumer)
                .DoListFor(table->Tablets(), [&] (TFluentList fluent, TTablet* tablet) {
                    auto* cell = tablet->GetCell();
                    fluent
                        .Item().BeginMap()
                            .Item("index").Value(tablet->GetIndex())
                            .Item("performance_counters").Value(tablet->PerformanceCounters())
                            .Item("pivot_key").Value(tablet->GetPivotKey())
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

        if (key == "atomicity") {
            BuildYsonFluently(consumer)
                .Value(table->GetAtomicity());
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        if (key == "key_columns") {
            auto keyColumns = ConvertTo<TKeyColumns>(value);

            ValidateNoTransaction();

            auto* table = LockThisTypedImpl();
            auto* chunkList = table->GetChunkList();
            if (table->IsDynamic()) {
                if (table->HasMountedTablets()) {
                    THROW_ERROR_EXCEPTION("Cannot change key columns of a dynamic table with mounted tablets");
                }
            } else {
                if (!chunkList->Children().empty()) {
                    THROW_ERROR_EXCEPTION("Cannot change key columns of a non-empty static table");
                }
            }

            ValidateKeyColumnsUpdate(table->KeyColumns(), keyColumns);

            table->KeyColumns() = keyColumns;
            if (!table->IsDynamic() && !keyColumns.empty()) {
                table->SetSorted(true);
            }
            return true;
        }

        if (key == "atomicity") {
            auto atomicity = ConvertTo<NTransactionClient::EAtomicity>(value);

            ValidateNoTransaction();

            auto* table = LockThisTypedImpl();
            if (table->HasMountedTablets()) {
                THROW_ERROR_EXCEPTION("Cannot atomicity mode of a dynamic table with mounted tablets");
            }

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
        const auto* table = GetThisTypedImpl();

        if (key == "channels") {
            if (!newValue) {
                ThrowCannotRemoveAttribute(key);
            }

            ConvertTo<TChannels>(*newValue);

            return;
        }

        if (key == "schema") {
            if (!newValue) {
                ThrowCannotRemoveAttribute(key);
            }

            ConvertTo<TTableSchema>(*newValue);

            if (table->HasMountedTablets()) {
                THROW_ERROR_EXCEPTION("Table has mounted tablets");
            }
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
            if ((upperLimit.HasKey() || lowerLimit.HasKey()) && !table->GetSorted()) {
                THROW_ERROR_EXCEPTION("Cannot fetch a range of an unsorted table");
            }
            if (upperLimit.HasOffset() || lowerLimit.HasOffset()) {
                THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
            }
        }
    }


    virtual void Clear() override
    {
        TChunkOwnerNodeProxy::Clear();

        auto* table = GetThisTypedImpl();
        table->KeyColumns().clear();
        table->SetSorted(false);
    }


    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
        DISPATCH_YPATH_SERVICE_METHOD(Mount);
        DISPATCH_YPATH_SERVICE_METHOD(Unmount);
        DISPATCH_YPATH_SERVICE_METHOD(Remount);
        DISPATCH_YPATH_SERVICE_METHOD(Reshard);
        DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
        return TBase::DoInvoke(context);
    }


    virtual void ValidateFetch() override
    {
        TBase::ValidateFetch();

        const auto* table = GetThisTypedImpl();
        if (table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot fetch a dynamic table");
        }
    }

    virtual void ValidatePrepareForUpdate() override
    {
        TBase::ValidatePrepareForUpdate();

        const auto* table = GetThisTypedImpl();
        if (table->IsDynamic()) {
            THROW_ERROR_EXCEPTION("Cannot write into a dynamic table");
        }
    }

    virtual bool IsSorted() override
    {
        const auto* table = GetThisTypedImpl();
        return table->GetSorted();
    }

    virtual void ResetSorted() override
    {
        auto* table = GetThisTypedImpl();
        table->KeyColumns().clear();
        table->SetSorted(false);
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, SetSorted)
    {
        DeclareMutating();

        auto keyColumns = FromProto<Stroka>(request->key_columns());
        context->SetRequestInfo("KeyColumns: %v",
            ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* table = LockThisTypedImpl();

        if (table->GetUpdateMode() == EUpdateMode::None) {
            THROW_ERROR_EXCEPTION("Table must not be in \"none\" mode");
        }

        ValidateKeyColumns(keyColumns);
        table->KeyColumns() = keyColumns;
        table->SetSorted(true);

        SetModified();

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, Mount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto cellId = request->has_cell_id() ? FromProto<TTabletCellId>(request->cell_id()) : NullTabletCellId;
        i64 estimatedUncompressedSize = request->estimated_uncompressed_size();
        i64 estimatedCompressedSize = request->estimated_compressed_size();
        context->SetRequestInfo(
            "FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v, "
            "EstimatedUncompressedSize: %v, EsimatedCompressedSize: %v",
            firstTabletIndex,
            lastTabletIndex,
            cellId);

        ValidateNoTransaction();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->MountTable(
            impl,
            firstTabletIndex,
            lastTabletIndex,
            cellId,
            estimatedUncompressedSize,
            estimatedCompressedSize);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, Unmount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        bool force = request->force();
        context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, Force: %v",
            firstTabletIndex,
            lastTabletIndex,
            force);

        ValidateNoTransaction();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->UnmountTable(
            impl,
            force,
            firstTabletIndex,
            lastTabletIndex);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, Remount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->first_tablet_index();
        context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v",
            firstTabletIndex,
            lastTabletIndex);

        ValidateNoTransaction();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->RemountTable(
            impl,
            firstTabletIndex,
            lastTabletIndex);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, Reshard)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto pivotKeys = FromProto<NVersionedTableClient::TOwningKey>(request->pivot_keys());
        context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, PivotKeyCount: %v",
            firstTabletIndex,
            lastTabletIndex,
            static_cast<int>(pivotKeys.size()));

        ValidateNoTransaction();
        ValidatePermission(EPermissionCheckScope::This, EPermission::Administer);

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap_->GetTabletManager();
        tabletManager->ReshardTable(
            impl,
            firstTabletIndex,
            lastTabletIndex,
            pivotKeys);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NVersionedTableClient::NProto, GetMountInfo)
    {
        DeclareNonMutating();

        context->SetRequestInfo();

        ValidateNoTransaction();

        auto* table = GetThisTypedImpl();

        ToProto(response->mutable_table_id(), table->GetId());
        ToProto(response->mutable_key_columns()->mutable_names(), table->KeyColumns());
        response->set_sorted(table->GetSorted());

        auto tabletManager = Bootstrap_->GetTabletManager();
        auto schema = tabletManager->GetTableSchema(table);
        ToProto(response->mutable_schema(), schema);

        yhash_set<TTabletCell*> cells;
        for (auto* tablet : table->Tablets()) {
            auto* cell = tablet->GetCell();
            auto* protoTablet = response->add_tablets();
            ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
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

};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateTableNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    TTransaction* transaction,
    TTableNode* trunkNode)
{
    return New<TTableNodeProxy>(
        typeHandler,
        bootstrap,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

