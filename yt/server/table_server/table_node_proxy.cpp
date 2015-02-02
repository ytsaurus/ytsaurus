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

#include <ytlib/table_client/table_ypath_proxy.h>

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
using namespace NTableClient;
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

    virtual NLog::TLogger CreateLogger() const override
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
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* table = GetThisTypedImpl();
        auto tabletManager = Bootstrap->GetTabletManager();

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
                            .Item("tablet_id").Value(tablet->GetId())
                            .Item("statistics").Value(tabletManager->GetTabletStatistics(tablet))
                            .Item("state").Value(tablet->GetState())
                            .Item("pivot_key").Value(tablet->GetPivotKey())
                            .DoIf(cell, [&] (TFluentMap fluent) {
                                fluent.Item("cell_id").Value(cell->GetId());
                            })
                        .EndMap();
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(const Stroka& key, const TYsonString& value) override
    {
        if (key == "key_columns") {
            ValidateNoTransaction();

            auto* table = LockThisTypedImpl();
            if (!table->Tablets().empty()) {
                THROW_ERROR_EXCEPTION("Table already has some configured tablets");
            }

            auto* chunkList = table->GetChunkList();
            if (!chunkList->Children().empty()) {
                THROW_ERROR_EXCEPTION("Table is not empty");
            }

            if (!chunkList->Parents().empty()) {
                THROW_ERROR_EXCEPTION("Table data is shared");
            }

            auto keyColumns = ConvertTo<TKeyColumns>(value);
            ValidateKeyColumns(keyColumns);
            table->KeyColumns() = keyColumns;
            table->SetSorted(!table->KeyColumns().empty());
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

    virtual NCypressClient::ELockMode GetLockMode(EUpdateMode updateMode) override
    {
        return updateMode == EUpdateMode::Append
            ? ELockMode::Shared
            : ELockMode::Exclusive;
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

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, SetSorted)
    {
        DeclareMutating();

        auto keyColumns = FromProto<Stroka>(request->key_columns());
        context->SetRequestInfo("KeyColumns: %v",
            ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

        auto* table = LockThisTypedImpl();

        if (table->GetUpdateMode() != EUpdateMode::Overwrite) {
            THROW_ERROR_EXCEPTION("Table must be in \"overwrite\" mode");
        }

        ValidateKeyColumns(keyColumns);
        table->KeyColumns() = keyColumns;
        table->SetSorted(true);

        SetModified();

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Mount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->last_tablet_index();
        auto cellId = request->has_cell_id() ? FromProto<TTabletCellId>(request->cell_id()) : NullTabletCellId;
        context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, CellId: %v",
            firstTabletIndex,
            lastTabletIndex,
            cellId);

        ValidateNoTransaction();

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap->GetTabletManager();
        tabletManager->MountTable(
            impl,
            firstTabletIndex,
            lastTabletIndex,
            cellId);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unmount)
    {
        DeclareMutating();

        int firstTabletIndex = request->first_tablet_index();
        int lastTabletIndex = request->first_tablet_index();
        bool force = request->force();
        context->SetRequestInfo("FirstTabletIndex: %v, LastTabletIndex: %v, Force: %v",
            firstTabletIndex,
            lastTabletIndex,
            force);

        ValidateNoTransaction();

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap->GetTabletManager();
        tabletManager->UnmountTable(
            impl,
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

        ValidateNoTransaction();

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap->GetTabletManager();
        tabletManager->RemountTable(
            impl,
            firstTabletIndex,
            lastTabletIndex);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Reshard)
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

        auto* impl = LockThisTypedImpl();

        auto tabletManager = Bootstrap->GetTabletManager();
        tabletManager->ReshardTable(
            impl,
            firstTabletIndex,
            lastTabletIndex,
            pivotKeys);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo)
    {
        DeclareNonMutating();

        context->SetRequestInfo();

        ValidateNoTransaction();

        auto* table = GetThisTypedImpl();

        ToProto(response->mutable_table_id(), table->GetId());
        ToProto(response->mutable_key_columns()->mutable_names(), table->KeyColumns());
        response->set_sorted(table->GetSorted());

        auto tabletManager = Bootstrap->GetTabletManager();
        auto schema = tabletManager->GetTableSchema(table);
        ToProto(response->mutable_schema(), schema);

        TNodeDirectoryBuilder builder(response->mutable_node_directory());

        for (auto* tablet : table->Tablets()) {
            auto* cell = tablet->GetCell();
            auto* protoTablet = response->add_tablets();
            ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
            protoTablet->set_state(static_cast<int>(tablet->GetState()));
            ToProto(protoTablet->mutable_pivot_key(), tablet->GetPivotKey());
            if (cell) {
                auto config = cell->GetConfig()->ToElection(cell->GetId());
                protoTablet->set_cell_config_version(cell->GetConfigVersion());
                protoTablet->set_cell_config(ConvertToYsonString(config).Data());
                for (const auto& peer : cell->Peers()) {
                    auto* node = peer.Node;
                    if (node) {
                        const auto* slot = node->GetTabletSlot(cell);
                        if (slot->PeerState == EPeerState::Leading) {
                            builder.Add(node);
                            protoTablet->add_replica_node_ids(node->GetId());
                        }
                    }
                }
            }
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

