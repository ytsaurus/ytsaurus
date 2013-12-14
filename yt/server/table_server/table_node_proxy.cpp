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

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/chunk_client/read_limit.h>

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
using namespace NChunkClient::NProto;
using namespace NCypressServer;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NTransactionServer;
using namespace NTabletServer;

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
        TTableNode* trunkNode);

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTableNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) override;
    bool SetSystemAttribute(const Stroka& key, const TYsonString& value) override;
    
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override;
    virtual void ValidatePathAttributes(
        const TNullable<TChannel>& channel,
        const TReadLimit& upperLimit,
        const TReadLimit& lowerLimit) override;
    virtual void ValidatePrepareForUpdate() override;

    virtual NCypressClient::ELockMode GetLockMode(EUpdateMode updateMode) override;
    virtual bool DoInvoke(IServiceContextPtr context) override;

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, SetSorted);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Mount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unmount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Reshard);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, GetMountInfo);

};

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
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

bool TTableNodeProxy::DoInvoke(IServiceContextPtr context)
{
    DISPATCH_YPATH_SERVICE_METHOD(SetSorted);
    DISPATCH_YPATH_SERVICE_METHOD(Mount);
    DISPATCH_YPATH_SERVICE_METHOD(Unmount);
    DISPATCH_YPATH_SERVICE_METHOD(Reshard);
    DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
    return TBase::DoInvoke(context);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* node = GetThisTypedImpl();

    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back(TAttributeInfo("sorted_by", node->IsSorted()));
    attributes->push_back(TAttributeInfo("tablets", true, true));
    TBase::ListSystemAttributes(attributes);
}

void TTableNodeProxy::ValidatePathAttributes(
    const TNullable<TChannel>& channel,
    const TReadLimit& upperLimit,
    const TReadLimit& lowerLimit)
{
    TChunkOwnerNodeProxy::ValidatePathAttributes(
        channel,
        upperLimit,
        lowerLimit);

    if (upperLimit.HasOffset() || lowerLimit.HasOffset()) {
        THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
    }
}

void TTableNodeProxy::ValidatePrepareForUpdate()
{
    TChunkOwnerNodeProxy::ValidatePrepareForUpdate();

    auto* node = GetThisTypedImpl();
    if (!node->Tablets().empty()) {
        THROW_ERROR_EXCEPTION("Cannot write into a table with tablets");
    }
}

bool TTableNodeProxy::GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer)
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();
    const auto& statistics = chunkList->Statistics();

    if (key == "row_count") {
        BuildYsonFluently(consumer)
            .Value(statistics.RowCount);
        return true;
    }

    if (key == "sorted") {
        BuildYsonFluently(consumer)
            .Value(node->IsSorted());
        return true;
    }

    if (node->IsSorted()) {
        if (key == "sorted_by") {
            BuildYsonFluently(consumer)
                .List(chunkList->SortedBy());
            return true;
        }
    }

    if (key == "tablets") {
        BuildYsonFluently(consumer)
            .DoListFor(node->Tablets(), [] (TFluentList fluent, TTablet* tablet) {
                auto* cell = tablet->GetCell();
                fluent
                    .Item().BeginMap()
                        .Item("tablet_id").Value(tablet->GetId())
                        .Item("state").Value(tablet->GetState())
                        .Item("pivot_key").Value(tablet->PivotKey())
                        .DoIf(cell, [&] (TFluentMap fluent) {
                            fluent
                                .Item("cell_id").Value(cell->GetId());
                        })
                    .EndMap();
            });
        return true;
    }

    return TBase::GetSystemAttribute(key, consumer);
}

bool TTableNodeProxy::SetSystemAttribute(const Stroka& key, const TYsonString& value)
{
    if (key == "sorted_by") {
        ValidateNoTransaction();

        auto* node = LockThisTypedImpl();
        auto* chunkList = node->GetChunkList();
        if (!chunkList->Children().empty() || !chunkList->Parents().empty()) {
            THROW_ERROR_EXCEPTION("Operation is not supported");
        }

        chunkList->SortedBy() = ConvertTo<TKeyColumns>(value);
        return true;
    }

    return TBase::SetSystemAttribute(key, value);
}

void TTableNodeProxy::ValidateUserAttributeUpdate(
    const Stroka& key,
    const TNullable<TYsonString>& oldValue,
    const TNullable<TYsonString>& newValue)
{
    UNUSED(oldValue);

    if (key == "channels") {
        if (!newValue) {
            ThrowCannotRemoveAttribute(key);
        }
        ConvertTo<TChannels>(newValue.Get());
        return;
    }

    if (key == "schema") {
        if (!newValue) {
            ThrowCannotRemoveAttribute(key);
        }
        ConvertTo<TTableSchema>(newValue.Get());
        return;
    }

    TBase::ValidateUserAttributeUpdate(key, oldValue, newValue);
}

ELockMode TTableNodeProxy::GetLockMode(NChunkClient::EUpdateMode updateMode)
{
    return updateMode == EUpdateMode::Append
        ? ELockMode::Shared
        : ELockMode::Exclusive;
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    DeclareMutating();

    auto keyColumns = FromProto<Stroka>(request->key_columns());
    context->SetRequestInfo("SortedBy: %s",
        ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = LockThisTypedImpl();

    if (node->GetUpdateMode() != EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Table node must be in \"overwrite\" mode");
    }

    node->GetChunkList()->SortedBy() = keyColumns;

    SetModified();

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Mount)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->first_tablet_index();
    context->SetRequestInfo("FirstTabletIndex: %d, LastTabletIndex: %d",
        firstTabletIndex,
        lastTabletIndex);

    ValidateNoTransaction();

    auto* impl = LockThisTypedImpl();

    auto tabletManager = Bootstrap->GetTabletManager();
    tabletManager->MountTable(
        impl,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Unmount)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->first_tablet_index();
    context->SetRequestInfo("FirstTabletIndex: %d, LastTabletIndex: %d",
        firstTabletIndex,
        lastTabletIndex);

    ValidateNoTransaction();

    auto* impl = LockThisTypedImpl();

    auto tabletManager = Bootstrap->GetTabletManager();
    tabletManager->UnmountTable(
        impl,
        firstTabletIndex,
        lastTabletIndex);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Reshard)
{
    DeclareMutating();

    int firstTabletIndex = request->first_tablet_index();
    int lastTabletIndex = request->first_tablet_index();
    auto pivotKeys = FromProto<NVersionedTableClient::TOwningKey>(request->pivot_keys());
    context->SetRequestInfo("FirstTabletIndex: %d, LastTabletIndex: %d, PivotKeyCount: %d",
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

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, GetMountInfo)
{
    DeclareNonMutating();

    context->SetRequestInfo("");

    ValidateNoTransaction();

    auto* impl = GetThisTypedImpl();

    ToProto(response->mutable_table_id(), impl->GetId());

    auto tabletManager = Bootstrap->GetTabletManager();
    auto schema = tabletManager->GetTableSchema(impl);
    ToProto(response->mutable_schema(), schema);

    const auto* chunkList = impl->GetChunkList();
    ToProto(response->mutable_key_columns()->mutable_names(), chunkList->SortedBy());

    for (auto* tablet : impl->Tablets()) {
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->add_tablets();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        protoTablet->set_state(tablet->GetState());
        ToProto(protoTablet->mutable_pivot_key(), tablet->PivotKey());
        if (cell) {
            ToProto(protoTablet->mutable_cell_id(), cell->GetId());
            protoTablet->mutable_cell_config()->CopyFrom(cell->Config());
        }
    }

    context->SetRequestInfo("TabletCount: %d",
        static_cast<int>(impl->Tablets().size()));
    context->Reply();
}

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

