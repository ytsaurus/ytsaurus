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

using NChunkClient::TChannel;

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
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override;

    virtual void ValidatePathAttributes(
        const TNullable<TChannel>& channel,
        const TReadLimit& upperLimit,
        const TReadLimit& lowerLimit) override;

    virtual NCypressClient::ELockMode GetLockMode(EUpdateMode updateMode) override;
    virtual bool DoInvoke(IServiceContextPtr context) override;

    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, SetSorted);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Mount);
    DECLARE_YPATH_SERVICE_METHOD(NTableClient::NProto, Unmount);
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
    DISPATCH_YPATH_SERVICE_METHOD(GetMountInfo);
    return TBase::DoInvoke(context);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* node = GetThisTypedImpl();

    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back(TAttributeInfo("sorted_by", node->IsSorted()));
    attributes->push_back("mounted");
    attributes->push_back(TAttributeInfo("tablet_id", node->IsMounted()));
    TBase::ListSystemAttributes(attributes);
}

void TTableNodeProxy::ValidatePathAttributes(
    const TNullable<TChannel>& channel,
    const TReadLimit& upperLimit,
    const TReadLimit& lowerLimit)
{
    UNUSED(channel);

    if (upperLimit.has_offset() || lowerLimit.has_offset()) {
        THROW_ERROR_EXCEPTION("Offset selectors are not supported for tables");
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

    if (key == "mounted") {
        BuildYsonFluently(consumer)
            .Value(node->IsMounted());
        return true;
    }

    if (node->IsMounted()) {
        if (key == "tablet_id") {
            BuildYsonFluently(consumer)
                .Value(node->GetTablet()->GetId());
            return true;
        }
    }

    return TBase::GetSystemAttribute(key, consumer);
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

    context->SetRequestInfo("");

    ValidateNoTransaction();

    auto* impl = LockThisTypedImpl();

    auto tabletManager = Bootstrap->GetTabletManager();
    tabletManager->MountTable(impl);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, Unmount)
{
    DeclareMutating();

    context->SetRequestInfo("");

    ValidateNoTransaction();

    auto* impl = LockThisTypedImpl();

    auto tabletManager = Bootstrap->GetTabletManager();
    tabletManager->UnmountTable(impl);

    context->Reply();
}

DEFINE_YPATH_SERVICE_METHOD(TTableNodeProxy, GetMountInfo)
{
    DeclareNonMutating();

    context->SetRequestInfo("");

    ValidateNoTransaction();

    auto* impl = GetThisTypedImpl();
    auto proxy = GetProxy(impl);

    ToProto(response->mutable_table_id(), impl->GetId());

    // COMPAT(babenko): schema must be mandatory
    auto schema = proxy->Attributes().Get<TTableSchema>("schema", TTableSchema());
    ToProto(response->mutable_schema(), schema);

    const auto* chunkList = impl->GetChunkList();
    ToProto(response->mutable_key_columns()->mutable_names(), chunkList->SortedBy());

    auto* tablet = impl->GetTablet();
    if (tablet) {
        auto* cell = tablet->GetCell();
        auto* protoTablet = response->mutable_tablet();
        ToProto(protoTablet->mutable_tablet_id(), tablet->GetId());
        ToProto(protoTablet->mutable_cell_id(), cell->GetId());
        protoTablet->mutable_cell_config()->CopyFrom(cell->Config());
    }

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

