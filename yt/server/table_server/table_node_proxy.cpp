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

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_owner_node_proxy.h>

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

    virtual bool IsWriteRequest(IServiceContextPtr context) const override;

private:
    typedef TCypressNodeProxyBase<TChunkOwnerNodeProxy, IEntityNode, TTableNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override;
    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& oldValue,
        const TNullable<TYsonString>& newValue) override;

    virtual void ValidatePathAttributes(
        const TNullable<TChannel>& channel,
        const TReadLimit& upperLimit,
        const TReadLimit& lowerLimit) override;

    virtual NCypressClient::ELockMode GetLockMode(EUpdateMode updateMode) override;
    virtual bool DoInvoke(IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, SetSorted);

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
    return TBase::DoInvoke(context);
}

bool TTableNodeProxy::IsWriteRequest(IServiceContextPtr context) const
{
    DECLARE_YPATH_SERVICE_WRITE_METHOD(SetSorted);
    return TBase::IsWriteRequest(context);
}

void TTableNodeProxy::ListSystemAttributes(std::vector<TAttributeInfo>* attributes)
{
    const auto* node = GetThisTypedImpl();
    const auto* chunkList = node->GetChunkList();

    attributes->push_back("row_count");
    attributes->push_back("sorted");
    attributes->push_back(TAttributeInfo("sorted_by", !chunkList->SortedBy().empty()));
    // Custom system attributes
    attributes->push_back(TAttributeInfo("compression_codec", true, false, true));
    attributes->push_back(TAttributeInfo("erasure_codec", true, false, true));
    attributes->push_back(TAttributeInfo("channels", true, false, true));
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

bool TTableNodeProxy::GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer)
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
            .Value(!chunkList->SortedBy().empty());
        return true;
    }

    if (!chunkList->SortedBy().empty()) {
        if (key == "sorted_by") {
            BuildYsonFluently(consumer)
                .Value(chunkList->SortedBy());
            return true;
        }
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

void TTableNodeProxy::ValidateCustomAttributeUpdate(
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

    TBase::ValidateCustomAttributeUpdate(key, oldValue, newValue);
}

ELockMode TTableNodeProxy::GetLockMode(NChunkClient::EUpdateMode updateMode)
{
    return updateMode == EUpdateMode::Append
        ? ELockMode::Shared
        : ELockMode::Exclusive;
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    auto keyColumns = FromProto<Stroka>(request->key_columns());
    context->SetRequestInfo("KeyColumns: %s", ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = LockThisTypedImpl();

    if (node->GetUpdateMode() != EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Table node must be in \"overwrite\" mode");
    }

    node->GetChunkList()->SortedBy() = keyColumns;

    SetModified();

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

