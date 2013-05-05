#include "stdafx.h"
#include "table_node_proxy.h"
#include "table_node.h"
#include "private.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/serialize.h>

#include <ytlib/erasure/codec.h>

#include <ytlib/ytree/tree_builder.h>
#include <ytlib/ytree/ephemeral_node_factory.h>

#include <ytlib/ypath/token.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_owner_node_proxy.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NTableServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NCypressServer;
using namespace NYTree;
using namespace NYson;
using namespace NRpc;
using namespace NObjectServer;
using namespace NTableClient;
using namespace NCellMaster;
using namespace NTransactionServer;
using namespace NSecurityServer;

using NChunkClient::NProto::TReadLimit;
using NChunkClient::NProto::TKey;

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public TChunkOwnerNodeProxy<TTableNode>
{
public:
    TTableNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TTableNode* trunkNode);

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

private:
    typedef TChunkOwnerNodeProxy<TTableNode> TBase;


    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;

    virtual NCypressClient::ELockMode GetLockMode(NChunkClient::EUpdateMode updateMode) override;
    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, SetSorted);

};

////////////////////////////////////////////////////////////////////////////////

TTableNodeProxy::TTableNodeProxy(
    INodeTypeHandlerPtr typeHandler,
    TBootstrap* bootstrap,
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
    TBase::ListSystemAttributes(attributes);
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
            .Value(!chunkList->SortedBy().empty());
        return true;
    }

    if (!chunkList->SortedBy().empty()) {
        if (key == "sorted_by") {
            BuildYsonFluently(consumer)
                .List(chunkList->SortedBy());
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

    TBase::ValidateUserAttributeUpdate(key, oldValue, newValue);
}

NCypressClient::ELockMode TTableNodeProxy::GetLockMode(NChunkClient::EUpdateMode updateMode)
{
    if (updateMode == NChunkClient::EUpdateMode::Append) {
        return NCypressClient::ELockMode::Shared;
    } else {
        return NCypressClient::ELockMode::Exclusive;
    }
}

DEFINE_RPC_SERVICE_METHOD(TTableNodeProxy, SetSorted)
{
    auto keyColumns = FromProto<Stroka>(request->key_columns());
    context->SetRequestInfo("KeyColumns: %s", ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());

    ValidatePermission(EPermissionCheckScope::This, EPermission::Write);

    auto* node = LockThisTypedImpl();

    if (node->GetUpdateMode() != NChunkClient::EUpdateMode::Overwrite) {
        THROW_ERROR_EXCEPTION("Table node must be in overwrite mode");
    }

    node->GetChunkList()->SortedBy() = keyColumns;

    SetModified();

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateTableNodeProxy(
    NCypressServer::INodeTypeHandlerPtr typeHandler,
    NCellMaster::TBootstrap* bootstrap,
    NTransactionServer::TTransaction* transaction,
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

