#pragma once

#include "file_node.h"

#include <ytlib/file_client/file_ypath.pb.h>

#include <ytlib/ytree/ypath_service.h>

#include <server/cypress_server/node_proxy_detail.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public NCypressServer::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode>
{
public:
    TFileNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        NCypressServer::ICypressNode* trunkNode);

    bool IsExecutable();
    Stroka GetFileName();

private:
    typedef NCypressServer::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override;
    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer) const override;
    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NFileClient::NProto, FetchFile);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

