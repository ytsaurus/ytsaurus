#pragma once

#include "common.h"
#include "file_node.h"
#include <ytlib/file_server/file_ypath.pb.h>

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode>
{
public:
    typedef TIntrusivePtr<TFileNodeProxy> TPtr;

    TFileNodeProxy(
        NCypress::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const NCypress::TNodeId& nodeId);

    bool IsExecutable();
    Stroka GetFileName();

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode> TBase;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);

    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYson>& oldValue,
        const TNullable<NYTree::TYson>& newValue);

    virtual void DoInvoke(NRpc::IServiceContextPtr context);

    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

