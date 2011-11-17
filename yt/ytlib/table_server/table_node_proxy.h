#pragma once

#include "common.h"
#include "table_node.h"
#include "table_ypath_rpc.pb.h"

#include "../ytree/ypath_service.h"
#include "../cypress/node_proxy_detail.h"
#include "../chunk_server/chunk_manager.h"

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode>
{
public:
    typedef TIntrusivePtr<TTableNodeProxy> TPtr;

    TTableNodeProxy(
        NCypress::INodeTypeHandler* typeHandler,
        NCypress::TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager,
        const NTransaction::TTransactionId& transactionId,
        const NCypress::TNodeId& nodeId);

    virtual bool IsOperationLogged(NYTree::TYPath path, const Stroka& verb) const;

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode> TBase;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    virtual void DoInvoke(NRpc::IServiceContext* context);

    RPC_SERVICE_METHOD_DECL(NProto, AddTableChunks);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

