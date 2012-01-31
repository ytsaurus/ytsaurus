#pragma once

#include "common.h"
#include "table_node.h"
#include "table_ypath.pb.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/chunk_server/chunk_manager.h>

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
        const NObjectServer::TTransactionId& transactionId,
        const NCypress::TNodeId& nodeId);

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const;

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode> TBase;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    virtual void GetSystemAttributes(yvector<NYTree::TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);

    virtual void DoInvoke(NRpc::IServiceContext* context);

    void TraverseChunkTree(
        yvector<NChunkServer::TChunkId>* chunkIds,
        const NChunkServer::TChunkTreeId& treeId);

    DECLARE_RPC_SERVICE_METHOD(NProto, GetChunkListForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

