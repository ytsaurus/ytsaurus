#pragma once

#include "common.h"
#include "file_node.h"
#include "file_ypath.pb.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/chunk_server/chunk_manager.h>

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode>
{
public:
    typedef TIntrusivePtr<TFileNodeProxy> TPtr;

    TFileNodeProxy(
        NCypress::INodeTypeHandler* typeHandler,
        NCypress::TCypressManager* cypressManager,
        NChunkServer::TChunkManager* chunkManager,
        const NTransactionServer::TTransactionId& transactionId,
        const NCypress::TNodeId& nodeId);

    bool IsExecutable();
    Stroka GetFileName();

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode> TBase;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    virtual void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

