#pragma once

#include "common.h"
#include "file_node.h"
#include "file_ypath_rpc.pb.h"

#include "../ytree/ypath_service.h"
#include "../cypress/node_proxy_detail.h"
#include "../chunk_server/chunk_manager.h"

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

    virtual bool IsLogged(NRpc::IServiceContext* context) const;

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TFileNode> TBase;

    NChunkServer::TChunkManager::TPtr ChunkManager;

    virtual void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NProto, GetFileChunk);
    DECLARE_RPC_SERVICE_METHOD(NProto, SetFileChunk);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

