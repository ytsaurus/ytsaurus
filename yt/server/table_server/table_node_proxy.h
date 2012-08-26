#pragma once

#include "table_node.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/public.h>

#include <ytlib/table_client/schema.h>

#include <ytlib/table_client/table_ypath.pb.h>

#include <server/cypress_server/node_proxy_detail.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NCypressServer::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode>
{
public:
    TTableNodeProxy(
        NCypressServer::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const NCypressServer::TNodeId& nodeId);

    virtual TResolveResult Resolve(const NYTree::TYPath& path, const Stroka& verb);
    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const;

private:
    typedef NCypressServer::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode> TBase;

    virtual void DoCloneTo(TTableNode* clonedNode);

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);
    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue);

    virtual void DoInvoke(NRpc::IServiceContextPtr context);

    void TraverseChunkTree(
        std::vector<NChunkClient::TChunkId>* chunkIds,
        const NChunkServer::TChunkList* chunkList);

    void TraverseChunkTree(
        const NChunkServer::TChunkList* chunkList,
        i64 lowerBound,
        TNullable<i64> upperBound,
        NTableClient::NProto::TRspFetch* response);

    void TraverseChunkTree(
        const NChunkServer::TChunkList* chunkList,
        const NTableClient::NProto::TKey& lowerBound,
        const NTableClient::NProto::TKey* upperBound,
        NTableClient::NProto::TRspFetch* response);

    void ParseYPath(
        const NYTree::TYPath& path,
        NTableClient::TChannel* channel,
        NTableClient::NProto::TReadLimit* lowerBound,
        NTableClient::NProto::TReadLimit* upperBound);

    NChunkServer::TChunkList* EnsureNodeMutable(TTableNode* node);
    void ClearNode(TTableNode* node);

    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, GetChunkListForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, Fetch);
    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, SetSorted);
    DECLARE_RPC_SERVICE_METHOD(NTableClient::NProto, Clear);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

