#pragma once

#include "table_node.h"
#include <ytlib/table_server/table_ypath.pb.h>

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/public.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NTableServer {

////////////////////////////////////////////////////////////////////////////////

class TTableNodeProxy
    : public NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode>
{
public:
    TTableNodeProxy(
        NCypress::INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const NCypress::TNodeId& nodeId);

    virtual TResolveResult Resolve(const NYTree::TYPath& path, const Stroka& verb);
    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const;

private:
    typedef NCypress::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode> TBase;

    virtual void DoCloneTo(TTableNode* clonedNode);

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer);
    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue);

    virtual void DoInvoke(NRpc::IServiceContextPtr context);

    void TraverseChunkTree(
        std::vector<NChunkServer::TChunkId>* chunkIds,
        const NChunkServer::TChunkList* chunkList);

    void TraverseChunkTree(
        const NChunkServer::TChunkList* chunkList,
        i64 lowerBound,
        TNullable<i64> upperBound,
        NProto::TRspFetch* response);

    void TraverseChunkTree(
        const NChunkServer::TChunkList* chunkList,
        const NTableClient::NProto::TKey& lowerBound,
        const NTableClient::NProto::TKey* upperBound,
        NProto::TRspFetch* response);

    void ParseYPath(
        const NYTree::TYPath& path,
        NTableClient::TChannel* channel,
        NTableClient::NProto::TReadLimit* lowerBound,
        NTableClient::NProto::TReadLimit* upperBound);

    NChunkServer::TChunkList* EnsureNodeMutable(TTableNode* node);
    void ClearNode(TTableNode* node);

    DECLARE_RPC_SERVICE_METHOD(NProto, GetChunkListForUpdate);
    DECLARE_RPC_SERVICE_METHOD(NProto, Fetch);
    DECLARE_RPC_SERVICE_METHOD(NProto, SetSorted);
    DECLARE_RPC_SERVICE_METHOD(NProto, Clear);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableServer
} // namespace NYT

