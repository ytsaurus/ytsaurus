#pragma once

#include "table_node.h"

#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/public.h>

#include <ytlib/table_client/schema.h>

#include <ytlib/table_client/table_ypath.pb.h>
#include <server/chunk_server/public.h>

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
        NCypressServer::ICypressNode* trunkNode);

    virtual TResolveResult Resolve(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;
    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

private:
    typedef NCypressServer::TCypressNodeProxyBase<NYTree::IEntityNode, TTableNode> TBase;

    class TFetchChunkProcessor;
    typedef TIntrusivePtr<TFetchChunkProcessor> TFetchChunkProcessorPtr;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer) override;
    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;

    void TraverseChunkTree(
        std::vector<NChunkClient::TChunkId>* chunkIds,
        const NChunkServer::TChunkList* chunkList);

    template <class TBoundary>
    void RunFetchTraversal(
        const NChunkServer::TChunkList* chunkList,
        TFetchChunkProcessorPtr chunkProcessor, 
        const TBoundary& lowerBound, 
        const TNullable<TBoundary>& upperBound,
        bool negate);

    void ParseYPath(
        const NYPath::TYPath& path,
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

