#pragma once

#include "private.h"
#include "chunk_list.h"

#include <core/ytree/public.h>

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <server/cypress_server/type_handler.h>

#include <server/transaction_server/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
class TChunkOwnerTypeHandler
    : public NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner>
{
public:
    typedef NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner> TBase;

    explicit TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NYTree::ENodeType GetNodeType() override;

    virtual NSecurityServer::TClusterResources GetTotalResourceUsage(
        const NCypressServer::TCypressNodeBase* node) override;

    virtual NSecurityServer::TClusterResources GetIncrementalResourceUsage(
        const NCypressServer::TCypressNodeBase* node) override;

protected:
    NLogging::TLogger Logger;

    virtual std::unique_ptr<TChunkOwner> DoCreate(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* attributes,
        NCypressServer::INodeTypeHandler::TReqCreate* request,
        NCypressServer::INodeTypeHandler::TRspCreate* response) override;

    virtual void DoDestroy(TChunkOwner* node) override;

    virtual void DoBranch(
        const TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    virtual void DoMerge(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    virtual void DoClone(
        TChunkOwner* sourceNode,
        TChunkOwner* clonedNode,
        NCypressServer::ICypressNodeFactoryPtr factory,
        NCypressServer::ENodeCloneMode mode) override;

private:
    void MergeChunkLists(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define CHUNK_OWNER_TYPE_HANDLER_INL_H_
#include "chunk_owner_type_handler-inl.h"
#undef CHUNK_OWNER_TYPE_HANDLER_INL_H_
