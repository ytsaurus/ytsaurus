#pragma once

#include "private.h"
#include "chunk_list.h"
#include "medium.h"

#include <yt/server/cypress_server/type_handler.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/overlaid_attribute_dictionaries.h>

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

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual bool IsSupportedInheritableAttribute(const TString& key) const;

private:
    NSecurityServer::TClusterResources GetChunkOwnerDiskUsage(
        const NChunkClient::NProto::TDataStatistics& statistics,
        const TChunkOwner& chunkOwner);

protected:
    NLogging::TLogger Logger;

    std::unique_ptr<TChunkOwner> DoCreateImpl(
        const NCypressServer::TVersionedNodeId& id,
        NObjectClient::TCellTag externalCellTag,
        NTransactionServer::TTransaction* transaction,
        NYTree::IAttributeDictionary* inheritedAttributes,
        NYTree::IAttributeDictionary* explicitAttributes,
        NSecurityServer::TAccount* account,
        int replicationFactor,
        NCompression::ECodec compressionCodec,
        NErasure::ECodec erasureCodec);

    virtual void DoDestroy(TChunkOwner* node) override;

    virtual void DoBranch(
        const TChunkOwner* originatingNode,
        TChunkOwner* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    virtual void DoLogBranch(
        const TChunkOwner* originatingNode,
        TChunkOwner* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    virtual void DoMerge(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    virtual void DoLogMerge(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    virtual void DoClone(
        TChunkOwner* sourceNode,
        TChunkOwner* clonedNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

#define CHUNK_OWNER_TYPE_HANDLER_INL_H_
#include "chunk_owner_type_handler-inl.h"
#undef CHUNK_OWNER_TYPE_HANDLER_INL_H_
