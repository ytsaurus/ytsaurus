#pragma once

#include "private.h"

#include <yt/server/master/cypress_server/node_detail.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
class TChunkOwnerTypeHandler
    : public NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner>
{
private:
    using TBase = NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner>;

public:
    explicit TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap);

    virtual NObjectServer::ETypeFlags GetFlags() const override;

    virtual NYTree::ENodeType GetNodeType() const override;

    virtual bool IsSupportedInheritableAttribute(const TString& key) const;

    virtual bool HasBranchedChangesImpl(TChunkOwner* originatingNode, TChunkOwner* branchedNode) override;

protected:
    NLogging::TLogger Logger;

    std::unique_ptr<TChunkOwner> DoCreateImpl(
        const NCypressServer::TVersionedNodeId& id,
        const NCypressServer::TCreateNodeContext& context,
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
        TChunkOwner* clonedTrunkNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    virtual void DoBeginCopy(
        TChunkOwner* node,
        NCypressServer::TBeginCopyContext* context) override;
    virtual void DoEndCopy(
        TChunkOwner* trunkNode,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
