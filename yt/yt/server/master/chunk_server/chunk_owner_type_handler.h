#pragma once

#include "private.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

template <class TChunkOwner>
class TChunkOwnerTypeHandler
    : virtual public NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner>
{
private:
    using TBase = NCypressServer::TCypressNodeTypeHandlerBase<TChunkOwner>;

public:
    explicit TChunkOwnerTypeHandler(NCellMaster::TBootstrap* bootstrap);

    NObjectServer::ETypeFlags GetFlags() const override;

    NYTree::ENodeType GetNodeType() const override;

    bool IsSupportedInheritableAttribute(const TString& key) const override;

    bool HasBranchedChangesImpl(TChunkOwner* originatingNode, TChunkOwner* branchedNode) override;

protected:
    NLogging::TLogger Logger;

    std::unique_ptr<TChunkOwner> DoCreateImpl(
        NCypressServer::TVersionedNodeId id,
        const NCypressServer::TCreateNodeContext& context,
        int replicationFactor,
        NCompression::ECodec compressionCodec,
        NErasure::ECodec erasureCodec,
        bool enableStripedErasure,
        NChunkServer::EChunkListKind rootChunkListKind = NChunkServer::EChunkListKind::Static);

    void DoDestroy(TChunkOwner* node) override;

    void DoBranch(
        const TChunkOwner* originatingNode,
        TChunkOwner* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    void DoLogBranch(
        const TChunkOwner* originatingNode,
        TChunkOwner* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    void DoMerge(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    void DoLogMerge(
        TChunkOwner* originatingNode,
        TChunkOwner* branchedNode) override;

    void DoClone(
        TChunkOwner* sourceNode,
        TChunkOwner* clonedTrunkNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;

    void DoBeginCopy(
        TChunkOwner* node,
        NCypressServer::TBeginCopyContext* context) override;
    void DoEndCopy(
        TChunkOwner* trunkNode,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
