#pragma once

#include "public.h"

#include <yt/yt/server/master/cypress_server/node_detail.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TSchemafulNodeTypeHandlerBase
    : virtual public NCypressServer::TCypressNodeTypeHandlerBase<TImpl>
{
private:
    using TBase = NCypressServer::TCypressNodeTypeHandlerBase<TImpl>;

public:
    explicit TSchemafulNodeTypeHandlerBase(NCellMaster::TBootstrap* bootstrap);

protected:

    // It's impossible to override Create method in any meaningful way, because
    // it behaves very differently in chaos replicated tables and table nodes.

    TMasterTableSchema* DoFindSchema(TImpl* schemafulNode) const override;

    void DoZombify(TImpl* schemafulNode) override;

    void DoDestroy(TImpl* schemafulNode) override;

    void DoBranch(
        const TImpl* originatingNode,
        TImpl* branchedNode,
        const NCypressServer::TLockRequest& lockRequest) override;

    void DoMerge(
        TImpl* originatingNode,
        TImpl* branchedNode) override;

    void DoClone(
        TImpl* sourceNode,
        TImpl* clonedTrunkNode,
        NCypressServer::ICypressNodeFactory* factory,
        NCypressServer::ENodeCloneMode mode,
        NSecurityServer::TAccount* account) override;
    void DoBeginCopy(
        TImpl* schemafulNode,
        NCypressServer::TBeginCopyContext* context) override;
    void DoEndCopy(
        TImpl* schemafulNode,
        NCypressServer::TEndCopyContext* context,
        NCypressServer::ICypressNodeFactory* factory) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
