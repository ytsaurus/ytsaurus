#pragma once

#include "public.h"

#include <server/cell_master/public.h>
#include <core/misc/error.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeVisitor
    : public virtual TRefCounted
{
    virtual void OnNode(ICypressNodeProxyPtr node) = 0;
    virtual void OnError(const TError& error) = 0;
    virtual void OnCompleted() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressNodeVisitor)

void TraverseCypress(
    NCellMaster::TBootstrap* bootstrap,
    ICypressNodeProxyPtr rootNode,
    ICypressNodeVisitorPtr visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
