#pragma once

#include "public.h"

#include <server/cell_master/public.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeVisitor
    : public virtual TRefCounted
{
    virtual void OnNode(ICypressNodeProxyPtr nodeProxy) = 0;
    virtual void OnError(const TError& error) = 0;
    virtual void OnCompleted() = 0;
};

typedef TIntrusivePtr<ICypressNodeVisitor> ICypressNodeVisitorPtr;

void TraverseSubtree(
    NCellMaster::TBootstrap* bootstrap, 
    ICypressNodeProxyPtr nodeProxy, 
    ICypressNodeVisitorPtr visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT