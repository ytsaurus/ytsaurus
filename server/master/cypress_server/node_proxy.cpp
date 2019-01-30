#include "node_proxy.h"

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxy* ICypressNodeProxy::FromNode(NYTree::INode* ptr)
{
    auto* result = dynamic_cast<ICypressNodeProxy*>(ptr);
    Y_ASSERT(result);
    return result;
}

TIntrusivePtr<ICypressNodeProxy> ICypressNodeProxy::FromNode(const TIntrusivePtr<NYTree::INode>& ptr)
{
    return FromNode(ptr.Get());
}

const ICypressNodeProxy* ICypressNodeProxy::FromNode(const NYTree::INode* ptr)
{
    auto* result = dynamic_cast<const ICypressNodeProxy*>(ptr);
    Y_ASSERT(result);
    return result;
}

TIntrusivePtr<const ICypressNodeProxy> ICypressNodeProxy::FromNode(const TIntrusivePtr<const NYTree::INode>& ptr)
{
    return FromNode(ptr.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

