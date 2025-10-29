#pragma once

#include "public.h"

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/ytree/attribute_filter.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

template <class TNode>
struct INodeVisitor
{
    virtual ~INodeVisitor() = default;

    virtual void OnNodeEntered(const TNode& /*node*/) = 0;

    virtual void OnNodeExited(const TNode& /*node*/) = 0;

    //! Determines if the traversal should continue or termintate early.
    //! Called on each node visit (after #OnNodeEntered).
    virtual bool ShouldContinue() = 0;
};

// Simulates an in-order tree traversal using a precomputed sequence of nodes.
template <std::ranges::input_range TNodeRange, class TCallback>
    requires CInvocable<
        TCallback,
        bool(
            const std::ranges::range_value_t<TNodeRange>&,
            const std::ranges::range_value_t<TNodeRange>&)>
void TraverseSequoiaTree(
    TNodeRange&& treeTraversal,
    INodeVisitor<std::ranges::range_value_t<TNodeRange>>* visitor,
    TCallback isParent);

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    NCypressClient::TNodeId rootId,
    int maxDepth,
    NYson::IYsonConsumer* consumer,
    const NYTree::TAttributeFilter& attributeFilter,
    const THashMap<NCypressClient::TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const THashMap<NCypressClient::TNodeId, NYTree::INodePtr>& nodesWithAttributes);

// NB: Same as the above, but using async consumer instead of a sync one.
void VisitSequoiaTree(
    NCypressClient::TNodeId rootId,
    int maxDepth,
    NYson::IAsyncYsonConsumer* consumer,
    const NYTree::TAttributeFilter& attributeFilter,
    const THashMap<NCypressClient::TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const THashMap<NCypressClient::TNodeId, NYTree::INodePtr>& nodesWithAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy

#define SEQUOIA_TREE_VISITOR_H
#include "sequoia_tree_visitor-inl.h"
#undef SEQUOIA_TREE_VISITOR_H
