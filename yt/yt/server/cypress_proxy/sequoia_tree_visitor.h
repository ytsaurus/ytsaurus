#pragma once

#include "public.h"
#include "private.h"

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

    //! Determines if the traversal should visit a node or ignore it.
    //! Called before #OnNodeEntered.
    virtual bool ShouldVisit(const TNode& /*node*/) = 0;
};

class TSequoiaTreeTraverser
{
public:
    explicit TSequoiaTreeTraverser(INodeVisitor<TCypressNodeDescriptor>* visitor);

    void Walk(const TCypressNodeDescriptor& node);
    void Finish() &&;

private:
    INodeVisitor<TCypressNodeDescriptor>* const Visitor_;

    std::vector<TCypressNodeDescriptor> Trace_;
    std::optional<TCypressNodeDescriptor> SkippedRoot_;
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

// Performs an in-order tree traversal from a root node.
void TraverseSequoiaTree(
    NCypressClient::TNodeId rootId,
    const TNodeIdToChildDescriptors& nodeIdToChildren,
    INodeVisitor<TCypressChildDescriptor>* visitor);

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    NCypressClient::TNodeId rootId,
    int maxDepth,
    NYson::IAsyncYsonConsumer* consumer,
    const NYTree::TAttributeFilter& attributeFilter,
    const THashMap<NCypressClient::TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const TNodeIdToAttributes& nodesWithAttributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy

#define SEQUOIA_TREE_VISITOR_H
#include "sequoia_tree_visitor-inl.h"
#undef SEQUOIA_TREE_VISITOR_H
