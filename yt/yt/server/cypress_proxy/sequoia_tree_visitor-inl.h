#ifndef SEQUOIA_TREE_VISITOR_H
#error "Direct inclusion of this file is not allowed, include sequoia_tree_visitor.h"
// For the sake of sane code completion.
#include "sequoia_tree_visitor.h"
#endif

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

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
    TCallback isParent)
{
    using TNode = std::ranges::range_value_t<TNodeRange>;
    using TNodeRef = std::ranges::range_reference_t<TNodeRange>;

    using TTraceElement = std::conditional_t<
        std::is_lvalue_reference_v<TNodeRef>,
        std::reference_wrapper<const std::remove_reference_t<TNodeRef>>,
        TNode>;
    std::vector<TTraceElement> trace;

    for (auto&& node : treeTraversal) {
        // Adjust the trace to reflect the current path.
        while (!trace.empty() && !isParent(trace.back(), node)) {
            visitor->OnNodeExited(trace.back());
            trace.pop_back();
        }

        visitor->OnNodeEntered(node);

        if constexpr (std::is_lvalue_reference_v<TNodeRef>) {
            trace.emplace_back(node);
        } else {
            trace.emplace_back(std::move(node));
        }

        if (!visitor->ShouldContinue()) {
            return;
        }
    }

    // Clean up the trace by calling OnNodeExited for remaining nodes.
    for (const auto& node : trace | std::views::reverse) {
        visitor->OnNodeExited(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
