#include "sequoia_tree_visitor.h"

#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/async_consumer.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

void TraverseSequoiaTree(
    NCypressClient::TNodeId rootId,
    const TNodeIdToChildDescriptors& nodeIdToChildren,
    INodeVisitor<TCypressChildDescriptor>* visitor)
{
    const auto& children = GetOrCrash(nodeIdToChildren, rootId);
    for (const auto& child : children) {
        if (!visitor->ShouldVisit(child)) {
            continue;
        }
        visitor->OnNodeEntered(child);
        TraverseSequoiaTree(child.ChildId, nodeIdToChildren, visitor);
        visitor->OnNodeExited(child);
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Traverses a Sequoia tree stored in a hash map and invokes appropriate methods of IYsonConsumer.
// TODO(h0pless): Make a base class between TSequoiaTreeVisitor and TTreeVisitor.
class TSequoiaTreeVisitor
    : private TNonCopyable
{
public:
    TSequoiaTreeVisitor(
        NYson::IAsyncYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        int maxAllowedNodeDepth,
        const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
        const THashMap<TNodeId, INodePtr>& nodesWithAttributes)
        : Consumer_(consumer)
        , AttributeFilter_(attributeFilter)
        , MaxAllowedNodeDepth_(maxAllowedNodeDepth)
        , NodeIdToChildren_(std::move(nodeIdToChildren))
        , CalculateOpaqueness_(attributeFilter.AdmitsKeySlow({"opaque"}))
        , NodesWithAttributes_(std::move(nodesWithAttributes))
    { }

    void Visit(TNodeId rootId)
    {
        VisitAny(rootId, /*currentNodeDepth*/ 0, /*itemKey*/ std::nullopt);
    }

private:
    NYson::IAsyncYsonConsumer* const Consumer_;
    const TAttributeFilter AttributeFilter_;
    const int MaxAllowedNodeDepth_;
    const THashMap<TNodeId, std::vector<TCypressChildDescriptor>> NodeIdToChildren_;
    const bool CalculateOpaqueness_;

    THashMap<TNodeId, INodePtr> NodesWithAttributes_;

    void VisitAny(TNodeId nodeId, int currentNodeDepth, std::optional<TStringBuf> itemKey)
    {
        ++currentNodeDepth;

        bool keyWritten = false;
        auto maybeWriteKey = [&] {
            if (!keyWritten && itemKey) {
                keyWritten = true;
                Consumer_->OnKeyedItem(*itemKey);
            }
        };

        if (!NodeIdToChildren_.contains(nodeId)) {
            // Access denied.
            maybeWriteKey();
            VisitEntity(nodeId);
            return;
        }

        if (AttributeFilter_ && !AttributeFilter_.IsEmpty()) {
            auto nodeIter = NodesWithAttributes_.find(nodeId);
            if (nodeIter == NodesWithAttributes_.end() && !CalculateOpaqueness_) {
                // NodesWithAttributes_ come from attribute fetcher, and the
                // contract is that it may silently omit some nodes (due to a
                // race between listing the subtree via dyntable and actually
                // fetching attributes via master). It will never omit the
                // target (i.e. root) node, though.

                if (currentNodeDepth == 1) {
                    // This is a bug. If the root hasn't been fetched due to the
                    // aforementioned race, the whole request should've failed
                    // with a retriable error, and we shouldn't have gotten here.
                    YT_LOG_ALERT_AND_THROW("Cannot fetch attributes for node %v", nodeId);
                }

                // Silently omit the node.
                return;
            }

            MaybeOverrideAttributes(nodeId, currentNodeDepth);

            nodeIter = NodesWithAttributes_.find(nodeId);
            if (nodeIter != NodesWithAttributes_.end()) {
                maybeWriteKey();
                nodeIter->second->WriteAttributes(Consumer_, AttributeFilter_, /*stable*/ true);
            }
        }

        maybeWriteKey();

        auto nodeType = TypeFromId(nodeId);
        switch (nodeType) {
            case EObjectType::StringNode:
            case EObjectType::Int64Node:
            case EObjectType::Uint64Node:
            case EObjectType::DoubleNode:
            case EObjectType::BooleanNode:
                VisitScalar(nodeId);
                break;

            case EObjectType::Scion:
            case EObjectType::SequoiaMapNode:
                VisitMap(nodeId, currentNodeDepth);
                break;

            default:
                VisitEntity(nodeId);
                break;
        }
    }

    void MaybeOverrideAttributes(TNodeId nodeId, int currentNodeDepth)
    {
        if (!CalculateOpaqueness_) {
            return;
        }

        auto nodeType = TypeFromId(nodeId);
        if (nodeType != EObjectType::Scion && nodeType != EObjectType::SequoiaMapNode) {
            return;
        }

        auto emptyNode = ConvertToNode(NYson::TYsonString(TString("{}")));
        auto [it, _] = NodesWithAttributes_.insert({nodeId, std::move(emptyNode)});

        auto shouldAppearOpaque = currentNodeDepth == MaxAllowedNodeDepth_;
        it->second->MutableAttributes()->Set("opaque", shouldAppearOpaque);
    }

    void VisitScalar(TNodeId nodeId)
    {
        auto nodeType = TypeFromId(nodeId);
        auto node = GetOrCrash(NodesWithAttributes_, nodeId);
        switch (nodeType) {
            case EObjectType::StringNode:
                Consumer_->OnStringScalar(node->AsString()->GetValue());
                break;

            case EObjectType::Int64Node:
                Consumer_->OnInt64Scalar(node->AsInt64()->GetValue());
                break;

            case EObjectType::Uint64Node:
                Consumer_->OnUint64Scalar(node->AsUint64()->GetValue());
                break;

            case EObjectType::DoubleNode:
                Consumer_->OnDoubleScalar(node->AsDouble()->GetValue());
                break;

            case EObjectType::BooleanNode:
                Consumer_->OnBooleanScalar(node->AsBoolean()->GetValue());
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown scalar type encountered while traversing Sequoia tree");
        }
    }

    void VisitEntity(TNodeId /*nodeId*/)
    {
        Consumer_->OnEntity();
    }

    void VisitMap(TNodeId nodeId, int currentNodeDepth)
    {
        const auto& children = GetOrCrash(NodeIdToChildren_, nodeId);
        if (currentNodeDepth == MaxAllowedNodeDepth_) {
            Consumer_->OnEntity();
            return;
        }

        Consumer_->OnBeginMap();
        for (const auto& childDescriptor : children) {
            VisitAny(childDescriptor.ChildId, currentNodeDepth, childDescriptor.ChildKey);
        }
        Consumer_->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    TNodeId rootId,
    int maxAllowedNodeDepth,
    NYson::IYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const THashMap<TNodeId, INodePtr>& nodesWithAttributes)
{
    NYson::TAsyncYsonConsumerAdapter adapter(consumer);
    VisitSequoiaTree(
        rootId,
        maxAllowedNodeDepth,
        &adapter,
        attributeFilter,
        std::move(nodeIdToChildren),
        std::move(nodesWithAttributes));
}

void VisitSequoiaTree(
    TNodeId rootId,
    int maxAllowedNodeDepth,
    NYson::IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const THashMap<TNodeId, INodePtr>& nodesWithAttributes)
{
    TSequoiaTreeVisitor treeVisitor(
        consumer,
        attributeFilter,
        maxAllowedNodeDepth,
        std::move(nodeIdToChildren),
        std::move(nodesWithAttributes));
    treeVisitor.Visit(rootId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
