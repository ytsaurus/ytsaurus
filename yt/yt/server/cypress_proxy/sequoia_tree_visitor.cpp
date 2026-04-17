#include "sequoia_tree_visitor.h"

#include "helpers.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/yson/producer.h>
#include <yt/yt/core/yson/async_consumer.h>

namespace NYT::NCypressProxy {

using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsAncestor(const TCypressNodeDescriptor& maybeAncestor, const TCypressNodeDescriptor& child)
{
    return IsAncestorPath(maybeAncestor.Path, child.Path);
}

} // namespace

TSequoiaTreeTraverser::TSequoiaTreeTraverser(INodeVisitor<TCypressNodeDescriptor>* visitor)
    : Visitor_(visitor)
{ }

void TSequoiaTreeTraverser::Walk(const TCypressNodeDescriptor& node)
{
    if (SkippedRoot_.has_value() && IsAncestor(*SkippedRoot_, node)) {
        return;
    }
    SkippedRoot_.reset();

    while (!Trace_.empty() && !IsAncestor(Trace_.back(), node)) {
        Visitor_->OnNodeExited(Trace_.back());
        Trace_.pop_back();
    }

    if (!Visitor_->ShouldVisit(node)) {
        SkippedRoot_ = node;
        return;
    }

    Visitor_->OnNodeEntered(node);
    Trace_.push_back(node);
}

void TSequoiaTreeTraverser::Finish() &&
{
    for (const auto& node : Trace_ | std::views::reverse) {
        Visitor_->OnNodeExited(node);
    }
}

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
        const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>* nodeIdToChildren,
        const TNodeIdToAttributes* nodesWithAttributes,
        const THashSet<TNodeId>* opaqueNodeIds)
        : Consumer_(consumer)
        , AttributeFilter_(attributeFilter)
        , NodeIdToChildren_(nodeIdToChildren)
        , NodesWithAttributes_(nodesWithAttributes)
        , OpaqueNodeIds_(opaqueNodeIds)
        , CalculateOpaqueness_(attributeFilter.AdmitsKeySlow({EInternedAttributeKey::Opaque.Unintern()}))
    { }

    void Visit(TNodeId rootId)
    {
        VisitAny(rootId, /*itemKey*/ std::nullopt);
    }

private:
    NYson::IAsyncYsonConsumer* const Consumer_;
    const TAttributeFilter AttributeFilter_;
    const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>* const NodeIdToChildren_;
    const TNodeIdToAttributes* const NodesWithAttributes_;
    const THashSet<TNodeId>* const OpaqueNodeIds_;
    const bool CalculateOpaqueness_;

    void VisitAny(TNodeId nodeId, std::optional<TStringBuf> itemKey)
    {
        auto maybeWriteKey = [&, written = false] () mutable {
            if (!std::exchange(written, true) && itemKey) {
                Consumer_->OnKeyedItem(*itemKey);
            }
        };

        if (!NodeIdToChildren_->contains(nodeId)) {
            // Access denied.
            maybeWriteKey();
            VisitEntity(nodeId);
            return;
        }

        auto nodeType = TypeFromId(nodeId);

        if (AttributeFilter_ && !AttributeFilter_.IsEmpty()) {
            const auto& attributes = GetOrCrash(*NodesWithAttributes_, nodeId);

            auto node = NYT::Visit(attributes,
                [&] (const INodePtr& node) -> std::optional<INodePtr> {
                    return node;
                },
                [&] (const IAttributeDictionaryPtr& attributes) -> std::optional<INodePtr> {
                    // TODO(danilalexeev): YT-26172. Do not copy attributes.
                    auto node = CreateEphemeralNodeFactory()->CreateEntity();
                    node->MutableAttributes()->MergeFrom(*attributes);
                    return std::move(node);
                },
                [&] (TMissingNodeTag) -> std::optional<INodePtr> {
                    return {};
                });

            if (!node) {
                return; // Omit the node.
            }

            if (CalculateOpaqueness_ && IsSequoiaCompositeNodeType(nodeType)) {
                auto opaque = OpaqueNodeIds_->contains(nodeId);
                (*node)->MutableAttributes()->Set(EInternedAttributeKey::Opaque.Unintern(), opaque);
            }

            maybeWriteKey();
            (*node)->WriteAttributes(Consumer_, AttributeFilter_, /*stable*/ true);
        }

        maybeWriteKey();

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
                VisitMap(nodeId);
                break;

            default:
                VisitEntity(nodeId);
                break;
        }
    }

    void VisitScalar(TNodeId nodeId)
    {
        const auto& attributes = GetOrCrash(*NodesWithAttributes_, nodeId);
        const auto* node = std::get_if<INodePtr>(&attributes);
        YT_VERIFY(node);

        auto nodeType = TypeFromId(nodeId);
        switch (nodeType) {
            case EObjectType::StringNode:
                Consumer_->OnStringScalar((*node)->AsString()->GetValue());
                break;

            case EObjectType::Int64Node:
                Consumer_->OnInt64Scalar((*node)->AsInt64()->GetValue());
                break;

            case EObjectType::Uint64Node:
                Consumer_->OnUint64Scalar((*node)->AsUint64()->GetValue());
                break;

            case EObjectType::DoubleNode:
                Consumer_->OnDoubleScalar((*node)->AsDouble()->GetValue());
                break;

            case EObjectType::BooleanNode:
                Consumer_->OnBooleanScalar((*node)->AsBoolean()->GetValue());
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown scalar type encountered while traversing Sequoia tree");
        }
    }

    void VisitEntity(TNodeId /*nodeId*/)
    {
        Consumer_->OnEntity();
    }

    void VisitMap(TNodeId nodeId)
    {
        const auto& children = GetOrCrash(*NodeIdToChildren_, nodeId);
        if (OpaqueNodeIds_->contains(nodeId)) {
            Consumer_->OnEntity();
            return;
        }

        Consumer_->OnBeginMap();
        for (const auto& childDescriptor : children) {
            VisitAny(childDescriptor.ChildId, childDescriptor.ChildKey);
        }
        Consumer_->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    TNodeId rootId,
    NYson::IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    const THashMap<TNodeId, std::vector<TCypressChildDescriptor>>& nodeIdToChildren,
    const TNodeIdToAttributes& nodesWithAttributes,
    const THashSet<TNodeId>& opaqueNodeIds)
{
    TSequoiaTreeVisitor treeVisitor(
        consumer,
        attributeFilter,
        &nodeIdToChildren,
        &nodesWithAttributes,
        &opaqueNodeIds);
    treeVisitor.Visit(rootId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
