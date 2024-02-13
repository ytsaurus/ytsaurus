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
        const THashMap<TNodeId, std::vector<NRecords::TChildNode>>& nodeIdToChildren,
        const THashMap<TNodeId, TYPathProxy::TRspGetPtr>& nodeIdToMasterResponse)
        : Consumer(consumer)
        , AttributeFilter_(attributeFilter)
        , MaxAllowedNodeDepth_(maxAllowedNodeDepth)
        , NodeIdToChildren_(std::move(nodeIdToChildren))
        , NodeIdToMasterResponse_(std::move(nodeIdToMasterResponse))
    { }

    void Visit(TNodeId rootId)
    {
        VisitAny(rootId, /*currentNodeDepth*/ 0);
    }

private:
    NYson::IAsyncYsonConsumer* const Consumer;
    const TAttributeFilter AttributeFilter_;
    const int MaxAllowedNodeDepth_;
    const THashMap<TNodeId, std::vector<NRecords::TChildNode>> NodeIdToChildren_;
    const THashMap<TNodeId, TYPathProxy::TRspGetPtr> NodeIdToMasterResponse_;

    void VisitAny(TNodeId nodeId, int currentNodeDepth)
    {
        ++currentNodeDepth;

        if (AttributeFilter_) {
            auto masterResponse = GetOrCrash(NodeIdToMasterResponse_, nodeId)->value();
            auto node = ConvertToNode(NYson::TYsonString(masterResponse));
            node->WriteAttributes(Consumer, AttributeFilter_, /*stable*/ true);
        }

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

    void VisitScalar(TNodeId nodeId)
    {
        auto nodeType = TypeFromId(nodeId);
        auto masterResponse = GetOrCrash(NodeIdToMasterResponse_, nodeId)->value();
        auto node = ConvertToNode(NYson::TYsonString(masterResponse));
        switch (nodeType) {
            case EObjectType::StringNode:
                Consumer->OnStringScalar(node->AsString()->GetValue());
                break;

            case EObjectType::Int64Node:
                Consumer->OnInt64Scalar(node->AsInt64()->GetValue());
                break;

            case EObjectType::Uint64Node:
                Consumer->OnUint64Scalar(node->AsUint64()->GetValue());
                break;

            case EObjectType::DoubleNode:
                Consumer->OnDoubleScalar(node->AsDouble()->GetValue());
                break;

            case EObjectType::BooleanNode:
                Consumer->OnBooleanScalar(node->AsBoolean()->GetValue());
                break;

            default:
                THROW_ERROR_EXCEPTION("Unknown scalar type encountered while traversing Sequoia tree");
        }
    }

    void VisitEntity(TNodeId /*nodeId*/)
    {
        Consumer->OnEntity();
    }

    void VisitMap(TNodeId nodeId, int currentNodeDepth)
    {
        const auto& children = GetOrCrash(NodeIdToChildren_, nodeId);
        // If map node has no children, then it's better to return an empty map,
        // since returning entity could result in an extra request from user.
        // TODO(h0pless): Think about adding other heuristics from opaque setter script.
        if (currentNodeDepth == MaxAllowedNodeDepth_ && !children.empty()) {
            Consumer->OnEntity();
            return;
        }

        Consumer->OnBeginMap();
        for (const auto& [key, child] : children) {
            Consumer->OnKeyedItem(key.ChildKey);
            VisitAny(child, currentNodeDepth);
        }
        Consumer->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void VisitSequoiaTree(
    TNodeId rootId,
    int maxAllowedNodeDepth,
    NYson::IYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    const THashMap<TNodeId, std::vector<NRecords::TChildNode>>& nodeIdToChildren,
    const THashMap<TNodeId, TYPathProxy::TRspGetPtr>& nodeIdToMasterResponse)
{
    NYson::TAsyncYsonConsumerAdapter adapter(consumer);
    VisitSequoiaTree(
        rootId,
        maxAllowedNodeDepth,
        &adapter,
        attributeFilter,
        std::move(nodeIdToChildren),
        std::move(nodeIdToMasterResponse));
}

void VisitSequoiaTree(
    TNodeId rootId,
    int maxAllowedNodeDepth,
    NYson::IAsyncYsonConsumer* consumer,
    const TAttributeFilter& attributeFilter,
    const THashMap<TNodeId, std::vector<NRecords::TChildNode>>& nodeIdToChildren,
    const THashMap<TNodeId, TYPathProxy::TRspGetPtr>& nodeIdToMasterResponse)
{
    TSequoiaTreeVisitor treeVisitor(
        consumer,
        attributeFilter,
        maxAllowedNodeDepth,
        std::move(nodeIdToChildren),
        std::move(nodeIdToMasterResponse));
    treeVisitor.Visit(rootId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
