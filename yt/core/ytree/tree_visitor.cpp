#include "tree_visitor.h"
#include "helpers.h"
#include "attributes.h"
#include "node.h"
#include "convert.h"

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/assert.h>

#include <yt/core/yson/producer.h>
#include <yt/core/yson/async_consumer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Traverses a YTree and invokes appropriate methods of IYsonConsumer.
class TTreeVisitor
    : private TNonCopyable
{
public:
    TTreeVisitor(
        IAsyncYsonConsumer* consumer,
        bool stable,
        const std::optional<std::vector<TString>>& attributeKeys)
        : Consumer(consumer)
        , Stable_(stable)
        , AttributeKeys(attributeKeys)
    { }

    void Visit(const INodePtr& root)
    {
        VisitAny(root, true);
    }

private:
    IAsyncYsonConsumer* const Consumer;
    const bool Stable_;
    const std::optional<std::vector<TString>> AttributeKeys;

    void VisitAny(const INodePtr& node, bool isRoot = false)
    {
        node->WriteAttributes(Consumer, AttributeKeys, Stable_);

        static const TString opaqueAttributeName("opaque");
        if (!isRoot &&
            node->Attributes().Get<bool>(opaqueAttributeName, false))
        {
            // This node is opaque, i.e. replaced by entity during tree traversal.
            Consumer->OnEntity();
            return;
        }

        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Uint64:
            case ENodeType::Double:
            case ENodeType::Boolean:
                VisitScalar(node);
                break;

            case ENodeType::Entity:
                VisitEntity(node);
                break;

            case ENodeType::List:
                VisitList(node->AsList());
                break;

            case ENodeType::Map:
                VisitMap(node->AsMap());
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    void VisitScalar(const INodePtr& node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Consumer->OnStringScalar(node->GetValue<TString>());
                break;

            case ENodeType::Int64:
                Consumer->OnInt64Scalar(node->GetValue<i64>());
                break;

            case ENodeType::Uint64:
                Consumer->OnUint64Scalar(node->GetValue<ui64>());
                break;

            case ENodeType::Double:
                Consumer->OnDoubleScalar(node->GetValue<double>());
                break;

            case ENodeType::Boolean:
                Consumer->OnBooleanScalar(node->GetValue<bool>());
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    void VisitEntity(const INodePtr& /*node*/)
    {
        Consumer->OnEntity();
    }

    void VisitList(const IListNodePtr& node)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            Consumer->OnListItem();
            VisitAny(node->GetChild(i));
        }
        Consumer->OnEndList();
    }

    void VisitMap(const IMapNodePtr& node)
    {
        Consumer->OnBeginMap();
        auto children = node->GetChildren();
        if (Stable_) {
            typedef std::pair<TString, INodePtr> TPair;
            std::sort(
                children.begin(),
                children.end(),
                [] (const TPair& lhs, const TPair& rhs) {
                    return lhs.first < rhs.first;
                });
        }
        for (const auto& pair : children) {
            Consumer->OnKeyedItem(pair.first);
            VisitAny(pair.second);
        }
        Consumer->OnEndMap();
    }

};

////////////////////////////////////////////////////////////////////////////////

void VisitTree(
    INodePtr root,
    IYsonConsumer* consumer,
    bool stable,
    const std::optional<std::vector<TString>>& attributeKeys)
{
    TAsyncYsonConsumerAdapter adapter(consumer);
    VisitTree(
        std::move(root),
        &adapter,
        stable,
        attributeKeys);
}

void VisitTree(
    INodePtr root,
    IAsyncYsonConsumer* consumer,
    bool stable,
    const std::optional<std::vector<TString>>& attributeKeys)
{
    TTreeVisitor treeVisitor(
        consumer,
        stable,
        attributeKeys);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
