#include "stdafx.h"
#include "tree_visitor.h"
#include "attributes.h"
#include "yson_producer.h"
#include "attribute_helpers.h"

#include <core/misc/serialize.h>
#include <core/misc/assert.h>

#include <core/ytree/node.h>
#include <core/ytree/convert.h>

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
        IYsonConsumer* consumer,
        const TAttributeFilter& attributeFilter,
        bool sortKeys,
        bool ignoreOpaque)
        : Consumer(consumer)
        , AttributeFilter(attributeFilter)
        , SortKeys(sortKeys)
        , IgnoreOpaque(ignoreOpaque)
    { }

    void Visit(const INodePtr& root)
    {
        VisitAny(root, true);
    }

private:
    IYsonConsumer* Consumer;
    TAttributeFilter AttributeFilter;
    bool SortKeys;
    bool IgnoreOpaque;

    void VisitAny(const INodePtr& node, bool isRoot = false)
    {
        node->SerializeAttributes(Consumer, AttributeFilter, SortKeys);

        if (!isRoot &&
            !IgnoreOpaque &&
            node->Attributes().Get<bool>("opaque", false))
        {
            // This node is opaque, i.e. replaced by entity during tree traversal.
            Consumer->OnEntity();
            return;
        }

        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
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
                YUNREACHABLE();
        }
    }

    void VisitScalar(const INodePtr& node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Consumer->OnStringScalar(node->GetValue<Stroka>());
                break;

            case ENodeType::Int64:
                Consumer->OnInt64Scalar(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Consumer->OnDoubleScalar(node->GetValue<double>());
                break;

            case ENodeType::Boolean:
                Consumer->OnBooleanScalar(node->GetValue<bool>());
                break;

            default:
                YUNREACHABLE();
        }
    }

    void VisitEntity(const INodePtr& node)
    {
        UNUSED(node);
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
        if (SortKeys) {
            typedef std::pair<Stroka, INodePtr> TPair;
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
    const TAttributeFilter& attributeFilter,
    bool sortKeys,
    bool ignoreOpaque)
{
    TTreeVisitor treeVisitor(
        consumer,
        attributeFilter,
        sortKeys,
        ignoreOpaque);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
