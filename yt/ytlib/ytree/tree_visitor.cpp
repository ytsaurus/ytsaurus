#include "stdafx.h"
#include "tree_visitor.h"
#include "attributes.h"
#include "yson_producer.h"
#include "attribute_helpers.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/assert.h>

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/convert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// Some handy shortcuts.
typedef TIntrusivePtr<const INode>     IConstNodePtr;
typedef TIntrusivePtr<const IMapNode>  IConstMapNodePtr;
typedef TIntrusivePtr<const IListNode> IConstListNodePtr;

//! Traverses a YTree and invokes appropriate methods of IYsonConsumer.
class TTreeVisitor
    : private TNonCopyable
{
public:
    TTreeVisitor(IYsonConsumer* consumer, const TAttributeFilter& attributeFilter)
        : Consumer(consumer)
        , AttributeFilter(attributeFilter)
    { }

    void Visit(const IConstNodePtr& root)
    {
        VisitAny(root, true);
    }

private:
    IYsonConsumer* Consumer;
    TAttributeFilter AttributeFilter;

    void VisitAny(const IConstNodePtr& node, bool isRoot = false)
    {
        node->SerializeAttributes(Consumer, AttributeFilter);

        if (!isRoot && node->Attributes().Get<bool>("opaque", false)) {
            // This node is opaque, i.e. replaced by entity during tree traversal.
            Consumer->OnEntity();
            return;
        }

        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Integer:
            case ENodeType::Double:
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

    void VisitScalar(const IConstNodePtr& node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Consumer->OnStringScalar(node->GetValue<Stroka>());
                break;

            case ENodeType::Integer:
                Consumer->OnIntegerScalar(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Consumer->OnDoubleScalar(node->GetValue<double>());
                break;

            default:
                YUNREACHABLE();
        }
    }

    void VisitEntity(const IConstNodePtr& node)
    {
        UNUSED(node);
        Consumer->OnEntity();
    }

    void VisitList(const IConstListNodePtr& node)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            Consumer->OnListItem();
            VisitAny(node->GetChild(i));
        }
        Consumer->OnEndList();
    }

    void VisitMap(const IConstMapNodePtr& node)
    {
        Consumer->OnBeginMap();
        FOREACH (const auto& pair, node->GetChildren()) {
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
    const TAttributeFilter& attributeFilter)
{
    TTreeVisitor treeVisitor(consumer, attributeFilter);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
