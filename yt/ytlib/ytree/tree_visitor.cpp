#include "stdafx.h"
#include "tree_visitor.h"
#include "attributes.h"
#include "yson_producer.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/assert.h>

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
    TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes)
        : Consumer(consumer)
        , VisitAttributes_(visitAttributes)
    { }

    void Visit(INodePtr root)
    {
        // NB: converting from INodePtr to IConstNodePtr ensures that
        // the constant overload of Attributes() is called.
        // Calling non-const version mutates the node and
        // makes TreeVisitor thread unsafe.
        VisitAny(root, true);
    }

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(IConstNodePtr node, bool isRoot = false)
    {
        if (VisitAttributes_) {
            VisitAttributes(node);
        }

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

    void VisitAttributes(IConstNodePtr node)
    {
        auto attributeKeySet = node->Attributes().List();
        if (!attributeKeySet.empty()) {
            std::vector<Stroka> attributeKeyList(attributeKeySet.begin(), attributeKeySet.end());
            std::sort(attributeKeyList.begin(), attributeKeyList.end());
            Consumer->OnBeginAttributes();
            FOREACH (const auto& key, attributeKeyList) {
                Consumer->OnKeyedItem(key);
                auto value = node->Attributes().GetYson(key);
                Consume(value, Consumer);
            }
            Consumer->OnEndAttributes();
        }
    }

    void VisitScalar(IConstNodePtr node)
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

    void VisitEntity(IConstNodePtr node)
    {
        UNUSED(node);
        Consumer->OnEntity();
    }

    void VisitList(IConstListNodePtr node)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Consumer->OnListItem();
            VisitAny(child);
        }
        Consumer->OnEndList();
    }

    void VisitMap(IConstMapNodePtr node)
    {
        Consumer->OnBeginMap();
        auto children = node->GetChildren();
        std::sort(children.begin(), children.end());
        FOREACH (const auto& pair, children) {
            Consumer->OnKeyedItem(pair.first);
            VisitAny(pair.second);
        }
        Consumer->OnEndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

void VisitTree(INodePtr root, IYsonConsumer* consumer, bool visitAttributes)
{
    TTreeVisitor treeVisitor(consumer, visitAttributes);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
