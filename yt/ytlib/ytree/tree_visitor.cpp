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
    TTreeVisitor(
        IYsonConsumer* consumer,
        bool withAttributes,
        const std::vector<Stroka>* const attributesFilter)
        : Consumer(consumer)
        , WithAttributes(withAttributes)
        , AttributesFilter(attributesFilter)
    { }

    void Visit(const INodePtr& root)
    {
        // NB: Converting from INodePtr to IConstNodePtr ensures that
        // the constant overload of Attributes() is called.
        // Calling non-const version mutates the node and
        // makes TreeVisitor thread-unsafe.
        VisitAny(root, true);
    }

private:
    IYsonConsumer* Consumer;
    bool WithAttributes;
    const std::vector<Stroka>* const AttributesFilter;

    void VisitAny(const IConstNodePtr& node, bool isRoot = false)
    {
        if (WithAttributes) {
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
        if (AttributesFilter) {
            // Fast path.
            const auto& attributes = node->Attributes();
            Consumer->OnBeginAttributes();    
            FOREACH (const auto& key, *AttributesFilter) {
                auto value = attributes.FindYson(key);
                if (value) {
                    Consumer->OnKeyedItem(key);
                    Consume(*value, Consumer);
                }
            }
            Consumer->OnEndAttributes();
        } else {
            // Slow path.
            auto keys = node->Attributes().List();
            if (keys.size() > 0) {
                Consumer->OnBeginAttributes();
                FOREACH (const auto& key, keys) {
                    Consumer->OnKeyedItem(key);
                    Consume(node->Attributes().GetYson(key), Consumer);
                }
                Consumer->OnEndAttributes();
            }
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

void VisitTree(const INodePtr& root, IYsonConsumer* consumer, bool withAttributes, const std::vector<Stroka>* const attributesFilter)
{
    TTreeVisitor treeVisitor(consumer, withAttributes, attributesFilter);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
