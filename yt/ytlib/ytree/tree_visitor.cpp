#include "stdafx.h"
#include "tree_visitor.h"
#include "serialize.h"
#include "attributes.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/assert.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Traverses a YTree and invokes appropriate methods of IYsonConsumer.
class TTreeVisitor
    : private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param consumer A consumer to call.
     *  \param visitAttributes Enables going into attribute maps during traversal.
     */
    TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes);

    //! Starts the traversal.
    /*!
     *  \param root A root from which to start.
     */
    void Visit(INodePtr root);

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(INodePtr node);
    void VisitAttributes(INodePtr node);
    void VisitScalar(INodePtr node);
    void VisitEntity(INodePtr node);
    void VisitList(IListNodePtr node);
    void VisitMap(IMapNodePtr node);
};

////////////////////////////////////////////////////////////////////////////////

TTreeVisitor::TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes)
    : Consumer(consumer)
    , VisitAttributes_(visitAttributes)
{ }

void TTreeVisitor::Visit(INodePtr root)
{
    VisitAny(root);
}

void TTreeVisitor::VisitAny(INodePtr node)
{
    if (VisitAttributes_) {
        VisitAttributes(node);
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

void TTreeVisitor::VisitAttributes(INodePtr node) {
    yhash_set<Stroka> attributeKeySet = node->Attributes().List();
    if (!attributeKeySet.empty()) {
        std::vector<Stroka> attributeKeyList(attributeKeySet.begin(), attributeKeySet.end());
        std::sort(attributeKeyList.begin(), attributeKeyList.end());
        Consumer->OnBeginAttributes();
        FOREACH (const auto& key, attributeKeyList) {
            Consumer->OnKeyedItem(key);
            auto value = node->Attributes().GetYson(key);
            ProducerFromYson(value).Run(Consumer);
        }
        Consumer->OnEndAttributes();
    }
}

void TTreeVisitor::VisitScalar(INodePtr node)
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

void TTreeVisitor::VisitEntity(INodePtr node)
{
    UNUSED(node);
    Consumer->OnEntity();
}

void TTreeVisitor::VisitList(IListNodePtr node)
{
    Consumer->OnBeginList();
    for (int i = 0; i < node->GetChildCount(); ++i) {
        auto child = node->GetChild(i);
        Consumer->OnListItem();
        VisitAny(child);
    }
    Consumer->OnEndList();
}

void TTreeVisitor::VisitMap(IMapNodePtr node)
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

////////////////////////////////////////////////////////////////////////////////

void VisitTree(INodePtr root, IYsonConsumer* consumer, bool visitAttributes)
{
    TTreeVisitor treeVisitor(consumer, visitAttributes);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
