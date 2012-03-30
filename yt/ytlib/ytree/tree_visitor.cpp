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
    void Visit(INode* root);

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(INode* node);
    void VisitScalar(INode* node, bool hasAttributes);
    void VisitEntity(INode* node, bool hasAttributes);
    void VisitList(IListNode* node, bool hasAttributes);
    void VisitMap(IMapNode* node, bool hasAttributes);
};

////////////////////////////////////////////////////////////////////////////////

TTreeVisitor::TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes)
    : Consumer(consumer)
    , VisitAttributes_(visitAttributes)
{ }

void TTreeVisitor::Visit(INode* root)
{
    VisitAny(root);
}

void TTreeVisitor::VisitAny(INode* node)
{
    yhash_set<Stroka> attributeKeySet;
    if (VisitAttributes_) {
        attributeKeySet = node->Attributes().List();
    }
    bool hasAttributes = !attributeKeySet.empty();

    switch (node->GetType()) {
        case ENodeType::String:
        case ENodeType::Int64:
        case ENodeType::Double:
            VisitScalar(node, hasAttributes);
            break;

        case ENodeType::Entity:
            VisitEntity(node, hasAttributes);
            break;

        case ENodeType::List:
            VisitList(~node->AsList(), hasAttributes);
            break;

        case ENodeType::Map:
            VisitMap(~node->AsMap(), hasAttributes);
            break;

        default:
            YUNREACHABLE();
    }

    if (hasAttributes) {
        std::vector<Stroka> attributeKeyList(attributeKeySet.begin(), attributeKeySet.end());
        std::sort(attributeKeyList.begin(), attributeKeyList.end());
        Consumer->OnBeginAttributes();
        FOREACH (const auto& key, attributeKeyList) {
            Consumer->OnAttributesItem(key);
            auto value = node->Attributes().GetYson(key);
            ProducerFromYson(value).Run(Consumer);
        }
        Consumer->OnEndAttributes();
    }
}

void TTreeVisitor::VisitScalar(INode* node, bool hasAttributes)
{
    switch (node->GetType()) {
        case ENodeType::String:
            Consumer->OnStringScalar(node->GetValue<Stroka>(), hasAttributes);
            break;

        case ENodeType::Int64:
            Consumer->OnInt64Scalar(node->GetValue<i64>(), hasAttributes);
            break;

        case ENodeType::Double:
            Consumer->OnDoubleScalar(node->GetValue<double>(), hasAttributes);
            break;

        default:
            YUNREACHABLE();
    }
}

void TTreeVisitor::VisitEntity(INode* node, bool hasAttributes)
{
    UNUSED(node);
    Consumer->OnEntity(hasAttributes);
}

void TTreeVisitor::VisitList(IListNode* node, bool hasAttributes)
{
    Consumer->OnBeginList();
    for (int i = 0; i < node->GetChildCount(); ++i) {
        auto child = node->GetChild(i);
        Consumer->OnListItem();
        VisitAny(~child);
    }
    Consumer->OnEndList(hasAttributes);
}

void TTreeVisitor::VisitMap(IMapNode* node, bool hasAttributes)
{
    Consumer->OnBeginMap();
    auto children = node->GetChildren();
    std::sort(children.begin(), children.end());
    FOREACH(const auto& pair, children) {
        Consumer->OnMapItem(pair.first);
        VisitAny(~pair.second);
    }
    Consumer->OnEndMap(hasAttributes);
}

////////////////////////////////////////////////////////////////////////////////

void VisitTree(INode* root, IYsonConsumer* consumer, bool visitAttributes)
{
    TTreeVisitor treeVisitor(consumer, visitAttributes);
    treeVisitor.Visit(root);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
