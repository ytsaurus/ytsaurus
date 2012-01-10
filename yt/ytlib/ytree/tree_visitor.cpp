#include "stdafx.h"
#include "tree_visitor.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TTreeVisitor::TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes)
    : Consumer(consumer)
    , VisitAttributes_(visitAttributes)
{ }

void TTreeVisitor::Visit(const INode* root)
{
    VisitAny(root);
}

void TTreeVisitor::VisitAny(const INode* node)
{
    auto attributes = node->GetAttributes();
    bool hasAttributes = attributes && VisitAttributes_;

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
        VisitAttributes(~node->GetAttributes());
    }
}

void TTreeVisitor::VisitScalar(const INode* node, bool hasAttributes)
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

void TTreeVisitor::VisitEntity(const INode* node, bool hasAttributes)
{
    UNUSED(node);
    Consumer->OnEntity(hasAttributes);
}

void TTreeVisitor::VisitList(const IListNode* node, bool hasAttributes)
{
    Consumer->OnBeginList();
    for (int i = 0; i < node->GetChildCount(); ++i) {
        auto child = ~node->GetChild(i);
        Consumer->OnListItem();
        VisitAny(child);
    }
    Consumer->OnEndList(hasAttributes);
}

void TTreeVisitor::VisitMap(const IMapNode* node, bool hasAttributes)
{
    Consumer->OnBeginMap();
    auto children = node->GetChildren();
    auto sortedChildren = GetSortedIterators(children);
    FOREACH(const auto& pair, sortedChildren) {
        Consumer->OnMapItem(pair->First());
        VisitAny(~pair->Second());
    }
    Consumer->OnEndMap(hasAttributes);
}

void TTreeVisitor::VisitAttributes(const IMapNode* node)
{
    Consumer->OnBeginAttributes();
    auto children = node->GetChildren();
    auto sortedChildren = GetSortedIterators(children);
    FOREACH(const auto& pair, sortedChildren) {
        Consumer->OnAttributesItem(pair->First());
        VisitAny(~pair->Second());
    }
    Consumer->OnEndAttributes();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
