#include "stdafx.h"
#include "tree_visitor.h"
#include "ypath_client.h"
#include "serialize.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/misc/assert.h>

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
    yvector<Stroka> attributeNames;
    if (VisitAttributes_) {
        attributeNames = SyncYPathList(const_cast<INode*>(node), RootMarker + AttributeMarker);
    }

    bool hasAttributes = !attributeNames.empty();

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
        std::sort(attributeNames.begin(), attributeNames.end());
        Consumer->OnBeginAttributes();
        FOREACH (const auto& attributeName, attributeNames) {
            Consumer->OnAttributesItem(attributeName);
            auto attributeValue = SyncYPathGet(const_cast<INode*>(node), RootMarker + AttributeMarker + attributeName);
            ProducerFromYson(attributeValue)->Do(Consumer);
        }
        Consumer->OnEndAttributes();
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
    std::sort(children.begin(), children.end());
    FOREACH(const auto& pair, children) {
        Consumer->OnMapItem(pair.first);
        VisitAny(~pair.second);
    }
    Consumer->OnEndMap(hasAttributes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
