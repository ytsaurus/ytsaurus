#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TTreeVisitor
    : private TNonCopyable
{
public:
    TTreeVisitor(IYsonConsumer* events)
        : Events(events)
    { }

    void Visit(INode::TConstPtr root)
    {
        Events->BeginTree();
        VisitAny(root);
        Events->EndTree();
    }

private:
    IYsonConsumer* Events;

    void VisitAny(INode::TConstPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
            case ENodeType::Int64:
            case ENodeType::Double:
            case ENodeType::Entity:
                VisitScalar(node);
                break;

            case ENodeType::List:
                VisitList(node->AsList());
                break;

            case ENodeType::Map:
                VisitMap(node->AsMap());
                break;

            default:
                YASSERT(false);
                break;
        }

        auto attributes = node->GetAttributes();
        if (~attributes != NULL) {
            VisitAttributes(attributes);
        }
    }

    void VisitScalar(INode::TConstPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Events->StringValue(node->GetValue<Stroka>());
                break;

            case ENodeType::Int64:
                Events->Int64Value(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Events->DoubleValue(node->GetValue<double>());
                break;

            case ENodeType::Entity:
                Events->EntityValue();
                break;

            default:
                YASSERT(false);
                break;
        }
    }

    void VisitList(IListNode::TConstPtr node)
    {
        Events->BeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Events->ListItem(i);
            Visit(child);
        }
        Events->EndList();
    }

    void VisitMap(IMapNode::TConstPtr node)
    {
        Events->BeginMap();
        FOREACH(const auto& pair, node->GetChildren()) {
            Events->MapItem(pair.First());
            Visit(pair.Second());
        }
        Events->EndMap();
    }

    void VisitAttributes(IMapNode::TConstPtr node)
    {
        Events->BeginAttributes();
        FOREACH(const auto& pair, node->GetChildren()) {
            Events->AttributesItem(pair.First());
            Visit(pair.Second());
        }
        Events->EndAttributes();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
