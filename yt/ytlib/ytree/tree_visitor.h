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

    void Visit(INode::TPtr root)
    {
        VisitAny(root);
    }

private:
    IYsonConsumer* Events;

    void VisitAny(INode::TPtr node)
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
                YUNREACHABLE();
                break;
        }

        auto attributes = node->GetAttributes();
        if (~attributes != NULL) {
            VisitAttributes(attributes);
        }
    }

    void VisitScalar(INode::TPtr node)
    {
        switch (node->GetType()) {
            case ENodeType::String:
                Events->OnStringScalar(node->GetValue<Stroka>());
                break;

            case ENodeType::Int64:
                Events->OnInt64Scalar(node->GetValue<i64>());
                break;

            case ENodeType::Double:
                Events->OnDoubleScalar(node->GetValue<double>());
                break;

            case ENodeType::Entity:
                Events->OnEntityScalar();
                break;

            default:
                YUNREACHABLE();
                break;
        }
    }

    void VisitList(IListNode::TPtr node)
    {
        Events->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Events->OnListItem(i);
            VisitAny(child);
        }
        Events->OnEndList();
    }

    void VisitMap(IMapNode::TPtr node)
    {
        Events->OnBeginMap();
        FOREACH(const auto& pair, node->GetChildren()) {
            Events->OnMapItem(pair.First());
            VisitAny(pair.Second());
        }
        Events->OnEndMap();
    }

    void VisitAttributes(IMapNode::TPtr node)
    {
        Events->OnBeginAttributes();
        FOREACH(const auto& pair, node->GetChildren()) {
            Events->OnAttributesItem(pair.First());
            VisitAny(pair.Second());
        }
        Events->OnEndAttributes();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
