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
    TTreeVisitor(
        IYsonConsumer* consumer,
        bool visitAttributes = true)
        : Consumer(consumer)
        , VisitAttributes_(visitAttributes)
    { }

    void Visit(INode::TPtr root)
    {
        VisitAny(root);
    }

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

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
        }

        auto attributes = node->GetAttributes();
        if (~attributes != NULL && VisitAttributes_) {
            VisitAttributes(attributes);
        }
    }

    void VisitScalar(INode::TPtr node)
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

            case ENodeType::Entity:
                Consumer->OnEntityScalar();
                break;

            default:
                YUNREACHABLE();
        }
    }

    void VisitList(IListNode::TPtr node)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Consumer->OnListItem(i);
            VisitAny(child);
        }
        Consumer->OnEndList();
    }

    void VisitMap(IMapNode::TPtr node)
    {
        Consumer->OnBeginMap();
        FOREACH(const auto& pair, node->GetChildren()) {
            Consumer->OnMapItem(pair.First());
            VisitAny(pair.Second());
        }
        Consumer->OnEndMap();
    }

    void VisitAttributes(IMapNode::TPtr node)
    {
        Consumer->OnBeginAttributes();
        FOREACH(const auto& pair, node->GetChildren()) {
            Consumer->OnAttributesItem(pair.First());
            VisitAny(pair.Second());
        }
        Consumer->OnEndAttributes();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
