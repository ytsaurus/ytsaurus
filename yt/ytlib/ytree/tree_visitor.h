#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// TODO: move impl to cpp
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
    TTreeVisitor(
        IYsonConsumer* consumer,
        bool visitAttributes = true)
        : Consumer(consumer)
        , VisitAttributes_(visitAttributes)
    { }

    //! Starts the traversal.
    /*!
     *  \param root A root from which to start.
     */
    void Visit(INode::TPtr root)
    {
        VisitAny(root);
    }

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(INode::TPtr node)
    {
        auto attributes = node->GetAttributes();
        bool hasAttributes = ~attributes != NULL && VisitAttributes_;

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
                VisitList(node->AsList(), hasAttributes);
                break;

            case ENodeType::Map:
                VisitMap(node->AsMap(), hasAttributes);
                break;

            default:
                YUNREACHABLE();
        }

        if (hasAttributes) {
            VisitAttributes(node->GetAttributes());
        }
    }

    void VisitScalar(INode::TPtr node, bool hasAttributes)
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

    void VisitEntity(INode::TPtr node, bool hasAttributes)
    {
        UNUSED(node);
        Consumer->OnEntity(hasAttributes);
    }

    void VisitList(IListNode::TPtr node, bool hasAttributes)
    {
        Consumer->OnBeginList();
        for (int i = 0; i < node->GetChildCount(); ++i) {
            auto child = node->GetChild(i);
            Consumer->OnListItem();
            VisitAny(child);
        }
        Consumer->OnEndList(hasAttributes);
    }

    void VisitMap(IMapNode::TPtr node, bool hasAttributes)
    {
        Consumer->OnBeginMap();
        FOREACH(const auto& pair, node->GetChildren()) {
            Consumer->OnMapItem(pair.First());
            VisitAny(pair.Second());
        }
        Consumer->OnEndMap(hasAttributes);
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
