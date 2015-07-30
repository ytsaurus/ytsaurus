#include "node_visitor.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeVisitor::TNodeVisitor(IYsonConsumer* consumer)
    : Consumer_(consumer)
{ }

void TNodeVisitor::Visit(const TNode& node)
{
    VisitAny(node);
}

void TNodeVisitor::VisitAny(const TNode& node)
{
    if (node.HasAttributes()) {
        Consumer_->OnBeginAttributes();
        for (const auto& item : node.GetAttributes().AsMap()) {
            Consumer_->OnKeyedItem(item.first);
            VisitAny(item.second);
        }
        Consumer_->OnEndAttributes();
    }

    switch (node.GetType()) {
        case TNode::STRING:
            VisitString(node);
            break;
        case TNode::INT64:
            VisitInt64(node);
            break;
        case TNode::UINT64:
            VisitUint64(node);
            break;
        case TNode::DOUBLE:
            VisitDouble(node);
            break;
        case TNode::BOOL:
            VisitBool(node);
            break;
        case TNode::LIST:
            VisitList(node);
            break;
        case TNode::MAP:
            VisitMap(node);
            break;
        case TNode::ENTITY:
            VisitEntity();
            break;
        default:
            ythrow TNode::TTypeError()
                << Sprintf("unable to visit TNode of type %s",
                    ~TNode::TypeToString(node.GetType()));
    }
}

void TNodeVisitor::VisitString(const TNode& node)
{
    Consumer_->OnStringScalar(node.AsString());
}

void TNodeVisitor::VisitInt64(const TNode& node)
{
    Consumer_->OnInt64Scalar(node.AsInt64());
}

void TNodeVisitor::VisitUint64(const TNode& node)
{
    Consumer_->OnUint64Scalar(node.AsUint64());
}

void TNodeVisitor::VisitDouble(const TNode& node)
{
    Consumer_->OnDoubleScalar(node.AsDouble());
}

void TNodeVisitor::VisitBool(const TNode& node)
{
    Consumer_->OnBooleanScalar(node.AsBool());
}

void TNodeVisitor::VisitList(const TNode& node)
{
    Consumer_->OnBeginList();
    for (const auto& item : node.AsList()) {
        Consumer_->OnListItem();
        VisitAny(item);
    }
    Consumer_->OnEndList();
}

void TNodeVisitor::VisitMap(const TNode& node)
{
    Consumer_->OnBeginMap();
    for (const auto& item : node.AsMap()) {
        Consumer_->OnKeyedItem(item.first);
        VisitAny(item.second);
    }
    Consumer_->OnEndMap();
}

void TNodeVisitor::VisitEntity()
{
    Consumer_->OnEntity();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
