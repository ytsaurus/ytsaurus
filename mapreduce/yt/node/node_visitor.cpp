#include "node_visitor.h"

#include <util/string/printf.h>

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
            if (item.second.IsUndefined()) {
                ythrow TNode::TTypeError() << "unable to visit attribute value of type UNDEFINED; attribute name: `" << item.first << '\'' ;
            }
            VisitAny(item.second);
        }
        Consumer_->OnEndAttributes();
    }

    switch (node.GetType()) {
        case TNode::String:
            VisitString(node);
            break;
        case TNode::Int64:
            VisitInt64(node);
            break;
        case TNode::Uint64:
            VisitUint64(node);
            break;
        case TNode::Double:
            VisitDouble(node);
            break;
        case TNode::Bool:
            VisitBool(node);
            break;
        case TNode::List:
            VisitList(node.AsList());
            break;
        case TNode::Map:
            VisitMap(node.AsMap());
            break;
        case TNode::Null:
            VisitEntity();
            break;
        case TNode::Undefined:
            ythrow TNode::TTypeError()
                << Sprintf("unable to visit TNode of type %s",
                    ~TNode::TypeToString(node.GetType()));
        default:
            Y_FAIL("Unexpected type: %d", node.GetType());
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

void TNodeVisitor::VisitList(const TNode::TList& nodeList)
{
    Consumer_->OnBeginList();
    size_t index = 0;
    for (const auto& item : nodeList) {
        Consumer_->OnListItem();
        if (item.IsUndefined()) {
            ythrow TNode::TTypeError() << "unable to visit list node child of type UNDEFINED; list index: " << index;
        }
        VisitAny(item);
        ++index;
    }
    Consumer_->OnEndList();
}

void TNodeVisitor::VisitMap(const TNode::TMap& nodeMap)
{
    Consumer_->OnBeginMap();
    for (const auto& item : nodeMap) {
        Consumer_->OnKeyedItem(item.first);
        if (item.second.IsUndefined()) {
            ythrow TNode::TTypeError() << "unable to visit map node child of type UNDEFINED; map key: `" << item.first << '\'' ;
        }
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
