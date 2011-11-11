#include "stdafx.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

TNodeSetterBase::TNodeSetterBase(INode* node, ITreeBuilder* builder)
    : Node(node)
    , Builder(builder)
{ }

void TNodeSetterBase::ThrowInvalidType(ENodeType actualType)
{
    throw TYTreeException() << Sprintf("Cannot change node type from %s to %s",
        ~GetExpectedType().ToString().Quote(),
        ~actualType.ToString().Quote());
}

void TNodeSetterBase::OnMyStringScalar(const Stroka& value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::String);
}

void TNodeSetterBase::OnMyInt64Scalar(i64 value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Int64);
}

void TNodeSetterBase::OnMyDoubleScalar(double value, bool hasAttributes)
{
    UNUSED(value);
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Double);
}

void TNodeSetterBase::OnMyEntity(bool hasAttributes)
{
    UNUSED(hasAttributes);
    ThrowInvalidType(ENodeType::Entity);
}

void TNodeSetterBase::OnMyBeginList()
{
    ThrowInvalidType(ENodeType::List);
}

void TNodeSetterBase::OnMyListItem()
{
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndList(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginMap()
{
    ThrowInvalidType(ENodeType::Map);
}

void TNodeSetterBase::OnMyMapItem(const Stroka& name)
{
    UNUSED(name);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyEndMap(bool hasAttributes)
{
    UNUSED(hasAttributes);
    YUNREACHABLE();
}

void TNodeSetterBase::OnMyBeginAttributes()
{
    auto attributes = Node->GetAttributes();
    if (~attributes == NULL) {
        Node->SetAttributes(Node->GetFactory()->CreateMap());
    } else {
        attributes->Clear();
    }
}

void TNodeSetterBase::OnMyAttributesItem(const Stroka& name)
{
    YASSERT(~AttributeBuilder == NULL);
    AttributeName = name;
    AttributeBuilder = CreateBuilderFromFactory(Node->GetFactory());
    AttributeBuilder->BeginTree();
    ForwardNode(~AttributeBuilder, FromMethod(&TThis::OnForwardingFinished, this));
}

void TNodeSetterBase::OnForwardingFinished()
{
    YASSERT(~AttributeBuilder != NULL);
    Node->GetAttributes()->AddChild(AttributeBuilder->EndTree(), AttributeName);
    AttributeBuilder.Destroy();
    AttributeName.clear();
}

void TNodeSetterBase::OnMyEndAttributes()
{
    if (Node->GetAttributes()->GetChildCount() == 0) {
        Node->SetAttributes(NULL);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
