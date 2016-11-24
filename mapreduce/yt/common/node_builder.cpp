#include "node_builder.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNodeBuilder::TNodeBuilder(TNode* node)
{
    Stack_.push(node);
}

void TNodeBuilder::OnStringScalar(const TStringBuf& value)
{
    AddNode(value, true);
}

void TNodeBuilder::OnInt64Scalar(i64 value)
{
    AddNode(value, true);
}

void TNodeBuilder::OnUint64Scalar(ui64 value)
{
    AddNode(value, true);
}

void TNodeBuilder::OnDoubleScalar(double value)
{
    AddNode(value, true);
}

void TNodeBuilder::OnBooleanScalar(bool value)
{
    AddNode(value, true);
}

void TNodeBuilder::OnEntity()
{
    AddNode(TNode::CreateEntity(), true);
}

void TNodeBuilder::OnBeginList()
{
    AddNode(TNode::CreateList(), false);
}

void TNodeBuilder::OnListItem()
{
    Stack_.push(&Stack_.top()->Add());
}

void TNodeBuilder::OnEndList()
{
    Stack_.pop();
}

void TNodeBuilder::OnBeginMap()
{
    AddNode(TNode::CreateMap(), false);
}

void TNodeBuilder::OnKeyedItem(const TStringBuf& key)
{
    Stack_.push(&(*Stack_.top())[Stroka(key)]);
}

void TNodeBuilder::OnEndMap()
{
    Stack_.pop();
}

void TNodeBuilder::OnBeginAttributes()
{
    Stack_.push(&Stack_.top()->Attributes());
}

void TNodeBuilder::OnEndAttributes()
{
    Stack_.pop();
}

void TNodeBuilder::AddNode(TNode value, bool pop)
{
    Stack_.top()->MoveWithoutAttributes(std::move(value));
    if (pop)
        Stack_.pop();
}

TYson2JsonCallbacksAdapter::TYson2JsonCallbacksAdapter(TYsonConsumerBase* impl, bool throwException)
    : NJson::TJsonCallbacks(throwException)
    , Impl(impl)
{}

bool TYson2JsonCallbacksAdapter::OnNull()
{
    WrapIfListItem();
    Impl->OnEntity();
    return true;
}

bool TYson2JsonCallbacksAdapter::OnBoolean(bool val)
{
    WrapIfListItem();
    Impl->OnBooleanScalar(val);
    return true;
}

bool TYson2JsonCallbacksAdapter::OnInteger(long long val)
{
    WrapIfListItem();
    Impl->OnInt64Scalar(val);
    return true;
}

bool TYson2JsonCallbacksAdapter::OnUInteger(unsigned long long val)
{
    WrapIfListItem();
    Impl->OnUint64Scalar(val);
    return true;
}

bool TYson2JsonCallbacksAdapter::OnString(const TStringBuf &val)
{
    WrapIfListItem();
    Impl->OnStringScalar(val);
    return true;
}

bool TYson2JsonCallbacksAdapter::OnDouble(double val)
{
    WrapIfListItem();
    Impl->OnDoubleScalar(val);
    return true;
}

bool TYson2JsonCallbacksAdapter::OnOpenArray()
{
    WrapIfListItem();
    ContextStack.push(true);
    Impl->OnBeginList();
    return true;
}

bool TYson2JsonCallbacksAdapter::OnCloseArray()
{
    ContextStack.pop();
    Impl->OnEndList();
    return true;
}

bool TYson2JsonCallbacksAdapter::OnOpenMap()
{
    WrapIfListItem();
    ContextStack.push(false);
    Impl->OnBeginMap();
    return true;
}

bool TYson2JsonCallbacksAdapter::OnCloseMap()
{
    ContextStack.pop();
    Impl->OnEndMap();
    return true;
}

bool TYson2JsonCallbacksAdapter::OnMapKey(const TStringBuf &val)
{
    Impl->OnKeyedItem(val);
    return true;
}

void TYson2JsonCallbacksAdapter::WrapIfListItem() {
    if (!ContextStack.empty() && ContextStack.top()) {
        Impl->OnListItem();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
