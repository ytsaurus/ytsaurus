#include "json_callbacks.h"

#include <core/ytree/node.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/ephemeral_node_factory.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TJsonCallbacks::TJsonCallbacks()
{ }

TJsonCallbacks::TJsonCallbacks(const TUtf8Transcoder& utf8Transcoder)
    : Utf8Transcoder_(utf8Transcoder)
    , TreeBuilder_(NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory()))
{
    TreeBuilder_->BeginTree();
}

void TJsonCallbacks::OnStringScalar(const TStringBuf& value)
{
    OnItemStarted();
    TreeBuilder_->OnStringScalar(Utf8Transcoder_.Decode(value));
    OnItemFinished();
}

void TJsonCallbacks::OnIntegerScalar(i64 value)
{
    OnItemStarted();
    TreeBuilder_->OnIntegerScalar(value);
    OnItemFinished();
}

void TJsonCallbacks::OnDoubleScalar(double value)
{
    OnItemStarted();
    TreeBuilder_->OnDoubleScalar(value);
    OnItemFinished();
}

void TJsonCallbacks::OnEntity()
{
    OnItemStarted();
    TreeBuilder_->OnEntity();
    OnItemFinished();
}

void TJsonCallbacks::OnBeginList()
{
    OnItemStarted();
    TreeBuilder_->OnBeginList();
    Stack_.push(ENodeType::List);
}

void TJsonCallbacks::OnEndList()
{
    TreeBuilder_->OnEndList();
    Stack_.pop();
    OnItemFinished();
}

void TJsonCallbacks::OnBeginMap()
{
    OnItemStarted();
    TreeBuilder_->OnBeginMap();
    Stack_.push(ENodeType::Map);
}

void TJsonCallbacks::OnKeyedItem(const TStringBuf& key)
{
    TreeBuilder_->OnKeyedItem(Utf8Transcoder_.Decode(key));
}

void TJsonCallbacks::OnEndMap()
{
    TreeBuilder_->OnEndMap();
    Stack_.pop();
    OnItemFinished();
}

void TJsonCallbacks::OnItemStarted()
{
    if (!Stack_.empty() && Stack_.top() == ENodeType::List)
    {
        TreeBuilder_->OnListItem();
    }
}

void TJsonCallbacks::OnItemFinished()
{
    if (Stack_.empty()) {
        FinishedNodes_.push(TreeBuilder_->EndTree());
        TreeBuilder_->BeginTree();
    }
}

bool TJsonCallbacks::HasFinishedNodes() const
{
    return !FinishedNodes_.empty();
}

NYTree::INodePtr TJsonCallbacks::ExtractFinishedNode()
{
    YCHECK(!FinishedNodes_.empty());
    NYTree::INodePtr node = FinishedNodes_.front();
    FinishedNodes_.pop();
    return node;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
