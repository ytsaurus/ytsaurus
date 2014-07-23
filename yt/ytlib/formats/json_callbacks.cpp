#include "json_callbacks.h"

#include <core/ytree/node.h>
#include <core/ytree/tree_builder.h>
#include <core/ytree/ephemeral_node_factory.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TJsonCallbacks::TJsonCallbacks()
{ }

TJsonCallbacks::TJsonCallbacks(const TUtf8Transcoder& utf8Transcoder, i64 memoryLimit)
    : Utf8Transcoder_(utf8Transcoder)
    , ConsumedMemory_(0)
    , MemoryLimit_(memoryLimit)
    , TreeBuilder_(NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory()))
{
    TreeBuilder_->BeginTree();
    NodesMemory_.push(0);
}

void TJsonCallbacks::OnStringScalar(const TStringBuf& value)
{
    AccountMemory(value.Size());
    OnItemStarted();
    TreeBuilder_->OnStringScalar(Utf8Transcoder_.Decode(value));
    OnItemFinished();
}

void TJsonCallbacks::OnInt64Scalar(i64 value)
{
    AccountMemory(sizeof(value));
    OnItemStarted();
    TreeBuilder_->OnInt64Scalar(value);
    OnItemFinished();
}

void TJsonCallbacks::OnDoubleScalar(double value)
{
    AccountMemory(sizeof(value));
    OnItemStarted();
    TreeBuilder_->OnDoubleScalar(value);
    OnItemFinished();
}

void TJsonCallbacks::OnEntity()
{
    AccountMemory(0);
    OnItemStarted();
    TreeBuilder_->OnEntity();
    OnItemFinished();
}

void TJsonCallbacks::OnBeginList()
{
    AccountMemory(0);
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
    AccountMemory(0);
    OnItemStarted();
    TreeBuilder_->OnBeginMap();
    Stack_.push(ENodeType::Map);
}

void TJsonCallbacks::OnKeyedItem(const TStringBuf& key)
{
    AccountMemory(sizeof(key.size()));
    TreeBuilder_->OnKeyedItem(Utf8Transcoder_.Decode(key));
}

void TJsonCallbacks::OnEndMap()
{
    TreeBuilder_->OnEndMap();
    Stack_.pop();
    OnItemFinished();
}

void TJsonCallbacks::AccountMemory(i64 memory)
{
    memory += sizeof(NYTree::INodePtr);
    if (ConsumedMemory_ + memory > MemoryLimit_) {
        THROW_ERROR_EXCEPTION(
            "Memory limit exceeded while parsing JSON: allocated %" PRId64 ", limit %" PRId64,
            ConsumedMemory_ + memory,
            MemoryLimit_);
    }
    ConsumedMemory_ += memory;
    NodesMemory_.back() += memory;
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
        NodesMemory_.push(0);
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

    ConsumedMemory_ -= NodesMemory_.front();
    NodesMemory_.pop();

    NYTree::INodePtr node = FinishedNodes_.front();
    FinishedNodes_.pop();

    return node;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
