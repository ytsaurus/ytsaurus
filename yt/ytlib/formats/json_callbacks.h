#pragma once

#include "public.h"
#include "utf8_decoder.h"

#include <yt/core/ytree/public.h>
#include <yt/core/ytree/tree_builder.h>

#include <queue>
#include <stack>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJsonCallbacksNodeType,
    (List)
    (Map)
);

class TJsonCallbacks
{
public:
    TJsonCallbacks(const TUtf8Transcoder& utf8Transcoder, i64 memoryLimit);

    void OnStringScalar(const TStringBuf& value);
    void OnInt64Scalar(i64 value);
    void OnUint64Scalar(ui64 value);
    void OnDoubleScalar(double value);
    void OnBooleanScalar(bool value);
    void OnEntity();
    void OnBeginList();
    void OnEndList();
    void OnBeginMap();
    void OnKeyedItem(const TStringBuf& key);
    void OnEndMap();

    bool HasFinishedNodes() const;
    NYTree::INodePtr ExtractFinishedNode();

private:
    // Memory accounted approximately
    void AccountMemory(i64 memory);
    void OnItemStarted();
    void OnItemFinished();

    TUtf8Transcoder Utf8Transcoder_;
    i64 ConsumedMemory_ = 0;
    const i64 MemoryLimit_;

    using ENodeType = EJsonCallbacksNodeType;
    std::stack<ENodeType> Stack_;

    const std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    std::queue<NYTree::INodePtr> FinishedNodes_;
    std::queue<i64> NodesMemory_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
