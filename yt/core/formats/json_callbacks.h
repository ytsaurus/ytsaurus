#pragma once

#include "public.h"
#include "utf8_decoder.h"

#include <core/ytree/public.h>
#include <core/ytree/tree_builder.h>

#include <stack>
#include <queue>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TJsonCallbacks {
public:
    TJsonCallbacks();
    TJsonCallbacks(const TUtf8Transcoder& utf8Transcoder);

    void OnStringScalar(const TStringBuf& value);
    void OnIntegerScalar(i64 value);
    void OnDoubleScalar(double value);
    void OnEntity();
    void OnBeginList();
    void OnEndList();
    void OnBeginMap();
    void OnKeyedItem(const TStringBuf& key);
    void OnEndMap();

    bool HasFinishedNodes() const;
    NYTree::INodePtr ExtractFinishedNode();

private:
    DECLARE_ENUM(ENodeType,
        (List)
        (Map)
    );

    void OnItemStarted();
    void OnItemFinished();

    TUtf8Transcoder Utf8Transcoder_;
    std::stack<ENodeType> Stack_;

    std::unique_ptr<NYTree::ITreeBuilder> TreeBuilder_;
    std::queue<NYTree::INodePtr> FinishedNodes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
