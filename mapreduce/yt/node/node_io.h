#pragma once

#include "node.h"
#include <library/yson/public.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const TStringBuf input, EYsonType type = YT_NODE);
TString NodeToYsonString(const TNode& node, EYsonFormat format = YF_TEXT);

TNode NodeFromYsonStream(IInputStream* input, EYsonType type = YT_NODE);
void NodeToYsonStream(const TNode& node, IOutputStream* output, EYsonFormat format = YF_TEXT);

TNode NodeFromJsonString(const TStringBuf input);
TNode NodeFromJsonValue(const NJson::TJsonValue& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
