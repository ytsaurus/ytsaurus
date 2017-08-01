#pragma once

#include "node.h"
#include <library/yson/public.h>

namespace NJson {
    class TJsonValue;
} // namespace NJson

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const TString& input, EYsonType type = YT_NODE);
TString NodeToYsonString(const TNode& node, EYsonFormat format = YF_TEXT);

// TODO: EYsonType argument should be removed since it doesn't affect anything
// (check unittest MakeSureThatSecondParamDoesntAffectAnything)
TNode NodeFromJsonString(const TString& input, EYsonType type = YT_NODE);
TNode NodeFromJsonValue(const NJson::TJsonValue& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
