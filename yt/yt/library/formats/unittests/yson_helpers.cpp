#include "yson_helpers.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/string/stream.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::string CanonizeYson(TStringBuf input)
{
    auto node = ConvertToNode(TYsonString(input));
    auto binaryYson = ConvertToYsonString(node);

    TStdStringStream out;
    {
        TYsonWriter writer(&out, NYson::EYsonFormat::Pretty);
        ParseYsonStringBuffer(binaryYson.AsStringBuf(), EYsonType::Node, &writer);
    }
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
