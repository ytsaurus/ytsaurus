#include "public.h"

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NApi {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

EMasterChannelKind ParseMasterChannelKind(TStringBuf source)
{
    if (source == "local_cache") {
        return EMasterChannelKind::ClientSideCache;
    }
    else if (source == "master_cache") {
        return EMasterChannelKind::MasterSideCache;
    } else {
        return ParseEnum<EMasterChannelKind>(source);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Deserialize(EMasterChannelKind& value, TYsonPullParserCursor* cursor)
{
    EnsureYsonToken("enum", *cursor, EYsonItemType::StringValue);
    value = ParseMasterChannelKind((*cursor)->UncheckedAsString());
    cursor->Next();
}

void Deserialize(EMasterChannelKind& value, INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::String: {
            value = ParseMasterChannelKind(node->AsString()->GetValue());
            break;
        }
        case ENodeType::Int64: {
            value = CheckedEnumCast<EMasterChannelKind>(node->AsInt64()->GetValue());
            break;
        }
        default:
            THROW_ERROR_EXCEPTION("Cannot deserialize enum from %Qlv node",
                node->GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
