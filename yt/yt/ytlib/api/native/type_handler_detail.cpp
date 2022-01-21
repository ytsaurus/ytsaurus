#include "type_handler_detail.h"

#include "ypath_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

std::optional<NYson::TYsonString> TNullTypeHandler::GetNode(
    const TYPath& /*path*/,
    const TGetNodeOptions& /*options*/)
{
    return {};
}

std::optional<NYson::TYsonString> TNullTypeHandler::ListNode(
    const TYPath& /*path*/,
    const TListNodeOptions& /*options*/)
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<NYson::TYsonString> TVirtualTypeHandler::GetNode(
    const TYPath& path,
    const TGetNodeOptions& options)
{
    TYPath pathSuffix;
    if (auto optionalYson = TryGetObjectYson(path, &pathSuffix)) {
        return SyncYPathGet(
            ConvertToNode(*optionalYson),
            pathSuffix,
            options.Attributes);
    }

    return {};
}

std::optional<NYson::TYsonString> TVirtualTypeHandler::ListNode(
    const TYPath& path,
    const TListNodeOptions& /*options*/)
{
    TYPath pathSuffix;
    if (auto optionalYson = TryGetObjectYson(path, &pathSuffix)) {
        return ConvertToYsonString(SyncYPathList(
            ConvertToNode(*optionalYson),
            pathSuffix));
    }

    return {};
}

std::optional<TYsonString> TVirtualTypeHandler::TryGetObjectYson(
    const TYPath& path,
    TYPath* pathSuffix)
{
    TObjectId objectId;
    if (!TryParseObjectId(path, &objectId, pathSuffix)) {
        return {};
    }

    if (TypeFromId(objectId) != GetSupportedObjectType()) {
        return {};
    }

    return GetObjectYson(objectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

