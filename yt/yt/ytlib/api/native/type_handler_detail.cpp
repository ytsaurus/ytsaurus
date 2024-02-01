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
using namespace NTabletClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

TTypeHandlerBase::TTypeHandlerBase(TClient* client)
    : Client_(client)
{
    Y_UNUSED(Client_);
}

std::optional<TObjectId> TTypeHandlerBase::CreateObject(
    EObjectType /*type*/,
    const TCreateObjectOptions& /*options*/)
{
    return {};
}

std::optional<TNodeId> TTypeHandlerBase::CreateNode(
    EObjectType /*type*/,
    const TYPath& /*path*/,
    const TCreateNodeOptions& /*options*/)
{
    return {};
}

std::optional<TYsonString> TTypeHandlerBase::GetNode(
    const TYPath& /*path*/,
    const TGetNodeOptions& /*options*/)
{
    return {};
}

std::optional<TYsonString> TTypeHandlerBase::ListNode(
    const TYPath& /*path*/,
    const TListNodeOptions& /*options*/)
{
    return {};
}

std::optional<bool> TTypeHandlerBase::NodeExists(
    const TYPath& /*path*/,
    const TNodeExistsOptions& /*options*/)
{
    return {};
}

std::optional<std::monostate> TTypeHandlerBase::RemoveNode(
    const TYPath& /*path*/,
    const TRemoveNodeOptions& /*options*/)
{
    return {};
}

std::optional<std::monostate> TTypeHandlerBase::AlterTableReplica(
    TTableReplicaId /*replicaId*/,
    const TAlterTableReplicaOptions& /*options*/)
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TObjectId> TVirtualTypeHandler::CreateObject(
    EObjectType type,
    const TCreateObjectOptions& options)
{
    if (type != GetSupportedObjectType()) {
        return {};
    }
    return DoCreateObject(options);
}

std::optional<TYsonString> TVirtualTypeHandler::GetNode(
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

std::optional<TYsonString> TVirtualTypeHandler::ListNode(
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

std::optional<bool> TVirtualTypeHandler::NodeExists(
    const TYPath& path,
    const TNodeExistsOptions& /*options*/)
{
    try {
        TYPath pathSuffix;
        if (auto optionalYson = TryGetObjectYson(path, &pathSuffix)) {
            return SyncYPathExists(
                ConvertToNode(*optionalYson),
                pathSuffix);
        }
    } catch (const std::exception& ex) {
        if (TError(ex).FindMatching(NYTree::EErrorCode::ResolveError)) {
            return false;
        }
        throw;
    }

    return {};
}

std::optional<std::monostate> TVirtualTypeHandler::RemoveNode(
    const TYPath& path,
    const TRemoveNodeOptions& options)
{
    TObjectId objectId;
    TYPath pathSuffix;
    if (!TryParseObjectId(path, &objectId, &pathSuffix)) {
        return {};
    }
    if (TypeFromId(objectId) != GetSupportedObjectType()) {
        return {};
    }
    if (!pathSuffix.empty()) {
        THROW_ERROR_EXCEPTION("Can only remove object as a whole");
    }

    DoRemoveObject(objectId, options);

    return std::monostate();
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

std::optional<TObjectId> TVirtualTypeHandler::DoCreateObject(
    const TCreateObjectOptions& /*options*/)
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

