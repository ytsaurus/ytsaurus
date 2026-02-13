#include "helpers.h"

#include "private.h"

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NObjectServer {

using namespace NYT::NYPath;
using namespace NYT::NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ObjectServerLogger;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TYPathRewrite& rewrite, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v -> %v",
        rewrite.Original,
        rewrite.Rewritten);
}

TYPathRewrite MakeYPathRewrite(
    const TYPath& originalPath,
    TObjectId targetObjectId,
    const TYPath& pathSuffix)
{
    return TYPathRewrite{
        .Original = originalPath,
        .Rewritten = FromObjectId(targetObjectId) + pathSuffix
    };
}

TYPathRewrite MakeYPathRewrite(
    const TYPath& originalPath,
    const TYPath& rewrittenPath)
{
    return TYPathRewrite{
        .Original = originalPath,
        .Rewritten = rewrittenPath
    };
}

void ValidateFolderId(const std::string& folderId)
{
    static constexpr size_t MaxFolderIdLength = 256;
    if (folderId.size() > MaxFolderIdLength) {
        THROW_ERROR_EXCEPTION("Folder id %Qv is too long", folderId)
            << TErrorAttribute("length", folderId.size())
            << TErrorAttribute("max_folder_id_length", MaxFolderIdLength);
    }
    if (folderId.empty()) {
        THROW_ERROR_EXCEPTION("Folder id cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

TError CheckObjectName(TStringBuf name)
{
    if (name.StartsWith(ObjectIdPathPrefix)) {
        auto error = TError("Invalid object name: starts with %v", ObjectIdPathPrefix)
            << TErrorAttribute("name", name);
        if (NHiveServer::IsHiveMutation()) {
            YT_LOG_ALERT(error, "Invalid object name in Hive mutation (Name: %v)", name);
            return {};
        }
        return error;
    }

    return {};
}

std::variant<TObjectId, TStringBuf, TError> ParseObjectNameOrId(TStringBuf name)
{
    if (name.StartsWith(ObjectIdPathPrefix)) {
        auto idString = name.SubString(ObjectIdPathPrefix.size(), name.size() - ObjectIdPathPrefix.size());
        TObjectId id;
        if (!TObjectId::FromString(idString, &id)) {
            return TError("Error parsing object ID from string %Qv", name);
        }
        return id;
    }
    return name;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
