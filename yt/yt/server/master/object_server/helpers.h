#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TYPathRewrite
{
    NYPath::TYPath Original;
    NYPath::TYPath Rewritten;
};

void FormatValue(TStringBuilderBase* builder, const TYPathRewrite& rewrite, TStringBuf /*spec*/);

TYPathRewrite MakeYPathRewrite(
    const NYPath::TYPath& originalPath,
    NObjectClient::TObjectId targetObjectId,
    const NYPath::TYPath& pathSuffix);
TYPathRewrite MakeYPathRewrite(
    const NYPath::TYPath& originalPath,
    const NYPath::TYPath& rewrittenPath);

void ValidateFolderId(const std::string& folderId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
