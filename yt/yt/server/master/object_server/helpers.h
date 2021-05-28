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
TString ToString(const TYPathRewrite& rewrite);

TYPathRewrite MakeYPathRewrite(
    const NYPath::TYPath& originalPath,
    NObjectClient::TObjectId targetObjectId,
    const NYPath::TYPath& pathSuffix);
TYPathRewrite MakeYPathRewrite(
    const NYPath::TYPath& originalPath,
    const NYPath::TYPath& rewrittenPath);

TDuration ComputeForwardingTimeout(
    TDuration timeout,
    const TObjectServiceConfigPtr& config);
TDuration ComputeForwardingTimeout(
    const NRpc::IServiceContextPtr& context,
    const TObjectServiceConfigPtr& config);

void ValidateFolderId(const TString& folderId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
