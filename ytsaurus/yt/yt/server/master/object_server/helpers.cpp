#include "helpers.h"
#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NObjectServer {

using namespace NYT::NYPath;
using namespace NYT::NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TYPathRewrite& rewrite, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v -> %v",
        rewrite.Original,
        rewrite.Rewritten);
}

TString ToString(const TYPathRewrite& rewrite)
{
    return ToStringViaBuilder(rewrite);
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

TDuration ComputeForwardingTimeout(
    TDuration timeout,
    const TObjectServiceConfigPtr& config)
{
    return timeout > 2 * config->ForwardedRequestTimeoutReserve
        ? timeout - config->ForwardedRequestTimeoutReserve
        : timeout;
}

TDuration ComputeForwardingTimeout(
    const NRpc::IServiceContextPtr& context,
    const TObjectServiceConfigPtr& config)
{
    if (!context->GetStartTime() || !context->GetTimeout()) {
        return config->DefaultExecuteTimeout;
    }

    return ComputeForwardingTimeout(
        *context->GetStartTime() + *context->GetTimeout() - NProfiling::GetInstant(),
        config);
}

void ValidateFolderId(const TString& folderId)
{
    static constexpr size_t MaxFolderIdLength = 256;
    if (folderId.size() > MaxFolderIdLength) {
        THROW_ERROR_EXCEPTION("Folder id %Qv is too long")
            << TErrorAttribute("length", folderId.size())
            << TErrorAttribute("max_folder_id_length", MaxFolderIdLength);
    }
    if (folderId.empty()) {
        THROW_ERROR_EXCEPTION("Folder id cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
