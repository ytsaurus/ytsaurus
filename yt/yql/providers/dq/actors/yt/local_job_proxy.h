#pragma once

#include <yt/yql/providers/dq/config/config.pb.h>

#include <util/generic/string.h>

namespace NYql {

inline TString GetResourceDownloadProxySocket(
    const NProto::TDqConfig::TYtBackend& backend,
    const TString& clusterAddress,
    TStringBuf socketPath)
{
    if (!backend.GetUseLocalJobProxyForResourceDownloads() || backend.GetProxyAddress() != clusterAddress || socketPath.empty()) {
        return {};
    }
    return TString(socketPath);
}

} // namespace NYql
