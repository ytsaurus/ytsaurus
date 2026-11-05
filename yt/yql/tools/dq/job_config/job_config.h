#pragma once

#include <yt/yql/providers/dq/config/config.pb.h>

#include <util/generic/string.h>

namespace NYql::NDq::NJobConfig {

extern const TString CoordinatorConfigFile;
extern const TString BackendConfigFile;
extern const TString YtTokenVaultKey;

TString ReadProtoConfigText(const TString& vaultKey, const TString& fileName);

void ApplyTokenFromVault(
    NProto::TDqConfig::TYtCoordinator& coordinatorConfig,
    NProto::TDqConfig::TYtBackend& backendConfig);

} // namespace NYql::NDq::NJobConfig
