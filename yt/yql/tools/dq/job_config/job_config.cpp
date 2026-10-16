#include "job_config.h"

#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/fs.h>

namespace NYql::NDq::NJobConfig {

const TString CoordinatorConfigFile = "yt_coordinator.cfg";
const TString BackendConfigFile = "yt_backend.cfg";
const TString YtTokenVaultKey = "YT_TOKEN";

TString ReadProtoConfigText(const TString& vaultKey, const TString& fileName) {
    TString fromVault = GetEnv(TString("YT_SECURE_VAULT_") + vaultKey, "");
    if (!fromVault.empty()) {
        return fromVault;
    }
    if (NFs::Exists(fileName)) {
        return TFileInput(fileName).ReadAll();
    }
    return "";
}

void ApplyTokenFromVault(
    NProto::TDqConfig::TYtCoordinator& coordinatorConfig,
    NProto::TDqConfig::TYtBackend& backendConfig)
{
    if (coordinatorConfig.HasToken()) {
        if (!backendConfig.HasToken()) {
            backendConfig.SetToken(coordinatorConfig.GetToken());
        }
        return;
    }
    TString token = GetEnv(TString("YT_SECURE_VAULT_") + YtTokenVaultKey, "");
    if (token.empty()) {
        return;
    }
    coordinatorConfig.SetToken(token);
    backendConfig.SetToken(token);
}

} // namespace NYql::NDq::NJobConfig
