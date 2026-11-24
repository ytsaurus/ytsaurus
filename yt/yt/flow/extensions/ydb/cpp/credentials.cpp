#include "credentials.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<NYdb::ICredentialsProviderFactory> ResolveYdbCredentials(
    const TYdbCredentialsConfig& config,
    const TYdbEnvironmentReader& readEnvironment,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> injected)
{
    if (injected) {
        return injected;
    }
    if (config.AuthMode == EYdbAuthMode::Login) {
        auto password = readEnvironment(config.PasswordEnvVar);
        // clang-format off
        THROW_ERROR_EXCEPTION_IF(password.empty(), "Configured password environment variable %Qv is empty",
            config.PasswordEnvVar);
        // clang-format on
        return NYdb::CreateLoginCredentialsProviderFactory({config.Username, std::move(password)});
    }
    if (config.AuthMode == EYdbAuthMode::Anonymous) {
        return nullptr;
    }
    auto token = readEnvironment(config.TokenEnvVar);
    if (!token.empty()) {
        return NYdb::CreateOAuthCredentialsProviderFactory(token);
    }
    // clang-format off
    THROW_ERROR_EXCEPTION_IF(config.AuthMode == EYdbAuthMode::Token, "Configured token environment variable %Qv is empty",
        config.TokenEnvVar);
    // clang-format on
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
