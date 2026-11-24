#pragma once

#include "public.h"

#include <ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <functional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Names where credentials come from; never holds secret values.
struct TYdbCredentialsConfig
{
    EYdbAuthMode AuthMode = EYdbAuthMode::Auto;
    std::string TokenEnvVar = "YDB_TOKEN";
    std::string Username;
    std::string PasswordEnvVar;
};

using TYdbEnvironmentReader = std::function<std::string(const std::string&)>;

//! Returns null for anonymous access.
std::shared_ptr<NYdb::ICredentialsProviderFactory> ResolveYdbCredentials(
    const TYdbCredentialsConfig& config,
    const TYdbEnvironmentReader& readEnvironment,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> injected);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
