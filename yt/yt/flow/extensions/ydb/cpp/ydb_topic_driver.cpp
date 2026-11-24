#include "ydb_topic_driver.h"

#include "driver_core.h"
#include "private.h"

#include <library/cpp/yt/logging/backends/arcadia/backend.h>

#include <util/system/env.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TYdbTopicDriverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("auth_mode", &TThis::AuthMode)
        .Default(EYdbAuthMode::Auto);
    registrar.Parameter("token_env_var", &TThis::TokenEnvVar)
        .Default("YDB_TOKEN");
    registrar.Parameter("username", &TThis::Username)
        .Default();
    registrar.Parameter("password_env_var", &TThis::PasswordEnvVar)
        .Default();
    registrar.Parameter("network_thread_count", &TThis::NetworkThreadCount)
        .Default(2);
    registrar.Parameter("client_thread_count", &TThis::ClientThreadCount)
        .Default(0); // Default: if 0 adaptive thread pool will be used.
    registrar.Parameter("stop_driver_before_destroying", &TThis::StopDriverBeforeDestroying)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        switch (config->AuthMode) {
            case EYdbAuthMode::Auto:
            case EYdbAuthMode::Token:
                // clang-format off
                THROW_ERROR_EXCEPTION_IF(config->TokenEnvVar.empty(), "Authentication mode %Qlv requires %Qv",
                    config->AuthMode,
                    "token_env_var");
                // clang-format on
                break;
            case EYdbAuthMode::Login:
                // clang-format off
                THROW_ERROR_EXCEPTION_IF(config->Username.empty() || config->PasswordEnvVar.empty(), "Authentication mode %Qlv requires %Qv and %Qv",
                    config->AuthMode,
                    "username",
                    "password_env_var");
                // clang-format on
                break;
            case EYdbAuthMode::Anonymous:
                break;
        }
    });
}

TYdbCredentialsConfig TYdbTopicDriverConfig::GetCredentialsConfig() const
{
    return {
        .AuthMode = AuthMode,
        .TokenEnvVar = TokenEnvVar,
        .Username = Username,
        .PasswordEnvVar = PasswordEnvVar,
    };
}

////////////////////////////////////////////////////////////////////////////////

TYdbTopicDriver::TYdbTopicDriver(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> injectedCredentials)
    : TResourceBase(std::move(context), std::move(dynamicContext))
    , Core_([&] {
        auto driverConfig = NYdb::TDriverConfig()
            .SetNetworkThreadsNum(GetParameters()->NetworkThreadCount)
            .SetClientThreadsNum(GetParameters()->ClientThreadCount)
            .SetLog(std::unique_ptr<TLogBackend>(NLogging::CreateArcadiaLogBackend(YdbTopicLogger).Release()));
        auto credentials = ResolveYdbCredentials(
            GetParameters()->GetCredentialsConfig(),
            [] (const std::string& name) {
                return std::string(GetEnv(TString(name)));
            },
            std::move(injectedCredentials));
        if (credentials) {
            driverConfig.SetCredentialsProviderFactory(std::move(credentials));
        }
        return New<TYdbDriverCore>(std::move(driverConfig), GetParameters()->StopDriverBeforeDestroying);
    }())
{ }

TFuture<void> TYdbTopicDriver::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    return OKFuture;
}

std::shared_ptr<NYdb::NTopic::TTopicClient> TYdbTopicDriver::CreateTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    return Core_->CreateTopicClient(endpoint, database);
}

std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> TYdbTopicDriver::CreateFederatedTopicClient(
    const std::string& endpoint,
    const std::string& database)
{
    return Core_->CreateFederatedTopicClient(endpoint, database);
}

IYdbWriterPtr TYdbTopicDriver::CreateWriter(
    const std::string& endpoint,
    const std::string& database,
    NYdb::NTopic::TWriteSessionSettings settings,
    IInvokerPtr invoker,
    NLogging::TLogger logger,
    IStatusProfilerPtr statusProfiler)
{
    return Core_->CreateWriter(
        MakeStrong(this),
        endpoint,
        database,
        std::move(settings),
        std::move(invoker),
        std::move(logger),
        std::move(statusProfiler));
}

IDeferredPublicationClientPtr TYdbTopicDriver::CreateDeferredPublicationClient(
    const std::string& endpoint,
    const std::string& database)
{
    return Core_->CreateDeferredPublicationClient(MakeStrong(this), endpoint, database);
}

TFuture<TYdbWriteTargetResolution> TYdbTopicDriver::ResolveWriteTarget(
    const TYdbTopologyOptions& options,
    TDuration timeout,
    std::optional<TYdbWriteTarget> preferredTarget,
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets)
{
    return Core_->ResolveWriteTarget(
        options,
        timeout,
        GetBackgroundInvoker(),
        std::move(preferredTarget),
        std::move(excludedTargets));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
