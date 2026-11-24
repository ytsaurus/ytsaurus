#pragma once

#include "public.h"

#include "client_factory.h"
#include "credentials.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYdbTopicDriverConfig
    : public NYTree::TYsonStruct
{
    EYdbAuthMode AuthMode{};
    std::string TokenEnvVar;
    std::string Username;
    std::string PasswordEnvVar;
    int NetworkThreadCount{};
    int ClientThreadCount{};
    bool StopDriverBeforeDestroying{};

    TYdbCredentialsConfig GetCredentialsConfig() const;

    REGISTER_YSON_STRUCT(TYdbTopicDriverConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbTopicDriverConfig);

////////////////////////////////////////////////////////////////////////////////

class TYdbTopicDriver
    : public TResourceBase
    , public IYdbClientFactory
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbTopicDriverConfig);

    //! |injectedCredentials| replaces the configured credentials.
    TYdbTopicDriver(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> injectedCredentials = {});

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    IYdbWriterPtr CreateWriter(
        const std::string& endpoint,
        const std::string& database,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler) override;

    IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        const std::string& endpoint,
        const std::string& database) override;

    TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& options,
        TDuration timeout,
        std::optional<TYdbWriteTarget> preferredTarget,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets) override;

private:
    const TYdbDriverCorePtr Core_;
};

DEFINE_REFCOUNTED_TYPE(TYdbTopicDriver);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
