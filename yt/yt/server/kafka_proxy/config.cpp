#include "config.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/re2/re2.h>


namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

void TStringTransformationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("match_pattern", &TThis::MatchPattern);
    registrar.Parameter("replacement", &TThis::Replacement)
        .Default();
}

DEFINE_REFCOUNTED_TYPE(TStringTransformationConfig)

////////////////////////////////////////////////////////////////////////////////

void TKafkaProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default(80);
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);

    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);

    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50'000);

    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);

    registrar.Parameter("read_idle_timeout", &TThis::ReadIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("write_idle_timeout", &TThis::WriteIdleTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("cypress_registrar", &TThis::CypressRegistrar)
        .DefaultNew();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();

    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default();

    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();

    registrar.Parameter("auth", &TThis::Auth)
        .DefaultNew();

    registrar.Parameter("topic_name_transformations", &TThis::TopicNameTransformations)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (auto& dynamicConfigPath = config->DynamicConfigPath; dynamicConfigPath.empty()) {
            dynamicConfigPath = Format("%v/@config", KafkaProxiesRootPath);
        }

        // Some kafka connectors allows only the underscore, hyphen, dot and alphanumeric characters in topic names.
        if (config->TopicNameTransformations.empty()) {
            auto slashTransformation = New<TStringTransformationConfig>();
            slashTransformation->MatchPattern = New<NRe2::TRe2>("\\.");
            slashTransformation->Replacement = "/";

            auto colonTransformation = New<TStringTransformationConfig>();
            colonTransformation->MatchPattern = New<NRe2::TRe2>("-//");
            colonTransformation->Replacement = "://";
            config->TopicNameTransformations = {std::move(slashTransformation), std::move(colonTransformation)};
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TGroupCoordinatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rebalance_timeout", &TThis::RebalanceTimeout)
        .Default(TDuration::Seconds(2));

    registrar.Parameter("session_timeout", &TThis::SessionTimeout)
        .Default(TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaProxyDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
        .Default(2);
    registrar.Parameter("acceptor_thread_count", &TThis::AcceptorThreadCount)
        .Default(2);
    registrar.Parameter("local_host_name", &TThis::LocalHostName)
        .Default();
    registrar.Parameter("group_coordinator", &TThis::GroupCoordinator)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
