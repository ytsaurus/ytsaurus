#include "config.h"

#include "helpers.h"

#include <yt/yt/orm/client/objects/helpers.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/event_log/config.h>

#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <util/system/env.h>

#include <contrib/libs/grpc/include/grpc/impl/grpc_types.h>

namespace NYT::NOrm::NServer::NMaster {

using NClient::NObjects::IsValidInstanceTag;
using NClient::NObjects::ValidInstanceTagRange;

////////////////////////////////////////////////////////////////////////////////

void TYTConnectorConfig::DoRegister(TRegistrar registrar, bool authenticationRequired)
{
    registrar.Parameter("connection", &TThis::Connection)
        .Optional();
    registrar.Parameter("rpc_proxy_connection", &TThis::RpcProxyConnection)
        .Optional();
    registrar.Parameter("federated_connection", &TThis::FederatedConnection)
        .Optional();
    registrar.Parameter("connection_cluster_url", &TThis::ConnectionClusterUrl)
        .Optional();
    registrar.Parameter("user", &TThis::User);
    registrar.Parameter("token", &TThis::Token)
        .Optional();
    registrar.Parameter("token_env", &TThis::TokenEnv)
        .Default("YT_TOKEN");
    registrar.Parameter("use_service_ticket_auth", &TThis::UseServiceTicketAuth)
        .Default(false);
    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();
    registrar.Parameter("root_path", &TThis::RootPath);
    registrar.Parameter("consumer_dir", &TThis::ConsumerDir)
        .Default("consumers");
    registrar.Parameter("cluster_tag", &TThis::ClusterTag);
    registrar.Parameter("cluster_name", &TThis::ClusterName)
        .Optional();
    registrar.Parameter("instance_tag", &TThis::InstanceTag)
        .Default(UndefinedMasterInstanceTag);
    registrar.Parameter("instance_transaction_timeout", &TThis::InstanceTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("leader_transaction_timeout", &TThis::LeaderTransactionTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("node_expiration_timeout", &TThis::NodeExpirationTimeout)
        .Default(TDuration::Days(1));
    registrar.Parameter("reconnect_period", &TThis::ReconnectPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("master_discovery_period", &TThis::MasterDiscoveryPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("setup_orchid", &TThis::SetupOrchid)
        .Default(true);
    registrar.Parameter("ban_check_period", &TThis::BanCheckPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("validations_concurrency_limit", &TThis::ValidationsConcurrencyLimit)
        .Default(4)
        .GreaterThanOrEqual(1);
    registrar.Postprocessor([authenticationRequired] (TThis* config) {
        config->ClientCache->Capacity = 1'000;

        if (config->RpcProxyConnection && config->Connection) {
            THROW_ERROR_EXCEPTION("\"connection\" and \"rpc_proxy_connection\" cannot be both specified");
        }
        if (config->RpcProxyConnection && !config->ConnectionClusterUrl.empty()) {
            THROW_ERROR_EXCEPTION("\"connection_cluster_url\" may be specified only with \"connection\"");
        }
        if (!config->RpcProxyConnection && !config->Connection && config->ConnectionClusterUrl.empty()) {
            THROW_ERROR_EXCEPTION("One of \"connection_cluster_url\", \"connection\" or \"rpc_proxy_connection\" "
                "must be specified");
        }
        if (authenticationRequired && !config->RpcProxyConnection && config->ConnectionClusterUrl.empty()) {
            THROW_ERROR_EXCEPTION("\"connection_cluster_url\" must be set for authentication of native connection");
        }

        if (config->InstanceTag != UndefinedMasterInstanceTag && !IsValidInstanceTag(config->InstanceTag))
        {
            THROW_ERROR_EXCEPTION(
                "Instance tag must be undefined or in range %v, but got %v",
                ValidInstanceTagRange(),
                config->InstanceTag);
        }
        if (!config->TokenEnv.empty()) {
            config->TokenEnvCachedValue_ = GetEnv(TString(config->TokenEnv));
        }
    });
}

const TString& TYTConnectorConfig::GetToken() const
{
    return Token ? *Token : TokenEnvCachedValue_;
}

////////////////////////////////////////////////////////////////////////////////

void TRpcProxyCollocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("api_service", &TThis::ApiService)
        .DefaultNew();
    registrar.Parameter("api_service_dynamic", &TThis::ApiServiceDynamic)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TEventLogManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("pending_rows_flush_period", &TThis::PendingRowsFlushPeriod)
        .Default(TDuration::Seconds(1));
}

NEventLog::TEventLogManagerConfigPtr TEventLogManagerConfig::AsYTEventLogManagerConfig()
{
    auto configNode = NYT::NYTree::ConvertToNode(*this);
    auto ytEventLogManagerConfig = New<NEventLog::TEventLogManagerConfig>();
    ytEventLogManagerConfig->SetUnrecognizedStrategy(NYTree::EUnrecognizedStrategy::Drop);
    ytEventLogManagerConfig->Load(configNode);
    return ytEventLogManagerConfig;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void SetGrpcDefaults(THashMap<TString, NYTree::INodePtr>& grpcArguments)
{
    grpcArguments.try_emplace(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, NYTree::ConvertToNode(0));
    grpcArguments.try_emplace(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, NYTree::ConvertToNode(10'000));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, NYTree::ConvertToNode(1));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_TIME_MS, NYTree::ConvertToNode(60'000));
    grpcArguments.try_emplace(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, NYTree::ConvertToNode(15'000));
    grpcArguments.try_emplace(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, NYTree::ConvertToNode(128_MB));
    grpcArguments.try_emplace(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, NYTree::ConvertToNode(128_MB));
    grpcArguments.try_emplace(GRPC_ARG_MAX_METADATA_SIZE, NYTree::ConvertToNode(16_KB));
}

} // namespace

void TMasterConfigBase::DoRegister(TRegistrar registrar)
{
    registrar.Parameter("monitoring_server", &TThis::MonitoringServer)
        .Optional();
    registrar.Parameter("internal_bus_server", &TThis::InternalBusServer)
        .Optional();
    registrar.Parameter("internal_rpc_server", &TThis::InternalRpcServer)
        .Optional();
    registrar.Parameter("client_grpc_server", &TThis::ClientGrpcServer)
        .Optional();
    registrar.Parameter("secure_client_grpc_server", &TThis::SecureClientGrpcServer)
        .Optional();
    registrar.Parameter("secure_client_http_server", &TThis::SecureClientHttpServer)
        .Optional();
    registrar.Parameter("client_http_server", &TThis::ClientHttpServer)
        .Optional();
    registrar.Parameter("fqdn_override", &TThis::FqdnOverride)
        .Alias("fqdn")
        .Optional()
        .CheckThat([] (const auto& fqdnOverride) {
            THROW_ERROR_EXCEPTION_IF(
                fqdnOverride.has_value() && fqdnOverride->Empty(),
                "FQDN override cannot be empty");
        });
    registrar.Parameter("ip6_address_override", &TThis::IP6AddressOverride)
        .Optional()
        .CheckThat([] (const auto& ip6AddressOverride) {
            if (!ip6AddressOverride.has_value()) {
                return;
            }
            THROW_ERROR_EXCEPTION_IF(
                ip6AddressOverride->Empty(),
                "IP6 address override cannot be empty");

            NNet::TIP6Address ip6Address;
            THROW_ERROR_EXCEPTION_UNLESS(
                NNet::TIP6Address::FromString(*ip6AddressOverride, &ip6Address),
                "IP6 address override must be a valid IP6 address, got %Qv",
                *ip6AddressOverride);
        });
    registrar.Parameter("transaction_manager", &TThis::TransactionManager)
        .DefaultNew();
    registrar.Parameter("watch_manager", &TThis::WatchManager)
        .DefaultNew();
    registrar.Parameter("pool_weight_manager", &TThis::PoolWeightManager)
        .DefaultNew();
    registrar.Parameter("tvm_service", &TThis::TvmService)
        .Optional();
    registrar.Parameter("authentication_manager", &TThis::AuthenticationManager)
        .DefaultNew();
    registrar.Parameter("object_service", &TThis::ObjectService)
        .DefaultNew();
    registrar.Parameter("db_name", &TThis::DBName)
        .Optional();
    registrar.Parameter("worker_thread_pool_size", &TThis::WorkerThreadPoolSize)
        .GreaterThan(0)
        .Default(8);
    registrar.Parameter("http_poller_thread_pool_size", &TThis::HttpPollerThreadPoolSize)
        .GreaterThan(0)
        .Default(4);
    registrar.Parameter("enable_ip6_address_resolving", &TThis::EnableIP6AddressResolving)
        .Default(true);
    registrar.Parameter("enable_ip6_address_validation", &TThis::EnableIP6AddressValidation)
        .Default(true);
    registrar.Parameter("ip6_address_validation_period", &TThis::IP6AddressValidationPeriod)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("rpc_proxy_collocation", &TThis::RpcProxyCollocation)
        .Default();
    registrar.Parameter("tracing_sampler", &TThis::TracingSampler)
        .DefaultNew();
    registrar.Parameter("log_count_limit_for_plural_requests", &TThis::LogCountLimitForPluralRequests)
        .Default(20);
    registrar.Parameter("event_log_manager", &TThis::EventLogManager)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        // TODO(dgolear): Migrate configs.
        if (!config->TvmService && config->AuthenticationManager->TvmService) {
            config->TvmService = config->AuthenticationManager->TvmService;
        }
        if (config->InternalBusServer && !config->InternalBusServer->Port) {
            THROW_ERROR_EXCEPTION("Missing /internal_bus_server/port");
        }
        if (config->ClientGrpcServer && config->ClientGrpcServer->Addresses.size() != 1) {
            THROW_ERROR_EXCEPTION("Exactly one GRPC API server address must be given in \"client_grpc_server\"");
        }
        if (config->SecureClientGrpcServer && config->SecureClientGrpcServer->Addresses.size() != 1) {
            THROW_ERROR_EXCEPTION("Exactly one GRPC API server address must be given \"secure_client_grpc_server\"");
        }
        if (config->RpcProxyCollocation) {
            if (!config->InternalBusServer) {
                THROW_ERROR_EXCEPTION("RPC proxy collocation requires /internal_bus_server");
            }
        }

        if (config->ClientGrpcServer) {
            SetGrpcDefaults(config->ClientGrpcServer->GrpcArguments);
        }
        if (config->SecureClientGrpcServer) {
            SetGrpcDefaults(config->SecureClientGrpcServer->GrpcArguments);
        }

        if (config->ClientHttpServer) {
            InitIfNoValue(config->ClientHttpServer->CancelFiberOnConnectionClose, true);
        }
        if (config->SecureClientHttpServer) {
            InitIfNoValue(config->SecureClientHttpServer->CancelFiberOnConnectionClose, true);
        }

        ValidateDbName(config->DBName);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConfig::DoRegister(TRegistrar registrar)
{
    TMasterConfigBase::DoRegister(registrar);

    registrar.Parameter("event_log_path", &TThis::EventLogPath)
        .Default();

    registrar.Parameter("enable_porto_resource_tracker", &TThis::EnablePortoResourceTracker)
        .Default(false);
    registrar.Parameter("pod_spec", &TThis::PodSpec)
        .DefaultNew();
}

} // namespace NYT::NOrm::NServer::NMaster
