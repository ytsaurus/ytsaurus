#include "config.h"

#include <yt/yt/core/net/address.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

void TMultiplexingBandConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("tos_level", &TThis::TosLevel)
        .Default(NYT::NBus::DefaultTosLevel);

    registrar.Parameter("network_to_tos_level", &TThis::NetworkToTosLevel)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TTcpDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Default(8);

    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Default();

    registrar.Parameter("networks", &TThis::Networks)
        .Default();

    registrar.Parameter("multiplexing_bands", &TThis::MultiplexingBands)
        .Default();
}

TTcpDispatcherConfigPtr TTcpDispatcherConfig::ApplyDynamic(
    const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TTcpDispatcherConfig>();
    mergedConfig->ThreadPoolSize = dynamicConfig->ThreadPoolSize.value_or(ThreadPoolSize);
    mergedConfig->Networks = dynamicConfig->Networks.value_or(Networks);
    mergedConfig->MultiplexingBands = dynamicConfig->MultiplexingBands.value_or(MultiplexingBands);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TTcpDispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_size", &TThis::ThreadPoolSize)
        .Optional()
        .GreaterThan(0);

    registrar.Parameter("network_bandwidth", &TThis::NetworkBandwidth)
        .Default();

    registrar.Parameter("networks", &TThis::Networks)
        .Default();

    registrar.Parameter("multiplexing_bands", &TThis::MultiplexingBands)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TTcpBusServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("port", &TThis::Port)
        .Default();
    registrar.Parameter("unix_domain_socket_path", &TThis::UnixDomainSocketPath)
        .Default();
    registrar.Parameter("max_backlog_size", &TThis::MaxBacklogSize)
        .Default(8192);
    registrar.Parameter("max_simultaneous_connections", &TThis::MaxSimultaneousConnections)
        .Default(50000);
}

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateTcp(int port)
{
    auto config = New<TTcpBusServerConfig>();
    config->Port = port;
    return config;
}

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateUnixDomain(const TString& socketPath)
{
    auto config = New<TTcpBusServerConfig>();
    config->UnixDomainSocketPath = socketPath;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TTcpBusConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_quick_ack", &TThis::EnableQuickAck)
        .Default(true);
    registrar.Parameter("bind_retry_count", &TThis::BindRetryCount)
        .Default(5);
    registrar.Parameter("bind_retry_backoff", &TThis::BindRetryBackoff)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("read_stall_timeout", &TThis::ReadStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("write_stall_timeout", &TThis::WriteStallTimeout)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("verify_checksums", &TThis::VerifyChecksums)
        .Default(true);
    registrar.Parameter("generate_checksums", &TThis::GenerateChecksums)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TTcpBusClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("unix_domain_socket_path", &TThis::UnixDomainSocketPath)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (!config->Address && !config->UnixDomainSocketPath) {
            THROW_ERROR_EXCEPTION("\"address\" and \"unix_domain_socket_path\" cannot be both missing");
        }
    });
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateTcp(const TString& address)
{
    auto config = New<TTcpBusClientConfig>();
    config->Address = address;
    return config;
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateUnixDomain(const TString& socketPath)
{
    auto config = New<TTcpBusClientConfig>();
    config->UnixDomainSocketPath = socketPath;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
