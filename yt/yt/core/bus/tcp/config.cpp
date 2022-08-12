#include "config.h"

#include <yt/yt/core/net/address.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

TMultiplexingBandConfig::TMultiplexingBandConfig()
{
    RegisterParameter("tos_level", TosLevel)
        .Default(NYT::NBus::DefaultTosLevel);

    RegisterParameter("network_to_tos_level", NetworkToTosLevel)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcherConfig::TTcpDispatcherConfig()
{
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .Default(8);

    RegisterParameter("network_bandwidth", NetworkBandwidth)
        .Default();

    RegisterParameter("networks", Networks)
        .Default();

    RegisterParameter("multiplexing_bands", MultiplexingBands)
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

TTcpDispatcherDynamicConfig::TTcpDispatcherDynamicConfig()
{
    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .Optional()
        .GreaterThan(0);

    RegisterParameter("network_bandwidth", NetworkBandwidth)
        .Default();

    RegisterParameter("networks", Networks)
        .Default();

    RegisterParameter("multiplexing_bands", MultiplexingBands)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TTcpBusServerConfig::TTcpBusServerConfig()
{
    RegisterParameter("port", Port)
        .Default();
    RegisterParameter("unix_domain_socket_path", UnixDomainSocketPath)
        .Default();
    RegisterParameter("max_backlog_size", MaxBacklogSize)
        .Default(8192);
    RegisterParameter("max_simultaneous_connections", MaxSimultaneousConnections)
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

TTcpBusConfig::TTcpBusConfig()
{
    RegisterParameter("enable_quick_ack", EnableQuickAck)
        .Default(true);
    RegisterParameter("bind_retry_count", BindRetryCount)
        .Default(5);
    RegisterParameter("bind_retry_backoff", BindRetryBackoff)
        .Default(TDuration::Seconds(3));
    RegisterParameter("read_stall_timeout", ReadStallTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("write_stall_timeout", WriteStallTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("verify_checksums", VerifyChecksums)
        .Default(true);
    RegisterParameter("generate_checksums", GenerateChecksums)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TTcpBusClientConfig::TTcpBusClientConfig()
{
    RegisterParameter("address", Address)
        .Default();
    RegisterParameter("unix_domain_socket_path", UnixDomainSocketPath)
        .Default();

    RegisterPostprocessor([&] () {
        if (!Address && !UnixDomainSocketPath) {
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
