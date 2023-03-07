#include "config.h"

namespace NYT::NBus {

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
    RegisterParameter("networks", Networks)
        .Default({});
    RegisterParameter("default_network", DefaultNetwork)
        .Default(DefaultNetworkName);

    RegisterPreprocessor([&] {
        if (DefaultNetwork && !Networks.contains(*DefaultNetwork) && *DefaultNetwork != DefaultNetworkName) {
            THROW_ERROR_EXCEPTION("Default network %Qv is not present in network list", DefaultNetwork);
        }
    });
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
        .Default(1);
    RegisterParameter("bind_retry_backoff", BindRetryBackoff)
        .Default(TDuration::Seconds(3));
    RegisterParameter("read_stall_timeout", ReadStallTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("write_stall_timeout", WriteStallTimeout)
        .Default(TDuration::Minutes(1));
    RegisterParameter("verify_checksums", VerifyChecksums)
        .Default(true);
    RegisterParameter("generate_checksum", GenerateChecksums)
        .Default(true);
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateTcp(const TString& address)
{
    auto config = New<TTcpBusClientConfig>();
    config->Address = address;
    return config;
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateTcp(const TString& address, const TString& network)
{
    auto config = New<TTcpBusClientConfig>();
    config->Address = address;
    config->NetworkName = network;
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
