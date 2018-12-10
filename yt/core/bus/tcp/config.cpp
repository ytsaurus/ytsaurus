#include "config.h"

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

TTcpBusServerConfig::TTcpBusServerConfig()
{
    RegisterParameter("port", Port)
        .Default();
    RegisterParameter("unix_domain_name", UnixDomainName)
        .Default();
    RegisterParameter("max_backlog_size", MaxBacklogSize)
        .Default(8192);
    RegisterParameter("max_simultaneous_connections", MaxSimultaneousConnections)
        .Default(50000);
    RegisterParameter("networks", Networks)
        .Default({});
    RegisterParameter("default_network", DefaultNetwork)
        .Default();

    RegisterPreprocessor([&] {
        if (DefaultNetwork && !Networks.contains(*DefaultNetwork)) {
            THROW_ERROR_EXCEPTION("Default network is not present in network list");
        }
    });
}

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateTcp(int port)
{
    auto config = New<TTcpBusServerConfig>();
    config->Port = port;
    return config;
}

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateUnixDomain(const TString& address)
{
    auto config = New<TTcpBusServerConfig>();
    config->UnixDomainName = address;
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
        .Default(TDuration::Minutes(2));
    RegisterParameter("write_stall_timeout", WriteStallTimeout)
        .Default(TDuration::Minutes(2));
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

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateUnixDomain(const TString& address)
{
    auto config = New<TTcpBusClientConfig>();
    config->UnixDomainName = address;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
