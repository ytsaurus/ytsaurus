#pragma once

#include "public.h"

#include <yt/core/net/config.h>
#include <yt/core/net/address.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TTcpBusConfig
    : public NNet::TDialerConfig
{
public:
    bool EnableQuickAck;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    TDuration ReadStallTimeout;
    TDuration WriteStallTimeout;

    bool VerifyChecksums;
    bool GenerateChecksums;

    TTcpBusConfig()
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
};

DEFINE_REFCOUNTED_TYPE(TTcpBusConfig)

class TTcpBusServerConfig
    : public TTcpBusConfig
{
public:
    TNullable<int> Port;
    TNullable<TString> UnixDomainName;
    int MaxBacklogSize;
    int MaxSimultaneousConnections;
    //! "Default" network is considered when checking if the network is under heavy load.
    TNullable<TString> DefaultNetwork;

    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    TTcpBusServerConfig()
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
            if (DefaultNetwork && !Networks.has(*DefaultNetwork)) {
                THROW_ERROR_EXCEPTION("Default network is not present in network list");
            }
        });
    }

    static TTcpBusServerConfigPtr CreateTcp(int port);

    static TTcpBusServerConfigPtr CreateUnixDomain(const TString& address);

};

DEFINE_REFCOUNTED_TYPE(TTcpBusServerConfig)

class TTcpBusClientConfig
    : public TTcpBusConfig
{
public:
    TNullable<TString> Address;
    TNullable<TString> UnixDomainName;

    TTcpBusClientConfig()
    {
        RegisterParameter("address", Address)
            .Default();
        RegisterParameter("unix_domain_name", UnixDomainName)
            .Default();

        RegisterPostprocessor([&] () {
            if (!Address && !UnixDomainName) {
                THROW_ERROR_EXCEPTION("\"address\" and \"unix_domain_name\" cannot be both missing");
            }
        });
    }

    static TTcpBusClientConfigPtr CreateTcp(const TString& address);

    static TTcpBusClientConfigPtr CreateUnixDomain(const TString& address);

};

DEFINE_REFCOUNTED_TYPE(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

