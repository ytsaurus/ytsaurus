#pragma once

#include "public.h"

#include <yt/core/net/config.h>
#include <yt/core/net/address.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NBus {

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

    TTcpBusConfig();
};

DEFINE_REFCOUNTED_TYPE(TTcpBusConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpBusServerConfig
    : public TTcpBusConfig
{
public:
    std::optional<int> Port;
    std::optional<TString> UnixDomainSocketPath;
    int MaxBacklogSize;
    int MaxSimultaneousConnections;
    //! "Default" network is considered when checking if the network is under heavy load.
    std::optional<TString> DefaultNetwork;

    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    TTcpBusServerConfig();

    static TTcpBusServerConfigPtr CreateTcp(int port);
    static TTcpBusServerConfigPtr CreateUnixDomain(const TString& socketPath);
};

DEFINE_REFCOUNTED_TYPE(TTcpBusServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClientConfig
    : public TTcpBusConfig
{
public:
    std::optional<TString> Address;
    std::optional<TString> NetworkName;
    std::optional<TString> UnixDomainSocketPath;

    TTcpBusClientConfig()
    {
        RegisterParameter("address", Address)
            .Default();
        RegisterParameter("network_name", NetworkName)
            .Default();
        RegisterParameter("unix_domain_socket_path", UnixDomainSocketPath)
            .Default();

        RegisterPostprocessor([&] () {
            if (!Address && !UnixDomainSocketPath) {
                THROW_ERROR_EXCEPTION("\"address\" and \"unix_domain_socket_path\" cannot be both missing");
            }
        });
    }

    static TTcpBusClientConfigPtr CreateTcp(const TString& address);
    static TTcpBusClientConfigPtr CreateTcp(const TString& address, const TString& network);
    static TTcpBusClientConfigPtr CreateUnixDomain(const TString& socketPath);
};

DEFINE_REFCOUNTED_TYPE(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

