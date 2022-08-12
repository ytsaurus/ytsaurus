#pragma once

#include "public.h"

#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingBandConfig
    : public NYTree::TYsonSerializable
{
public:
    int TosLevel;
    THashMap<TString, int> NetworkToTosLevel;

    TMultiplexingBandConfig();
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingBandConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherConfig
    : public NYTree::TYsonSerializable
{
public:
    int ThreadPoolSize;

    //! Used for profiling export and alerts.
    std::optional<i64> NetworkBandwidth;

    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr> MultiplexingBands;

    TTcpDispatcherConfig();
    TTcpDispatcherConfigPtr ApplyDynamic(const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const;
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    std::optional<int> ThreadPoolSize;

    std::optional<i64> NetworkBandwidth;

    std::optional<THashMap<TString, std::vector<NNet::TIP6Network>>> Networks;

    std::optional<TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr>> MultiplexingBands;

    TTcpDispatcherDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherDynamicConfig)

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
    std::optional<TString> UnixDomainSocketPath;

    TTcpBusClientConfig();

    static TTcpBusClientConfigPtr CreateTcp(const TString& address);
    static TTcpBusClientConfigPtr CreateUnixDomain(const TString& socketPath);
};

DEFINE_REFCOUNTED_TYPE(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

