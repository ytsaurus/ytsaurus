#pragma once

#include "public.h"

#include <yt/yt/core/net/config.h>
#include <yt/yt/core/net/address.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingBandConfig
    : public NYTree::TYsonStruct
{
public:
    int TosLevel;
    THashMap<TString, int> NetworkToTosLevel;

    REGISTER_YSON_STRUCT(TMultiplexingBandConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingBandConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadPoolSize;

    //! Used for profiling export and alerts.
    std::optional<i64> NetworkBandwidth;

    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr> MultiplexingBands;

    TTcpDispatcherConfigPtr ApplyDynamic(const TTcpDispatcherDynamicConfigPtr& dynamicConfig) const;

    REGISTER_YSON_STRUCT(TTcpDispatcherConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<int> ThreadPoolSize;

    std::optional<i64> NetworkBandwidth;

    std::optional<THashMap<TString, std::vector<NNet::TIP6Network>>> Networks;

    std::optional<TEnumIndexedVector<EMultiplexingBand, TMultiplexingBandConfigPtr>> MultiplexingBands;

    REGISTER_YSON_STRUCT(TTcpDispatcherDynamicConfig);

    static void Register(TRegistrar registrar);
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

    REGISTER_YSON_STRUCT(TTcpBusConfig);

    static void Register(TRegistrar registrar);
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

    static TTcpBusServerConfigPtr CreateTcp(int port);
    static TTcpBusServerConfigPtr CreateUnixDomain(const TString& socketPath);

    REGISTER_YSON_STRUCT(TTcpBusServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpBusServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTcpBusClientConfig
    : public TTcpBusConfig
{
public:
    std::optional<TString> Address;
    std::optional<TString> UnixDomainSocketPath;

    static TTcpBusClientConfigPtr CreateTcp(const TString& address);
    static TTcpBusClientConfigPtr CreateUnixDomain(const TString& socketPath);

    REGISTER_YSON_STRUCT(TTcpBusClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

