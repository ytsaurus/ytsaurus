#pragma once

#include "private.h"

#include <yt/server/http_proxy/clickhouse/public.h>

#include <yt/server/lib/misc/config.h>

#include <yt/ytlib/auth/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/driver/public.h>

#include <yt/client/formats/public.h>

#include <yt/client/api/public.h>

#include <yt/core/https/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    bool Announce;

    std::optional<TString> PublicFqdn;
    std::optional<TString> DefaultRoleFilter;

    TDuration HeartbeatInterval;
    TDuration DeathAge;

    bool ShowPorts;

    double LoadAverageWeight;
    double NetworkLoadWeight;
    double RandomnessWeight;
    double DampeningWeight;

    TCoordinatorConfig();
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    class TDelayBeforeCommand
        : public NYTree::TYsonSerializable
    {
    public:
        TDuration Delay;
        TString ParameterPath;
        TString Substring;

        TDelayBeforeCommand();
    };

public:
    THashMap<TString, TIntrusivePtr<TDelayBeforeCommand>> DelayBeforeCommand;

    TApiTestingOptions();
};

DEFINE_REFCOUNTED_TYPE(TApiTestingOptions);
DEFINE_REFCOUNTED_TYPE(TApiTestingOptions::TDelayBeforeCommand);

////////////////////////////////////////////////////////////////////////////////

class TFramingConfig
    : public NYTree::TYsonSerializable
{
public:
    bool Enable;
    std::optional<TDuration> KeepAlivePeriod;

    TFramingConfig();
};

DEFINE_REFCOUNTED_TYPE(TFramingConfig)

////////////////////////////////////////////////////////////////////////////////

class TApiConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration BanCacheExpirationTime;
    int ConcurrencyLimit;

    bool DisableCorsCheck;

    bool ForceTracing;

    TApiTestingOptionsPtr TestingOptions;

    TApiConfig();
};

DEFINE_REFCOUNTED_TYPE(TApiConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyConfig
    : public TServerConfig
{
public:
    int Port;
    int ThreadCount;

    NHttp::TServerConfigPtr HttpServer;
    NHttps::TServerConfigPtr HttpsServer;
    NYTree::INodePtr Driver;

    NAuth::TAuthenticationManagerConfigPtr Auth;

    bool RetryRequestQueueSizeLimitExceeded;

    TCoordinatorConfigPtr Coordinator;
    TApiConfigPtr Api;

    NClickHouse::TStaticClickHouseConfigPtr ClickHouse;

    TString UIRedirectUrl;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    TString DefaultNetwork;
    THashMap<TString, std::vector<NNet::TIP6Network>> Networks;

    TProxyConfig();
};

DEFINE_REFCOUNTED_TYPE(TProxyConfig)

////////////////////////////////////////////////////////////////////////////////

// TDynamicConfig is part of proxy configuration stored in cypress.
//
// NOTE: config might me unavalable. Users must handle such cases
// gracefully.
class TDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    NTracing::TSamplingConfigPtr Tracing;

    TString FitnessFunction;
    double CpuWeight;
    double CpuWaitWeight;
    double ConcurrentRequestsWeight;

    bool RelaxCsrfCheck;

    NClickHouse::TDynamicClickHouseConfigPtr ClickHouse;

    TFramingConfigPtr Framing;

    THashMap<NFormats::EFormatType, TFormatConfigPtr> Formats;

    TDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
