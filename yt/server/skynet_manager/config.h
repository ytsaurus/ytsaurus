#pragma once

#include "public.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/rpc_proxy/config.h>

#include <yt/server/misc/config.h>

#include <yt/core/http/config.h>

#include <yt/core/concurrency/config.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TClusterConnectionConfig
    : public NYTree::TYsonSerializable
{
public:
    TString ClusterName;
    TString ProxyUrl;
    TString User;
    TString OAuthTokenEnv;

    TString OAuthToken;

    NYPath::TYPath Root;

    NYTree::INodePtr Connection;

    NConcurrency::TThroughputThrottlerConfigPtr UserRequestThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr BackgroundThrottler;
    
    void LoadToken()
    {
        if (OAuthTokenEnv.empty()) {
            return;
        }

        auto token = getenv(OAuthTokenEnv.c_str());
        if (!token) {
            THROW_ERROR_EXCEPTION("%v environment variable is not set", OAuthTokenEnv);
        }
        OAuthToken = token;
    }

    TClusterConnectionConfig()
    {
        RegisterParameter("cluster_name", ClusterName);
        RegisterParameter("proxy_url", ProxyUrl);
        RegisterParameter("user", User)
            .Default("robot-yt-skynet-m5r");
        RegisterParameter("oauth_token_env", OAuthTokenEnv)
            .Default("YT_TOKEN");

        RegisterParameter("root", Root)
            .Default("//home/yt-skynet-m5r");

        RegisterParameter("connection", Connection);

        RegisterParameter("user_request_throttler", UserRequestThrottler)
            .DefaultNew();

        RegisterParameter("background_throttler", BackgroundThrottler)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TTombstoneCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration Ttl;
    int MaxSize;

    TTombstoneCacheConfig()
    {
        RegisterParameter("ttl", Ttl)
            .Default(TDuration::Seconds(30));

        RegisterParameter("max_size", MaxSize)
            .Default(1000 * 1000);
    }
};

DEFINE_REFCOUNTED_TYPE(TTombstoneCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TSkynetManagerConfig
    : public TServerConfig
{
public:
    //! Passed to skynet daemon, so it can call us back.
    TString SelfUrl;

    bool EnableSkynetMds;
    TString SkynetPythonInterpreterPath;
    TString SkynetMdsToolPath;

    int IOPoolSize;

    int Port;

    std::vector<TClusterConnectionConfigPtr> Clusters;

    NHttp::TServerConfigPtr HttpServer;
    NHttp::TClientConfigPtr HttpClient;

    TTombstoneCacheConfigPtr TombstoneCache;
    
    TSkynetManagerConfig()
    {
        RegisterParameter("self_url", SelfUrl);

        RegisterParameter("enable_skynet_mds", EnableSkynetMds)
            .Default(true);
        RegisterParameter("skynet_mds_tool_path", SkynetMdsToolPath)
            .Default("/skynet/tools/skybone-mds-ctl");
        RegisterParameter("skynet_python_interpreter_path", SkynetPythonInterpreterPath)
            .Default("/skynet/python/bin/python");

        RegisterParameter("io_pool_size", IOPoolSize)
            .Default(4);

        RegisterParameter("port", Port);

        RegisterParameter("clusters", Clusters);

        RegisterParameter("http_server", HttpServer)
            .DefaultNew();
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();

        RegisterParameter("tombstone_cache", TombstoneCache)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSkynetManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
