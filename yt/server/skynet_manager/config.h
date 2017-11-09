#pragma once

#include "public.h"

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/rpc_proxy/config.h>

#include <yt/server/misc/config.h>

#include <yt/core/http/config.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TClusterConnectionConfig
    : public NYTree::TYsonSerializable
{
public:
    TString Name;
    TString ProxyUrl;
    TString OAuthTokenEnv;

    TString OAuthToken;

    void LoadToken()
    {
        auto token = getenv(OAuthTokenEnv.c_str());
        if (!token) {
            THROW_ERROR_EXCEPTION("%v environment variable is not set", OAuthTokenEnv);
        }
        OAuthToken = token;
    }

    TClusterConnectionConfig()
    {
        RegisterParameter("name", Name);
        RegisterParameter("proxy_url", ProxyUrl);
        RegisterParameter("oauth_token_env", OAuthTokenEnv)
            .Default("YT_TOKEN");
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterConnectionConfig)

class TSkynetManagerConfig
    : public TServerConfig
{
public:
    //! Passed to skynet daemon, so it can call us back.
    TString SelfUrl;

    TString SkynetPythonInterpreterPath;
    TString SkynetMdsToolPath;

    int IOPoolSize;

    int Port;

    std::vector<TClusterConnectionConfigPtr> Clusters;

    NHttp::TServerConfigPtr HttpServer;
    NHttp::TClientConfigPtr HttpClient;
    
    TSkynetManagerConfig()
    {
        RegisterParameter("self_url", SelfUrl);

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
    }
};

DEFINE_REFCOUNTED_TYPE(TSkynetManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
