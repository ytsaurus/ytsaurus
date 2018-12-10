#pragma once

#include "public.h"

#include <yt/client/api/config.h>

#include <yt/client/api/rpc_proxy/config.h>

#include <yt/server/misc/config.h>

#include <yt/core/http/config.h>

#include <yt/core/concurrency/config.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TAnnouncerConfig
    : public NYTree::TYsonSerializable
{
public:
    std::vector<TString> Trackers;
    int PeerUdpPort;
    TDuration OutOfOrderUpdateTtl;

    TAnnouncerConfig()
    {
        RegisterParameter("trackers", Trackers);
        RegisterParameter("peer_udp_port", PeerUdpPort)
            .Default(7001);
        RegisterParameter("out_of_order_update_ttl", OutOfOrderUpdateTtl)
            .Default(TDuration::Minutes(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TAnnouncerConfig)

////////////////////////////////////////////////////////////////////////////////

class TClusterConnectionConfig
    : public NYTree::TYsonSerializable
{
public:
    TString ClusterName;
    TString User;
    TString OAuthTokenEnv;

    TString OAuthToken;

    NYPath::TYPath Root;

    NApi::NRpcProxy::TConnectionConfigPtr Connection;

    NConcurrency::TThroughputThrottlerConfigPtr UserRequestThrottler;
    NConcurrency::TThroughputThrottlerConfigPtr BackgroundThrottler;
    
    void LoadToken();

    TClusterConnectionConfig()
    {
        RegisterParameter("cluster_name", ClusterName);
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

        RegisterPostprocessor([this] () {
            if (!Connection->ClusterUrl) {
                THROW_ERROR_EXCEPTION("\"cluster_url\" is required");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TClusterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TSkynetManagerConfig
    : public TServerConfig
{
public:
    int IOPoolSize;

    int Port;
    int SkynetPort;

    TString PeerIdFile;

    TAsyncExpiringCacheConfigPtr Cache;

    std::vector<TClusterConnectionConfigPtr> Clusters;

    NHttp::TServerConfigPtr HttpServer;
    NHttp::TClientConfigPtr HttpClient;

    TAnnouncerConfigPtr Announcer;

    int TableShardLimit;
    i64 ResourceSizeLimit;

    TDuration SyncIterationInterval;
    TDuration RemovedTablesScanInterval;

    TSkynetManagerConfig()
    {
        RegisterParameter("io_pool_size", IOPoolSize)
            .Default(4);

        RegisterParameter("port", Port);
        RegisterParameter("skynet_port", SkynetPort)
            .Default(7000);

        RegisterParameter("cache", Cache)
            .DefaultNew();
        RegisterParameter("peer_id_file", PeerIdFile)
            .Default("peer_id");

        RegisterParameter("clusters", Clusters);

        RegisterParameter("http_server", HttpServer)
            .DefaultNew();
        RegisterParameter("http_client", HttpClient)
            .DefaultNew();

        RegisterParameter("announcer", Announcer)
            .DefaultNew();

        RegisterParameter("table_shard_limit", TableShardLimit)
            .Default(65536);
        RegisterParameter("resource_size_limit", ResourceSizeLimit)
            .Default(1_TB);

        RegisterParameter("sync_iteration_interval", SyncIterationInterval)
            .Default(TDuration::Seconds(60));

        RegisterParameter("removed_tables_scan_interval", RemovedTablesScanInterval)
            .Default(TDuration::Seconds(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TSkynetManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager
