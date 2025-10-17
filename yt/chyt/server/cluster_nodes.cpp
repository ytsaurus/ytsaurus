#include "cluster_nodes.h"

#include <Common/Exception.h>
#include <Core/Settings.h>

namespace DB::Setting {

////////////////////////////////////////////////////////////////////////////////

extern const SettingsUInt64 connections_with_failover_max_tries;
extern const SettingsUInt64 distributed_connections_pool_size;
extern const SettingsSeconds connect_timeout;
extern const SettingsSeconds max_execution_time;
extern const SettingsSeconds receive_timeout;
extern const SettingsSeconds send_timeout;
extern const SettingsLoadBalancing load_balancing;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::Setting

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TClusterNode
    : public IClusterNode
{
private:
    TClusterNodeName Name;
    int Cookie;
    bool Local;
    DB::ConnectionPoolWithFailoverPtr Connection;

public:
    TClusterNode(
        const TClusterNodeName name,
        int cookie,
        bool local,
        DB::ConnectionPoolWithFailoverPtr connection)
        : Name(name)
        , Cookie(cookie)
        , Local(local)
        , Connection(std::move(connection))
    {
    }

    const TClusterNodeName& GetName() const override
    {
        return Name;
    }

    int GetCookie() const override
    {
        return Cookie;
    }

    bool IsLocal() const override
    {
        return Local;
    }

    DB::ConnectionPoolWithFailoverPtr GetConnection() override
    {
        return Connection;
    }
};

////////////////////////////////////////////////////////////////////////////////

IClusterNodePtr CreateClusterNode(
    TClusterNodeName name,
    int cookie,
    const DB::Settings& settings,
    bool isLocal)
{
    if (!name.Host.empty() && name.Host.front() == '[' && name.Host.back() == ']') {
        name.Host = name.Host.substr(1, name.Host.size() - 2);
    }

    ConnectionPoolPtrs pools;
    auto timeouts = ConnectionTimeouts()
        .withConnectionTimeout(Cluster::saturate(settings[Setting::connect_timeout], settings[Setting::max_execution_time]))
        .withReceiveTimeout(Cluster::saturate(settings[Setting::receive_timeout], settings[Setting::max_execution_time]))
        .withSendTimeout(Cluster::saturate(settings[Setting::send_timeout], settings[Setting::max_execution_time]));

    pools.push_back(std::make_shared<ConnectionPool>(
        settings[Setting::distributed_connections_pool_size],
        name.Host,
        name.Port,
        "" /*defaultDatabase*/,
        std::string(InternalRemoteUserName.data()) /*user*/,
        "" /*password*/,
        "notchunked" /*proto_send_chunked*/,
        "notchunked" /*proto_recv_chunked*/,
        "" /*quota_key*/,
        "" /*cluster*/,
        "" /*cluster_secret*/,
        "server" /*client_name*/,
        Protocol::Compression::Enable,
        Protocol::Secure::Disable));

    auto connection = std::make_shared<ConnectionPoolWithFailover>(
        std::move(pools),
        settings[Setting::load_balancing],
        settings[Setting::connections_with_failover_max_tries]);

    return std::make_shared<TClusterNode>(
        name,
        cookie,
        isLocal,
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
