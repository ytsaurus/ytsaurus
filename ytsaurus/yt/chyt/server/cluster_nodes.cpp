#include "cluster_nodes.h"

#include <Common/Exception.h>

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
    ConnectionTimeouts timeouts(Cluster::saturate(settings.connect_timeout, settings.max_execution_time),
        Cluster::saturate(settings.receive_timeout, settings.max_execution_time),
        Cluster::saturate(settings.send_timeout, settings.max_execution_time));

    pools.push_back(std::make_shared<ConnectionPool>(
        settings.distributed_connections_pool_size,
        name.Host,
        name.Port,
        "" /*defaultDatabase*/,
        std::string(InternalRemoteUserName.data()) /*user*/,
        "" /*password*/,
        "" /*quota_key*/,
        "" /*cluster*/,
        "" /*cluster_secret*/,
        "server" /*client_name*/,
        Protocol::Compression::Enable,
        Protocol::Secure::Disable));

    auto connection = std::make_shared<ConnectionPoolWithFailover>(
        std::move(pools),
        settings.load_balancing,
        settings.connections_with_failover_max_tries);

    return std::make_shared<TClusterNode>(
        name,
        cookie,
        isLocal,
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
