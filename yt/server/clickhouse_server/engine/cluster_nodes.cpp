#include "cluster_nodes.h"

//#include <Common/Exception.h>

namespace DB {

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TClusterNode
    : public IClusterNode
{
private:
    TClusterNodeName Name;
    bool Local;
    DB::ConnectionPoolWithFailoverPtr Connection;

public:
    TClusterNode(
        const TClusterNodeName name,
        bool local,
        DB::ConnectionPoolWithFailoverPtr connection)
        : Name(name)
        , Local(local)
        , Connection(std::move(connection))
    {
    }

    TClusterNodeName GetName() const override
    {
        return Name;
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

IClusterNodePtr CreateClusterNode(const TClusterNodeName& name, const DB::Settings& settings, uint64_t clickhouse_port)
{
    // This is really weird...

    DB::Cluster::Address address(
       /*host_port=*/ name.ToString(),
       /*user=*/ "",
       /*password=*/ "",
       /*clickhouse_port=*/ clickhouse_port);

    ConnectionPoolPtrs pools;
    ConnectionTimeouts timeouts(Cluster::saturate(settings.connect_timeout, settings.max_execution_time),
        Cluster::saturate(settings.receive_timeout, settings.max_execution_time),
        Cluster::saturate(settings.send_timeout, settings.max_execution_time));

    pools.push_back(std::make_shared<ConnectionPool>(
        settings.distributed_connections_pool_size,
        address.host_name,
        address.port,
        address.default_database,
        address.user,
        address.password,
        timeouts,
        "server",
        Protocol::Compression::Enable,
        Protocol::Secure::Disable));

    auto connection = std::make_shared<ConnectionPoolWithFailover>(
        std::move(pools),
        settings.load_balancing,
        settings.connections_with_failover_max_tries);

    return std::make_shared<TClusterNode>(
        name,
        address.is_local,
        std::move(connection));
}

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
