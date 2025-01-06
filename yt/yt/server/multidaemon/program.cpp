#include "program.h"

#include "config.h"

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/server_program/server_program.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/scheduler/bootstrap.h>
#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/server/http_proxy/bootstrap.h>
#include <yt/yt/server/http_proxy/config.h>

#include <yt/yt/server/rpc_proxy/bootstrap.h>
#include <yt/yt/server/rpc_proxy/config.h>

#include <yt/yt/server/cell_balancer/bootstrap.h>
#include <yt/yt/server/cell_balancer/config.h>

#include <yt/yt/server/clock_server/cluster_clock/bootstrap.h>
#include <yt/yt/server/clock_server/cluster_clock/config.h>

#include <yt/yt/server/controller_agent/bootstrap.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/cypress_proxy/bootstrap.h>
#include <yt/yt/server/cypress_proxy/config.h>

#include <yt/yt/server/discovery_server/bootstrap.h>
#include <yt/yt/server/discovery_server/config.h>

#include <yt/yt/server/kafka_proxy/bootstrap.h>
#include <yt/yt/server/kafka_proxy/config.h>

#include <yt/yt/server/master_cache/bootstrap.h>
#include <yt/yt/server/master_cache/config.h>

#include <yt/yt/server/queue_agent/bootstrap.h>
#include <yt/yt/server/queue_agent/config.h>

#include <yt/yt/server/replicated_table_tracker/bootstrap.h>
#include <yt/yt/server/replicated_table_tracker/config.h>

#include <yt/yt/server/tablet_balancer/bootstrap.h>
#include <yt/yt/server/tablet_balancer/config.h>

#include <yt/yt/server/tcp_proxy/bootstrap.h>
#include <yt/yt/server/tcp_proxy/config.h>

#include <yt/yt/server/timestamp_provider/bootstrap.h>
#include <yt/yt/server/timestamp_provider/config.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

namespace NYT::NMultidaemon {

using namespace NFusion;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Multidaemon");

////////////////////////////////////////////////////////////////////////////////

using TBootstrapFactory = std::function<NServer::IDaemonBootstrapPtr(
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator)>;

class TDaemonTypeMap
{
public:
    TBootstrapFactory GetFactoryOrThrow(const std::string& type) const
    {
        auto it = TypeToFactory_.find(type);
        if (it == TypeToFactory_.end()) {
            THROW_ERROR_EXCEPTION("Unknown daemon type %Qv", type);
        }
        return it->second;
    }

private:
    friend class TDaemonTypeMapBuilder;

    explicit TDaemonTypeMap(THashMap<std::string, TBootstrapFactory> typeToFactory)
        : TypeToFactory_(std::move(typeToFactory))
    { }

    const THashMap<std::string, TBootstrapFactory> TypeToFactory_;
};

////////////////////////////////////////////////////////////////////////////////

class TDaemonTypeMapBuilder
{
public:
    template <class TBootstrap, class TConfig>
    TDaemonTypeMapBuilder Add(
        const std::string& type,
        TIntrusivePtr<TBootstrap>(*factory)(
            TIntrusivePtr<TConfig> config,
            NYTree::INodePtr configNode,
            NFusion::IServiceLocatorPtr serviceLocator)) &&
    {
        auto typeErasedFactory = [=] (NYTree::INodePtr configNode, NFusion::IServiceLocatorPtr serviceLocator) -> NServer::IDaemonBootstrapPtr {
            auto config = NYTree::ConvertTo<TIntrusivePtr<TConfig>>(configNode);
            return factory(
                std::move(config),
                std::move(configNode),
                std::move(serviceLocator));
        };
        EmplaceOrCrash(TypeToFactory_, type, std::move(typeErasedFactory));
        return std::move(*this);
    }

    TDaemonTypeMap Finish() &&
    {
        return TDaemonTypeMap(std::move(TypeToFactory_));
    }

private:
    THashMap<std::string, TBootstrapFactory> TypeToFactory_;
};

////////////////////////////////////////////////////////////////////////////////

const TDaemonTypeMap& GetDaemonTypeMap()
{
    static const auto result = TDaemonTypeMapBuilder()
        .Add("master", NCellMaster::CreateMasterBootstrap)
        .Add("node", NClusterNode::CreateNodeBootstrap)
        .Add("scheduler", NScheduler::CreateSchedulerBootstrap)
        .Add("http_proxy", NHttpProxy::CreateHttpProxyBootstrap)
        .Add("rpc_proxy", NRpcProxy::CreateRpcProxyBootstrap)
        .Add("cell_balancer", NCellBalancer::CreateCellBalancerBootstrap)
        .Add("clock", NClusterClock::CreateClusterClockBootstrap)
        .Add("controller_agent", NControllerAgent::CreateControllerAgentBootstrap)
        .Add("cypress_proxy", NCypressProxy::CreateCypressProxyBootstrap)
        .Add("discovery_server", NClusterDiscoveryServer::CreateDiscoveryServerBootstrap)
        .Add("kafka_proxy", NKafkaProxy::CreateKafkaProxyBootstrap)
        .Add("master_cache", NMasterCache::CreateMasterCacheBootstrap)
        .Add("queue_agent", NQueueAgent::CreateQueueAgentBootstrap)
        .Add("replicated_table_tracker", NReplicatedTableTracker::CreateReplicatedTableTrackerBootstrap)
        .Add("tablet_balancer", NTabletBalancer::CreateTabletBalancerBootstrap)
        .Add("tcp_proxy", NTcpProxy::CreateTcpProxyBootstrap)
        .Add("timestamp_provider", NTimestampProvider::CreateTimestampProviderBootstrap)
        .Finish();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TMultidaemonProgram
    : public TServerProgram<TMultidaemonProgramConfig>
{
public:
    TMultidaemonProgram()
    {
        SetMainThreadName("MultiProg");
    }

private:
    void DoStart() final
    {
        auto config = GetConfig();

        const auto& serverTypeMap = GetDaemonTypeMap();

        struct TServerSlot
        {
            std::string Name;
            NServer::IDaemonBootstrapPtr Bootstrap;
        };
        std::vector<TServerSlot> serverSlots;
        DoNotOptimizeAway(serverSlots);

        for (const auto& [serverId, serverConfig] : config->Daemons) {
            YT_LOG_INFO("Creating daemon (Type: %v, Id: %v)",
                serverConfig->Type,
                serverId);
            auto factory = serverTypeMap.GetFactoryOrThrow(serverConfig->Type);
            auto bootstrap = factory(serverConfig->Config, GetServiceLocator());
            serverSlots.emplace_back(serverId, std::move(bootstrap));
        }

        std::vector<TFuture<void>> startFutures;
        for (const auto& slot : serverSlots) {
            YT_LOG_INFO("Starting daemon (Id: %v)",
                slot.Name);
            startFutures.push_back(slot.Bootstrap->Run()
                .Apply(BIND([name = slot.Name] (const TError& error) {
                    if (!error.IsOK()) {
                        THROW_ERROR_EXCEPTION("Error starting daemon %Qv", name)
                            << error;
                    }
                    YT_LOG_INFO("Daemon started (Id: %v)",
                        name);
                })));
        }

        YT_LOG_INFO("Waiting for daemon to start");

        AllSucceeded(std::move(startFutures))
            .Get()
            .ThrowOnError();

        YT_LOG_INFO("Multidaemon is running");

       SleepForever();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RuNMultidaemonProgram(int argc, const char** argv)
{
    TMultidaemonProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMultidaemon
