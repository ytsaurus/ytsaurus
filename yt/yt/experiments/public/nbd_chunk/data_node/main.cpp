#include "bootstrap.h"

#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>

#include <yt/yt/server/lib/nbd/chunk_handler.h>

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/data_node_nbd_service.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

namespace NYT::NNbd {

using namespace NConcurrency;
using namespace NRpc;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, Profiler, "/test");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TConfig)

struct TConfig
    : public NYTree::TYsonStruct
{
    NClusterNode::TClusterNodeBootstrapConfigPtr ClusterNodeConfig;
    int Port;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster_node", &TThis::ClusterNodeConfig)
            .DefaultNew();
        registrar.Parameter("port", &TThis::Port)
            .Default(1);
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig)

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_.AddLongOption("config", "").RequiredArgument("PATH").StoreResult(&ConfigPath_);
    }

protected:
    void DoRun() override
    {
        auto singletonsConfig = New<TSingletonsConfig>();
        ConfigureSingletons(singletonsConfig);

        //NLogging::TLogManager::Get()->Configure(NLogging::TLogManagerConfig::CreateStderrLogger(NLogging::ELogLevel::Debug));

        auto config = NYTree::ConvertTo<TConfigPtr>(NYson::TYsonString(TFileInput(ConfigPath_).ReadAll()));
        auto busServer = NYT::NBus::CreateBusServer(NYT::NBus::TBusServerConfig::CreateTcp(config->Port));
        auto rpcServer = NRpc::NBus::CreateBusServer(busServer);

        auto queue = New<TActionQueue>("RPC");
        auto bootstrap = CreateBootstrap(queue->GetInvoker(), config->ClusterNodeConfig, Logger(), Profiler());
        bootstrap->Initialize();

        rpcServer->RegisterService(CreateDataNodeNbdService(bootstrap.Get(), Logger()));
        rpcServer->Start();

        Sleep(TDuration::Max());
    }

private:
    TString ConfigPath_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

int main(int argc, const char* argv[])
{
    return NYT::NNbd::TProgram().Run(argc, argv);
}
