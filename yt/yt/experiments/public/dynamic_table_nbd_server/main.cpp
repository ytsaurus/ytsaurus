#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/dynamic_table_block_device.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

namespace NYT::NNbd {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConfig)

class TConfig
    : public NYTree::TYsonStruct
{
public:
    TString ClusterUser;
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;
    TNbdServerConfigPtr NbdServer;
    THashMap<TString, TDynamicTableBlockDeviceConfigPtr> DynamicTableBlockDevices;
    int ThreadCount;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster_user", &TThis::ClusterUser)
            .Default();
        registrar.Parameter("cluster_connection", &TThis::ClusterConnection)
            .DefaultNew();
        registrar.Parameter("nbd_server", &TThis::NbdServer)
            .DefaultNew();
        registrar.Parameter("dynamic_table_block_devices", &TThis::DynamicTableBlockDevices)
            .Default();
        registrar.Parameter("thread_count", &TThis::ThreadCount)
            .Default(2);
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
        Opts_.AddLongOption("config", "path to config").StoreResult(&ConfigPath_).Required();
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult&) override
    {
        ConfigureCrashHandler();

        auto singletonsConfig = New<TSingletonsConfig>();
        ConfigureSingletons(singletonsConfig);

        auto config = NYT::NYTree::ConvertTo<NYT::NNbd::TConfigPtr>(NYT::NYson::TYsonString(TFileInput(ConfigPath_).ReadAll()));

        auto poller = NConcurrency::CreateThreadPoolPoller(config->ThreadCount, "Poller");
        auto threadPool = NConcurrency::CreateThreadPool(config->ThreadCount, "Nbd");

        NApi::NNative::TConnectionOptions connectionOptions{};
        connectionOptions.ConnectionInvoker = threadPool->GetInvoker();
        auto connection = CreateConnection(config->ClusterConnection, std::move(connectionOptions));
        connection->GetNodeDirectorySynchronizer()->Start();
        connection->GetClusterDirectorySynchronizer()->Start();

        auto nbdServer = CreateNbdServer(
            config->NbdServer,
            connection,
            poller,
            threadPool->GetInvoker());

        auto clientOptions = NYT::NApi::TClientOptions::FromUser(config->ClusterUser);
        auto client = connection->CreateNativeClient(clientOptions);

        for (const auto& [deviceId, deviceConfig] : config->DynamicTableBlockDevices) {
            auto device = CreateDynamicTableBlockDevice(deviceId, deviceConfig, client, nbdServer->GetLogger());
            NConcurrency::WaitFor(device->Initialize()).ThrowOnError();
            nbdServer->RegisterDevice(deviceId, std::move(device));
        }

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
