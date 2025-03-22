#include <yt/yt/server/lib/nbd/chunk_block_device.h>
#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TConfig)

struct TConfig
    : public NYTree::TYsonStruct
{
    TNbdServerConfigPtr NbdServer;
    THashMap<TString, TChunkBlockDeviceConfigPtr> NbdChunkBlockDevices;
    std::string DataNodeNbdServiceAddress;
    int ThreadCount;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nbd_server", &TThis::NbdServer)
            .DefaultNew();
        registrar.Parameter("nbd_devices", &TThis::NbdChunkBlockDevices)
            .Default();
        registrar.Parameter("data_node_nbd_service_address", &TThis::DataNodeNbdServiceAddress)
            .Default();
        registrar.Parameter("thread_count", &TThis::ThreadCount)
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
        Opts_.AddLongOption("config", "path to config").StoreResult(&ConfigPath_).Required();
    }

protected:
    void DoRun() override
    {
        auto config = NYTree::ConvertTo<NNbd::TConfigPtr>(NYson::TYsonString(TFileInput(ConfigPath_).ReadAll()));

        auto poller = NConcurrency::CreateThreadPoolPoller(config->ThreadCount, "Poller");
        auto threadPool = NConcurrency::CreateThreadPool(config->ThreadCount, "Nbd");

        auto nbdServer = CreateNbdServer(
            config->NbdServer,
            poller,
            threadPool->GetInvoker());

        for (const auto& [deviceId, deviceConfig] : config->NbdChunkBlockDevices) {
            // Create channel to data node NBD service.
            auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(config->DataNodeNbdServiceAddress));
            auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
            auto logger = nbdServer->GetLogger();

            auto device = CreateChunkBlockDevice(
                "export_id",
                std::move(deviceConfig),
                NConcurrency::GetUnlimitedThrottler(),
                NConcurrency::GetUnlimitedThrottler(),
                threadPool->GetInvoker(),
                std::move(channel),
                logger);

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
