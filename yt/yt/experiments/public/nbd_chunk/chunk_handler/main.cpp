#include <yt/yt/server/lib/nbd/chunk_handler.h>
#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

#include <util/system/hp_timer.h>

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
    i64 Size;
    TString Medium;
    EFilesystemType FsType;
    TString Address;
    TDuration DataNodeNbdServiceRpcTimeout;
    TDuration DataNodeNbdServiceMakeTimeout;
    i64 NumIters;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("size", &TThis::Size)
            .Default(10_GB);
        registrar.Parameter("medium", &TThis::Medium)
            .Default("ssd");
        registrar.Parameter("fs_type", &TThis::FsType)
            .Default(EFilesystemType::Ext4);
        registrar.Parameter("address", &TThis::Address)
            .Default("localhost:8888");
        registrar.Parameter("data_node_nbd_service_rpc_timeout", &TThis::DataNodeNbdServiceRpcTimeout)
            .Default(TDuration::MilliSeconds(500));
        registrar.Parameter("data_node_nbd_service_make_timeout", &TThis::DataNodeNbdServiceMakeTimeout)
            .Default(TDuration::Seconds(1));
        registrar.Parameter("num_iters", &TThis::NumIters)
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
        auto config = NYTree::ConvertTo<TConfigPtr>(NYson::TYsonString(TFileInput(ConfigPath_).ReadAll()));

        auto client = NYT::NBus::CreateBusClient(NYT::NBus::TBusClientConfig::CreateTcp(config->Address));
        auto channel = NRpc::NBus::CreateBusChannel(client);
        auto queue = New<TActionQueue>("RPC");

        auto deviceConfig = New<TChunkBlockDeviceConfig>();
        deviceConfig->Size = config->Size;
        deviceConfig->FsType = config->FsType;
        deviceConfig->Medium = config->Medium;
        deviceConfig->DataNodeNbdServiceRpcTimeout = config->DataNodeNbdServiceRpcTimeout;
        deviceConfig->DataNodeNbdServiceMakeTimeout = config->DataNodeNbdServiceMakeTimeout;

        auto handler = NYT::NNbd::CreateChunkHandler(
            std::move(deviceConfig),
            queue->GetInvoker(),
            std::move(channel),
            Logger());

        handler->Initialize().Get().ThrowOnError();

        NHPTimer::STime t;
        NHPTimer::GetTime(&t);

        for (int i = 0; i < config->NumIters; ++i) {
            auto data = Format("data_%v", i);
            auto offset = RandomNumber<ui64>(config->Size - data.size());
            handler->Write(offset, TSharedRef::FromString(data), {}).Get().ThrowOnError();
            auto value = handler->Read(offset, data.length()).Get().ValueOrThrow();
            assert(ToString(value.ToStringBuf()) == data);
        }

        NHPTimer::STime tcur = t;
        double seconds = NHPTimer::GetTimePassed(&tcur);

        handler->Finalize().Get().ThrowOnError();

        Cout << "Rps test complete in " << seconds << " seconds. RPS = " << (double) config->NumIters / seconds << Endl;
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
