#include <yt/yt/server/lib/nbd/memory_block_device.h>
#include <yt/yt/server/lib/nbd/config.h>
#include <yt/yt/server/lib/nbd/server.h>

#include <yt/yt/core/ytree/yson_struct.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NNbd {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConfig)

class TConfig
    : public NYTree::TYsonStruct
{
public:
    TNbdServerConfigPtr NbdServer;
    THashMap<TString, TMemoryBlockDeviceConfigPtr> MemoryDevices;

    REGISTER_YSON_STRUCT(TConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("nbd_server", &TThis::NbdServer)
            .DefaultNew();
        registrar.Parameter("devices", &TThis::MemoryDevices)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TConfig)

////////////////////////////////////////////////////////////////////////////////

void Main(const TConfigPtr& config)
{
    auto poller = CreateThreadPoolPoller(/*threadCount*/ 2, "Poller");
    auto threadPool = CreateThreadPool(/*threadCount*/ 2, "Nbd");
    auto nbdServer = CreateNbdServer(
        config->NbdServer,
        nullptr /* client */,
        poller,
        threadPool->GetInvoker());

    for (const auto& [name, memoryConfig] : config->MemoryDevices) {
        auto device = CreateMemoryBlockDevice(memoryConfig);
        NConcurrency::WaitFor(device->Initialize()).ThrowOnError();
        nbdServer->RegisterDevice(name, std::move(device));
    }

    Sleep(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

int main(int argc, const char* argv[])
{
    if (argc != 2) {
        Cerr << "Usage: <config>" << Endl;
        return 1;
    }

    auto config = NYT::NYTree::ConvertTo<NYT::NNbd::TConfigPtr>(NYT::NYson::TYsonString(TFileInput(argv[1]).ReadAll()));
    try {
        Main(config);
    } catch (const std::exception& ex) {
        Cerr << ToString(NYT::TError(ex)) << Endl;
        return 1;
    }

    return 0;
}
