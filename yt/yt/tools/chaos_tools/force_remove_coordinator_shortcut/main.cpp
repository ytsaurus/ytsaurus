#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/guid.h>

#include <string>

using namespace NYT;
using namespace NChaosClient;
using namespace NConcurrency;

int main(int /*argc*/, char* argv[])
{
    try {
        auto addr = std::string(argv[1]);
        auto coordinatorCellId = TGuid::FromString(std::string(argv[2]));
        auto chaosObjectId = TGuid::FromString(std::string(argv[3]));

        auto channel = NRpc::CreateRealmChannel(
            NRpc::NBus::CreateTcpBusChannelFactory(New<NBus::TBusConfig>())->CreateChannel(addr),
            coordinatorCellId);


        auto proxy = TCoordinatorServiceProxy(channel);
        auto req = proxy.ForsakeShortcut();
        ToProto(req->mutable_chaos_object_id(), chaosObjectId);

        WaitFor(req->Invoke()).ValueOrThrow();
    } catch (std::exception& e) {
        Cerr << ToString(TError(e)) << Endl;
    }

    return 0;
}
