#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/misc/error.h>

using namespace NYT;
using namespace NConcurrency;
using namespace NTransactionSupervisor;

int main(int /*argc*/, char* argv[])
{
    try {
        auto addr = TString(argv[1]);
        auto cellId = TGuid::FromString(TString(argv[2]));
        auto txId = TGuid::FromString(TString(argv[3]));

        auto channel = NRpc::CreateRealmChannel(
            NRpc::NBus::CreateTcpBusChannelFactory(New<NBus::TBusConfig>())->CreateChannel(addr),
            cellId);

        TTransactionParticipantServiceProxy proxy(channel);
        auto req = proxy.AbortTransaction();
        ToProto(req->mutable_transaction_id(), txId);

        WaitFor(req->Invoke()).ValueOrThrow();
    } catch (std::exception& e) {
        Cerr << ToString(TError(e)) << Endl;
    }

    return 0;
}
