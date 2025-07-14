#include "admin_service.h"

#include "private.h"

#include <yt/yt/ytlib/admin/admin_service_proxy.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/fluent_log.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NAdmin {

using namespace NConcurrency;
using namespace NLogging;
using namespace NRpc;
using namespace NCoreDump;

////////////////////////////////////////////////////////////////////////////////

class TAdminService
    : public TServiceBase
{
public:
    TAdminService(
        IInvokerPtr invoker,
        ICoreDumperPtr coreDumper,
        IChannelFactoryPtr channelFactory,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            std::move(invoker),
            TAdminServiceProxy::GetDescriptor(),
            AdminLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , CoreDumper_(std::move(coreDumper))
        , ChannelFactory_(std::move(channelFactory))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Die));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingNode));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteCoreDump));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteLogBarrier));
    }

private:
    const NCoreDump::ICoreDumperPtr CoreDumper_;
    const IChannelFactoryPtr ChannelFactory_;


    void BeforeInvoke(NRpc::IServiceContext* context) override
    {
        if (context->GetAuthenticationIdentity().User != RootUserName) {
            THROW_ERROR_EXCEPTION("Only root is allowed to use AdminService");
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Die)
    {
        AbortProcessSilently(request->exit_code());
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, PingNode)
    {
        context->SetRequestInfo();

        TSharedRef payload;
        switch (request->Attachments().size()) {
            case 0:
                break;
            case 1:
                payload = request->Attachments()[0];
                if (payload.size() > PingNodePayloadMax) {
                    context->Reply(TError("Ping payload too large"));
                    return;
                }
                break;
            default:
                context->Reply(TError("Too many ping attachments"));
                return;
        }

        if (!request->chain_addresses().empty()) {
            if (!ChannelFactory_) {
                context->Reply(TError("Channel factory is not set up for ping chain"));
                return;
            }
            const auto& addresses = request->chain_addresses();
            if (addresses.size() > PingNodeChainMax) {
                context->Reply(TError("Too many addresses in ping chain"));
                return;
            }

            auto channel = ChannelFactory_->CreateChannel(addresses[0]);
            TAdminServiceProxy proxy(channel);

            auto req = proxy.PingNode();
            // TODO(khlebnikov): Forward other parameters like multiplexing band.
            req->SetTimeout(context->GetTimeout());
            req->mutable_chain_addresses()->Add(addresses.cbegin()+1, addresses.cend());

            if (payload) {
                req->Attachments().push_back(payload);
                payload = TSharedRef::MakeEmpty();
            }

            auto startTime = NProfiling::GetCpuInstant();
            auto rsp = WaitFor(req->Invoke())
                .ValueOrThrow();
            auto finishTime = NProfiling::GetCpuInstant();;

            auto* latencies = response->mutable_chain_latencies();
            latencies->Reserve(rsp->chain_latencies_size()+1);
            latencies->Add(ToProto(CpuDurationToDuration(finishTime - startTime)));
            latencies->MergeFrom(rsp->chain_latencies());

            if (payload && rsp->Attachments().size() == 1) {
                payload = rsp->Attachments()[0];
            }
        }

        if (payload) {
            response->Attachments().push_back(payload);
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WriteCoreDump)
    {
        if (!CoreDumper_) {
            THROW_ERROR_EXCEPTION("Core dumper is not set up");
        }

        context->SetRequestInfo();

        auto path = CoreDumper_->WriteCoreDump({
            "Reason: RPC",
            "RequestId: " + ToString(context->GetRequestId()),
        }, "rpc_call").Path;
        response->set_path(path);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, WriteLogBarrier)
    {
        context->SetRequestInfo();

        // We need to ensure that the barrier isn't reordered with writes that happened before
        // WriteLogBarrier. Logging subsystem doesn't give any guarantees about happens-before
        // relations, so all we can do is to wait for some small amount of time.
        static constexpr auto PendingEventsWaitTime = TDuration::MilliSeconds(10);
        TDelayedExecutor::WaitForDuration(PendingEventsWaitTime);

        NLogging::TLogger logger(request->category());
        auto barrierId = TGuid::Create();
        LogStructuredEventFluently(logger, ELogLevel::Info)
            .Item("barrier_id").Value(ToString(barrierId))
            .Item("system_event_kind").Value("barrier");
        ToProto(response->mutable_barrier_id(), barrierId);

        // Ensure that the barrier is written on disk. This is necessary, since tests will try
        // to read our log just after writing the barrier and must see this barrier while reading.
        TLogManager::Get()->Synchronize();

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateAdminService(
    IInvokerPtr invoker,
    ICoreDumperPtr coreDumper,
    IChannelFactoryPtr channelFactory,
    IAuthenticatorPtr authenticator)
{
    return New<TAdminService>(
        std::move(invoker),
        std::move(coreDumper),
        std::move(channelFactory),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAdmin
