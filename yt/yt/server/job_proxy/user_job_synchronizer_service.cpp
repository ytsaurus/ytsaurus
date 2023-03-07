#include "job_proxy.h"

#include <yt/server/lib/user_job_synchronizer_client/user_job_synchronizer.h>
#include <yt/server/lib/user_job_synchronizer_client/user_job_synchronizer_proxy.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/bus/tcp/client.h>

#include <yt/core/rpc/bus/channel.h>

namespace NYT::NJobProxy {

using namespace NRpc;
using namespace NYT::NBus;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NUserJobSynchronizerClient;

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerService
    : public TServiceBase
{
public:
    TUserJobSynchronizerService(
        const NLogging::TLogger& logger,
        TPromise<void> executorPreparedPromise,
        IInvokerPtr controlInvoker)
        : TServiceBase(
            controlInvoker,
            TUserJobSynchronizerServiceProxy::GetDescriptor(),
            logger)
        , ExecutorPreparedPromise_(std::move(executorPreparedPromise))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecutorPrepared));
    }

private:
    const IUserJobSynchronizerClientPtr JobControl_;
    TPromise<void> ExecutorPreparedPromise_;

    DECLARE_RPC_SERVICE_METHOD(NUserJobSynchronizerClient::NProto, ExecutorPrepared)
    {
        Y_UNUSED(request);
        Y_UNUSED(response);

        // This is a workaround for porto container resurrection on core command.
        // YT-10547
        if (ExecutorPreparedPromise_.IsSet()) {
            context->Reply(TError("Executor has already prepared"));
            return;
        }

        ExecutorPreparedPromise_.TrySet(TError());
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    TPromise<void> executorPreparedPromise,
    IInvokerPtr controlInvoker)
{
    return New<TUserJobSynchronizerService>(logger, std::move(executorPreparedPromise), controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
