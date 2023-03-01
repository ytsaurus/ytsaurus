#include "job_proxy.h"
#include "user_job_synchronizer_service.h"

#include <yt/yt/server/exec/user_job_synchronizer.h>
#include <yt/yt/server/exec/user_job_synchronizer_proxy.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/rpc/bus/channel.h>

namespace NYT::NJobProxy {

using namespace NRpc;
using namespace NYT::NBus;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NUserJob;

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerService
    : public TServiceBase
{
public:
    TUserJobSynchronizerService(
        const NLogging::TLogger& logger,
        TPromise<TExecutorInfo> executorPreparedPromise,
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
    TPromise<TExecutorInfo> ExecutorPreparedPromise_;

    DECLARE_RPC_SERVICE_METHOD(NUserJob::NProto, ExecutorPrepared)
    {
        Y_UNUSED(response);

        // YT-10547: This is a workaround for Porto container resurrection on core command.
        if (ExecutorPreparedPromise_.IsSet()) {
            context->Reply(TError("Executor has already prepared"));
            return;
        }

        ExecutorPreparedPromise_.TrySet(TExecutorInfo{.ProcessPid = static_cast<pid_t>(request->pid())});
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    TPromise<TExecutorInfo> executorPreparedPromise,
    IInvokerPtr controlInvoker)
{
    return New<TUserJobSynchronizerService>(logger, std::move(executorPreparedPromise), controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
