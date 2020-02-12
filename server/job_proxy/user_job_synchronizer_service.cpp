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
        IUserJobSynchronizerClientPtr jobControl,
        IInvokerPtr controlInvoker)
        : TServiceBase(
            controlInvoker,
            TUserJobSynchronizerServiceProxy::GetDescriptor(),
            logger)
        , JobControl_(jobControl)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SatellitePrepared));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UserJobFinished));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecutorPrepared));
    }

private:
    const IUserJobSynchronizerClientPtr JobControl_;
    bool SatellitePrepared_ = false;
    bool ExecutorPrepared_ = false;

    DECLARE_RPC_SERVICE_METHOD(NUserJobSynchronizerClient::NProto, SatellitePrepared)
    {
        Y_UNUSED(response);

        // This is a workaround for porto container resurrection on core command.
        // YT-10547
        if (SatellitePrepared_) {
            context->Reply(TError("Satellite has already prepared"));
            return;
        }

        SatellitePrepared_ = true;

        auto error = FromProto<TError>(request->error());
        if (error.IsOK()) {
            auto rss = FromProto<i64>(request->rss());
            JobControl_->NotifyJobSatellitePrepared(rss);
        } else {
            JobControl_->NotifyJobSatellitePrepared(error);
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NUserJobSynchronizerClient::NProto, ExecutorPrepared)
    {
        Y_UNUSED(request);
        Y_UNUSED(response);

        // This is a workaround for porto container resurrection on core command.
        // YT-10547
        if (ExecutorPrepared_) {
            context->Reply(TError("Executor has already prepared"));
            return;
        }

        ExecutorPrepared_ = true;

        JobControl_->NotifyExecutorPrepared();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NUserJobSynchronizerClient::NProto, UserJobFinished)
    {
        Y_UNUSED(response);
        auto error = FromProto<TError>(request->error());
        context->SetRequestInfo("Error: %v", error);
        JobControl_->NotifyUserJobFinished(error);
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    IUserJobSynchronizerClientPtr jobControl,
    IInvokerPtr controlInvoker)
{
    return New<TUserJobSynchronizerService>(logger, jobControl, controlInvoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
