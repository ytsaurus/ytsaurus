#include "job_proxy.h"
#include "user_job_synchronizer.h"
#include "user_job_synchronizer_proxy.h"

#include <yt/core/rpc/service_detail.h>
#include <yt/core/bus/tcp_client.h>
#include <yt/core/rpc/bus_channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NRpc;
using namespace NBus;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;

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

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, SatellitePrepared)
    {
        Y_UNUSED(response);
        auto error = FromProto<TError>(request->error());
        if (error.IsOK()) {
            auto rss = FromProto<i64>(request->rss());
            JobControl_->NotifyJobSatellitePrepared(rss);
        } else {
            JobControl_->NotifyJobSatellitePrepared(error);
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, ExecutorPrepared)
    {
        Y_UNUSED(request);
        Y_UNUSED(response);
        JobControl_->NotifyExecutorPrepared();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NJobProxy::NProto, UserJobFinished)
    {
        Y_UNUSED(response);
        auto error = FromProto<TError>(request->error());
        context->SetRequestInfo("Error: %v", error);
        JobControl_->NotifyUserJobFinished(error);
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerClient
    : public IUserJobSynchronizerClient
{
public:
    explicit TUserJobSynchronizerClient(NBus::TTcpBusClientConfigPtr config)
    {
        auto client = CreateTcpBusClient(config);
        auto channel = NRpc::CreateBusChannel(std::move(client));
        ControlServiceProxy_.reset(new TUserJobSynchronizerServiceProxy(channel));
    }

    virtual void NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError) override
    {
        auto req = ControlServiceProxy_->SatellitePrepared();
        ToProto(req->mutable_error(), rssOrError);
        if (rssOrError.IsOK()) {
            req->set_rss(rssOrError.Value());
        }
        WaitFor(req->Invoke()).ThrowOnError();
    }

    virtual void NotifyUserJobFinished(const TError& error) override
    {
        auto req = ControlServiceProxy_->UserJobFinished();
        ToProto(req->mutable_error(), error);
        WaitFor(req->Invoke()).ThrowOnError();
    }

    virtual void NotifyExecutorPrepared() override
    {
        auto req = ControlServiceProxy_->ExecutorPrepared();
        WaitFor(req->Invoke()).ThrowOnError();
    }

private:
    std::unique_ptr<TUserJobSynchronizerServiceProxy> ControlServiceProxy_;
};

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateUserJobSynchronizerService(
    const NLogging::TLogger& logger,
    IUserJobSynchronizerClientPtr jobControl,
    IInvokerPtr controlInvoker)
{
    return New<TUserJobSynchronizerService>(logger, jobControl, controlInvoker);
}

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(NBus::TTcpBusClientConfigPtr config)
{
    return New<TUserJobSynchronizerClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
