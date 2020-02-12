#include "user_job_synchronizer.h"
#include "user_job_synchronizer_proxy.h"

#include <yt/core/bus/tcp/client.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/bus/channel.h>

namespace NYT::NUserJobSynchronizerClient {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void TUserJobSynchronizer::NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError)
{
    JobSatellitePreparedPromise_.TrySet(rssOrError);
}

void TUserJobSynchronizer::NotifyExecutorPrepared()
{
    ExecutorPreparedPromise_.TrySet(TError());
}

void TUserJobSynchronizer::NotifyUserJobFinished(const TError& error)
{
    UserJobFinishedPromise_.TrySet(error);
}

void TUserJobSynchronizer::Wait()
{
    JobSatelliteRssUsage_ = WaitFor(JobSatellitePreparedPromise_.ToFuture())
        .ValueOrThrow();
    WaitFor(ExecutorPreparedPromise_.ToFuture())
        .ThrowOnError();
}

i64 TUserJobSynchronizer::GetJobSatelliteRssUsage() const
{
    return JobSatelliteRssUsage_;
}

TError TUserJobSynchronizer::GetUserProcessStatus() const
{
    if (!UserJobFinishedPromise_.IsSet()) {
        THROW_ERROR_EXCEPTION("Satellite did not finish successfully");
    }
    return UserJobFinishedPromise_.Get();
}

void TUserJobSynchronizer::CancelWait()
{
    JobSatellitePreparedPromise_.TrySet(TErrorOr<i64>(0));
    ExecutorPreparedPromise_.TrySet(TError());
}

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerClient
    : public IUserJobSynchronizerClient
{
public:
    explicit TUserJobSynchronizerClient(TTcpBusClientConfigPtr config)
    {
        auto client = CreateTcpBusClient(config);
        auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
        ControlServiceProxy_.reset(new TUserJobSynchronizerServiceProxy(channel));
    }

    virtual void NotifyJobSatellitePrepared(const TErrorOr<i64>& rssOrError) override
    {
        auto req = ControlServiceProxy_->SatellitePrepared();
        ToProto(req->mutable_error(), rssOrError);
        if (rssOrError.IsOK()) {
            req->set_rss(rssOrError.Value());
        }
        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    virtual void NotifyUserJobFinished(const TError& error) override
    {
        auto req = ControlServiceProxy_->UserJobFinished();
        ToProto(req->mutable_error(), error);
        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    virtual void NotifyExecutorPrepared() override
    {
        auto req = ControlServiceProxy_->ExecutorPrepared();
        WaitFor(req->Invoke())
            .ThrowOnError();
    }

private:
    std::unique_ptr<TUserJobSynchronizerServiceProxy> ControlServiceProxy_;
};

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(TTcpBusClientConfigPtr config)
{
    return New<TUserJobSynchronizerClient>(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobSynchronizerClient
