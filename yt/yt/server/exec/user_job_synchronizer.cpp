#include "user_job_synchronizer.h"
#include "user_job_synchronizer_proxy.h"

#include <yt/yt/server/lib/user_job/config.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/bus/channel.h>

namespace NYT::NUserJob {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerClient
    : public IUserJobSynchronizerClient
{
public:
    explicit TUserJobSynchronizerClient(TBusClientConfigPtr config)
    {
        auto client = CreateBusClient(config);
        auto channel = NRpc::NBus::CreateBusChannel(std::move(client));
        ControlServiceProxy_.reset(new TUserJobSynchronizerServiceProxy(channel));
    }

    void NotifyExecutorPrepared() override
    {
        auto req = ControlServiceProxy_->ExecutorPrepared();
        req->set_pid(::getpid());
        WaitFor(req->Invoke())
            .ThrowOnError();
    }

private:
    std::unique_ptr<TUserJobSynchronizerServiceProxy> ControlServiceProxy_;
};

////////////////////////////////////////////////////////////////////////////////

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(TUserJobSynchronizerConnectionConfigPtr connectionConfig)
{
    return New<TUserJobSynchronizerClient>(connectionConfig->BusClientConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJob
