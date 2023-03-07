#include "user_job_synchronizer.h"
#include "user_job_synchronizer_proxy.h"

#include <yt/core/bus/tcp/client.h>
#include <yt/core/bus/tcp/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/rpc/bus/channel.h>

namespace NYT::NUserJobSynchronizerClient {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TUserJobSynchronizerConnectionConfig::TUserJobSynchronizerConnectionConfig()
{
    RegisterParameter("bus_client_config", BusClientConfig)
        .DefaultNew();
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

IUserJobSynchronizerClientPtr CreateUserJobSynchronizerClient(TUserJobSynchronizerConnectionConfigPtr connectionConfig)
{
    return New<TUserJobSynchronizerClient>(connectionConfig->BusClientConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobSynchronizerClient
