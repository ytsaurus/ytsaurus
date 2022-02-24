#include "bootstrap.h"
#include "config.h"
#include "tablet_balancer.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer
    :  public ITabletBalancer
{
public:
    TTabletBalancer(
        IBootstrap* bootstrap,
        TTabletBalancerConfigPtr config,
        IInvokerPtr controlInvoker);

    void Start() override;

    void Stop() override;

private:
    IBootstrap* const Bootstrap_;
    const TTabletBalancerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const NConcurrency::TPeriodicExecutorPtr PollExecutor_;

    void GatherTablets();

    void DoStop();
};

TTabletBalancer::TTabletBalancer(
    IBootstrap* bootstrap,
    TTabletBalancerConfigPtr config,
    IInvokerPtr controlInvoker)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , PollExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TTabletBalancer::GatherTablets, MakeWeak(this)),
        Config_->Period))
{ }

void TTabletBalancer::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Starting tablet balancer instance");

    PollExecutor_->Start();
}

void TTabletBalancer::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Stopping tablet balancer instance");

    ControlInvoker_->Invoke(BIND(&TTabletBalancer::DoStop, MakeWeak(this)));
}

void TTabletBalancer::GatherTablets()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    // TODO(alexelex): remove debug list request

    auto result = WaitFor(Bootstrap_
        ->GetMasterClient()
        ->ListNode("//sys"))
        .ValueOrThrow();

    YT_LOG_INFO("yt list //sys %v", result);
}

void TTabletBalancer::DoStop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Stopping pool");

    PollExecutor_->Stop();

    // TODO(alexelex): wait all tablet_actions

    // TODO(alexelex): could clear state, could not

    YT_LOG_INFO("Tablet balancer instance stopped");
}

////////////////////////////////////////////////////////////////////////////////

ITabletBalancerPtr CreateTabletBalancer(
    IBootstrap* bootstrap,
    TTabletBalancerConfigPtr config,
    IInvokerPtr controlInvoker)
{
    return New<TTabletBalancer>(bootstrap, config, std::move(controlInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
