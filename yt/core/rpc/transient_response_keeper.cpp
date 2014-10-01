#include "stdafx.h"
#include "transient_response_keeper.h"
#include "response_keeper_detail.h"

#include <core/concurrency/periodic_executor.h>

#include <core/actions/invoker_util.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto EvictionPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TTransientResponseKeeper
    : public TResponseKeeperBase
    , public IResponseKeeper
{
public:
    TTransientResponseKeeper(
        TResponseKeeperConfigPtr config,
        const NProfiling::TProfiler& profiler)
        : TResponseKeeperBase(config, profiler)
    {
        EvictionExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TTransientResponseKeeper::OnEvict, MakeWeak(this)),
            EvictionPeriod);
        EvictionExecutor_->Start();
    }

    virtual TFuture<TSharedRefArray> TryBeginRequest(const TMutationId& id) override
    {
        return TResponseKeeperBase::TryBeginRequest(id);
    }

    virtual void EndRequest(const TMutationId& id, TSharedRefArray response) override
    {
        TResponseKeeperBase::EndRequest(
            id,
            std::move(response),
            TInstant::Now(),
            true);
    }

private:
    TPeriodicExecutorPtr EvictionExecutor_;


    void OnEvict()
    {
        EvictExpiredResponses(TInstant::Now());
    }

};

IResponseKeeperPtr CreateTransientResponseKeeper(
    TResponseKeeperConfigPtr config,
    const NProfiling::TProfiler& profiler)
{
    return New<TTransientResponseKeeper>(config, profiler);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
