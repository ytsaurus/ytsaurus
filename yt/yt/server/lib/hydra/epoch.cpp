#include "epoch.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/concurrency/fls.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

TFlsSlot<TEpochId> CurrentEpochId;

NElection::TEpochId GetCurrentEpochId()
{
    return *CurrentEpochId;
}

////////////////////////////////////////////////////////////////////////////////

TCurrentEpochIdGuard::TCurrentEpochIdGuard(TEpochId epochId)
{
    YT_VERIFY(!*CurrentEpochId);
    *CurrentEpochId = epochId;
}

TCurrentEpochIdGuard::~TCurrentEpochIdGuard()
{
    *CurrentEpochId = {};
}

////////////////////////////////////////////////////////////////////////////////

class TEpochIdInjectingInvoker
    : public TInvokerWrapper<false>
{
public:
    TEpochIdInjectingInvoker(IInvokerPtr underlying, TEpochId epochId)
        : TInvokerWrapper(std::move(underlying))
        , EpochId_(epochId)
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND([callback = std::move(callback), epochId = EpochId_] {
            TCurrentEpochIdGuard guard(epochId);
            callback();
        }));
    }

private:
    const TEpochId EpochId_;
};

IInvokerPtr CreateEpochIdInjectingInvoker(
    IInvokerPtr underlying,
    NElection::TEpochId epochId)
{
    return New<TEpochIdInjectingInvoker>(
        std::move(underlying),
        epochId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
