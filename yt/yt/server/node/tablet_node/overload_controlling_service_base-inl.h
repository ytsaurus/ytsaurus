#ifndef OVERLOAD_CONTROLLING_SERVICE_BASE_INL_H_
#error "Direct inclusion of this file is not allowed, include overload_controlling_service_base.h"
// For the sake of sane code completion.
#include "overload_controlling_service_base.h"
#endif


#include "bootstrap.h"
#include "overload_controller.h"

#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NTabletNode {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

std::optional<TDuration> GetTimeout(const std::unique_ptr<NRpc::NProto::TRequestHeader>& header);

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
template <typename... TArgs>
TOverloadControllingServiceBase<TBaseService>::TOverloadControllingServiceBase(
    NTabletNode::IBootstrap* bootstrap,
    TArgs&&... args)
    : TBaseService(std::forward<TArgs>(args)...)
    , Bootstrap_(bootstrap)
{ }

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::SubscribeLoadAdjusted()
{
    const auto& controller = Bootstrap_->GetOverloadController();
    YT_VERIFY(controller);

    controller->SubscribeLoadAdjusted(BIND(
        &TOverloadControllingServiceBase::HandleLoadAdjusted,
        MakeWeak(this)));
}

template <class TBaseService>
auto TOverloadControllingServiceBase<TBaseService>::RegisterMethod(
    const TMethodDescriptor& descriptor) -> TRuntimeMethodInfoPtr
{
    Methods_.insert(descriptor.Method);
    return TBaseService::RegisterMethod(descriptor);
}

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::HandleLoadAdjusted()
{
    const auto& controller = Bootstrap_->GetOverloadController();
    const auto& serviceName = TBaseService::GetServiceId().ServiceName;

    for (const auto& method : Methods_) {
        auto* runtimeInfo = TBaseService::FindMethodInfo(method);
        YT_VERIFY(runtimeInfo);

        auto congestionState = controller->GetCongestionState(serviceName, method);
        runtimeInfo->ConcurrencyLimit.SetDynamicLimit(congestionState.CurrentWindow);
        runtimeInfo->WaitingTimeoutFraction.store(
            congestionState.WaitingTimeoutFraction,
            std::memory_order::relaxed);
    }
}

template <class TBaseService>
std::optional<TError> TOverloadControllingServiceBase<TBaseService>::GetThrottledError(
    const NRpc::NProto::TRequestHeader& requestHeader)
{
    const auto& controller = Bootstrap_->GetOverloadController();
    auto congestionState = controller->GetCongestionState(requestHeader.service(), requestHeader.method());
    const auto& overloadedTrackers = congestionState.OverloadedTrackers;

    if (!overloadedTrackers.empty()) {
        return TError(NRpc::EErrorCode::Overloaded, "Instance is overloaded")
            << TErrorAttribute("overloaded_trackers", overloadedTrackers);
    }

    return TBaseService::GetThrottledError(requestHeader);
}

template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::HandleRequest(
    std::unique_ptr<NRpc::NProto::TRequestHeader> header,
    TSharedRefArray message,
    NBus::IBusPtr replyBus)
{
    const auto& controller = Bootstrap_->GetOverloadController();
    auto congestionState = controller->GetCongestionState(
        header->service(),
        header->method());

    if (ShouldThrottleCall(congestionState)) {
        // Give other handling routines chance to execute.
        NConcurrency::Yield();
    }

    TBaseService::HandleRequest(std::move(header), std::move(message), std::move(replyBus));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
