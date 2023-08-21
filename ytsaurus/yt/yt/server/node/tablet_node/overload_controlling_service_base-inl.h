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

template <class TBaseService>
template <typename... TArgs>
TOverloadControllingServiceBase<TBaseService>::TOverloadControllingServiceBase(
    NTabletNode::IBootstrap* bootstrap,
    TArgs&&... args)
    : TBaseService(std::forward<TArgs>(args)...)
    , Bootstrap_(bootstrap)
{ }


template <class TBaseService>
void TOverloadControllingServiceBase<TBaseService>::HandleRequest(
    std::unique_ptr<NRpc::NProto::TRequestHeader> header,
    TSharedRefArray message,
    NBus::IBusPtr replyBus)
{
    const auto& controller = Bootstrap_->GetOverloadController();

    if (auto status = controller->GetOverloadStatus(header->service(), header->method()); status.SkipCall) {
        // Limit incoming request rate by slowing down further requests from this socket.
        TDelayedExecutor::WaitForDuration(status.ThrottleTime);

        if (status.DoNotReply) {
            // Fierce defense mode: do not send any reply on heavy overloads.
            return;
        }

        // Reply with error if we are slightly overloaded.
        return TBaseService::ReplyError(
            TError(
                NRpc::EErrorCode::Overloaded,
                "Request is dropped due to a tablet node overload"),
            *header,
            replyBus);
    }

    TBaseService::HandleRequest(std::move(header), std::move(message), std::move(replyBus));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
