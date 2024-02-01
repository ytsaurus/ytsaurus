#ifndef MEMORY_TRACKING_SERVICE_BASE_H_
#error "Direct inclusion of this file is not allowed, include memory_tracking_service_base.h"
// For the sake of sane code completion.
#include "memory_tracking_service_base.h"
#endif

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TBaseService>
template <typename... TArgs>
TMemoryTrackingServiceBase<TBaseService>::TMemoryTrackingServiceBase(
    ITypedNodeMemoryTrackerPtr memoryTracker,
    IMemoryReferenceTrackerPtr memoryReferenceTracker,
    TArgs&&... args)
    : TBaseService(std::forward<TArgs>(args)...)
    , MemoryTracker_(std::move(memoryTracker))
    , MemoryReferenceTracker_(std::move(memoryReferenceTracker))
{ }

template <class TBaseService>
void TMemoryTrackingServiceBase<TBaseService>::HandleRequest(
    std::unique_ptr<NRpc::NProto::TRequestHeader> header,
    TSharedRefArray message,
    NBus::IBusPtr replyBus)
{
    if (MemoryTracker_ && MemoryTracker_->IsExceeded()) {
        return TBaseService::ReplyError(
            TError(
                NRpc::EErrorCode::MemoryOverflow,
                "Request is dropped due to high memory pressure"),
            *header,
            replyBus);
    }

    if (MemoryReferenceTracker_) {
        message = TrackMemory(MemoryReferenceTracker_, std::move(message));
    }

    TBaseService::HandleRequest(std::move(header), std::move(message), std::move(replyBus));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
