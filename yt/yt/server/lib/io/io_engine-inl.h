#ifndef IO_ENGINE_INL_H_
#error "Direct inclusion of this file is not allowed, include io_engine.h"
// For the sake of sane code completion.
#include "io_engine.h"
#endif

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

inline bool TIOEngineHandle::IsOpenForDirectIO() const
{
     return OpenForDirectIO_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
TFuture<IIOEngine::TReadResponse> IIOEngine::Read(
    std::vector<TReadRequest> requests,
    EWorkloadCategory category,
    NYTAlloc::EMemoryZone memoryZone,
    TSessionId clientId)
{
    return Read(std::move(requests), category, memoryZone, GetRefCountedTypeCookie<TTag>(), clientId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
