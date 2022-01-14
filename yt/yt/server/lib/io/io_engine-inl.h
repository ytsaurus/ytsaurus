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

inline void TIOEngineHandle::MarkOpenForDirectIO(EOpenMode* oMode)
{
    *oMode |= DirectAligned;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
TFuture<IIOEngine::TReadResponse> IIOEngine::Read(
    std::vector<TReadRequest> requests,
    EWorkloadCategory category,
    TSessionId clientId)
{
    return Read(std::move(requests), category, GetRefCountedTypeCookie<TTag>(), clientId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
