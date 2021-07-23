#pragma once
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

template<class TTag>
TFuture<IIOEngine::TReadResponse> IIOEngine::Read(
    std::vector<TReadRequest> requests,
    i64 priority,
    NYTAlloc::EMemoryZone memoryZone)
{
    return Read(std::move(requests), priority, memoryZone, GetRefCountedTypeCookie<TTag>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
