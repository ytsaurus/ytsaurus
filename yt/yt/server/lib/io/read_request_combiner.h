#pragma once

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TReadRequestCombiner
{
public:
    struct TIORequest
    {
        TIOEngineHandlePtr Handle;
        i64 Offset = 0;
        i64 Size = 0;
        TSharedMutableRef ResultBuffer;
        int Index = 0;
    };

    void Combine(
        const std::vector<IIOEngine::TReadRequest>& requests,
        i64 pageSize,
        NYTAlloc::EMemoryZone memoryZone,
        TRefCountedTypeCookie tagCookie);

    TError CheckEOF(const TMutableRef& bufferTail);

    const std::vector<TIORequest>& GetIORequests();
    const std::vector<TSharedRef>& GetOutputBuffers();

private:
    std::vector<TSharedRef> OutputRefs_;
    std::vector<TIORequest> IORequests_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

