#pragma once

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TReadRequestCombiner
{
public:
    struct TIORequest
    {
        i64 Offset = 0;
        i64 Size = 0;
        TMutableRef ResultBuffer;
        int Index = 0;
    };

    using TCombineResult = std::tuple<
        std::vector<TIOEngineHandlePtr>,
        std::vector<TIORequest>>;

    TCombineResult Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        NYTAlloc::EMemoryZone memoryZone,
        TRefCountedTypeCookie tagCookie);

    TError CheckEOF(const TMutableRef& bufferTail);

    std::vector<TSharedRef>&& ReleaseOutputBuffers();

private:
    std::vector<TSharedRef> OutputRefs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

