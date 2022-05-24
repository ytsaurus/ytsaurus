#pragma once

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IReadRequestCombiner
{
    virtual ~IReadRequestCombiner() = default;

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

    virtual TCombineResult Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        TRefCountedTypeCookie tagCookie) = 0;

    virtual TError CheckEof(const TMutableRef& bufferTail) = 0;

    virtual std::vector<TSharedRef>&& ReleaseOutputBuffers() = 0;
};

using IReadRequestCombinerPtr = std::unique_ptr<IReadRequestCombiner>;

////////////////////////////////////////////////////////////////////////////////

IReadRequestCombinerPtr CreateReadRequestCombiner();

IReadRequestCombinerPtr CreateDummyReadRequestCombiner();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

