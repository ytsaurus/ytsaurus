#pragma once

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IReadRequestCombiner
    : public TRefCounted
{
    virtual ~IReadRequestCombiner() = default;

    struct TCombinedRequest
    {
        IIOEngine::TReadRequest ReadRequest;
        TMutableRef ResultBuffer;
    };

    virtual std::vector<TCombinedRequest> Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        TRefCountedTypeCookie tagCookie) = 0;

    virtual TError CheckEof(const TMutableRef bufferTail) const = 0;

    virtual std::vector<TSharedRef>&& ReleaseOutputBuffers() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReadRequestCombiner)

////////////////////////////////////////////////////////////////////////////////

IReadRequestCombinerPtr CreateReadRequestCombiner();

IReadRequestCombinerPtr CreateDummyReadRequestCombiner();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

