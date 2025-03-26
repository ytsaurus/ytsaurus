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
        TReadRequest ReadRequest;
        TSharedMutableRef ResultBuffer;
    };

    virtual std::vector<TCombinedRequest> Combine(
        std::vector<TReadRequest> requests,
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

