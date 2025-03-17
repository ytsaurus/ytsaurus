#pragma once

#include "io_engine.h"

#include <array>
#include <vector>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TSlicedReadRequest
{
    TReadRequest Request;
    TSharedMutableRef OutputBuffer;
};

class TIORequestSlicer
{
public:
    TIORequestSlicer(i64 desiredSize, i64 minSize);

    std::vector<TSlicedReadRequest> Slice(TReadRequest request, const TSharedMutableRef& buffer) const;

    std::vector<TWriteRequest> Slice(TWriteRequest request) const;
    std::vector<TFlushFileRangeRequest> Slice(TFlushFileRangeRequest request) const;

private:
    template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
    std::vector<TSlicedRequest> SliceRequest(const TInputRequest& request, TSliceHandler handler) const;

private:
    const i64 DesiredRequestSize_;
    const i64 MinRequestSize_;
};

////////////////////////////////////////////////////////////////////////////////

class TDummyRequestSlicer
{
public:
    TDummyRequestSlicer(i64 desiredSize, i64 minSize);

    std::array<TSlicedReadRequest, 1> Slice(TReadRequest request, TSharedMutableRef buffer) const;
    std::array<TWriteRequest, 1> Slice(TWriteRequest request) const;
    std::array<TFlushFileRangeRequest, 1> Slice(TFlushFileRangeRequest request) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
