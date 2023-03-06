#pragma once

#include "io_engine.h"

#include <array>
#include <vector>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TSlicedReadRequest
{
    IIOEngine::TReadRequest Request;
    TMutableRef OutputBuffer;
};

class TIORequestSlicer
{
public:
    TIORequestSlicer(i64 desiredSize, i64 minSize);

    std::vector<TSlicedReadRequest> Slice(IIOEngine::TReadRequest request, TMutableRef buffer) const;

    std::vector<IIOEngine::TWriteRequest> Slice(IIOEngine::TWriteRequest request) const;
    std::vector<IIOEngine::TFlushFileRangeRequest> Slice(IIOEngine::TFlushFileRangeRequest request) const;

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

    std::array<TSlicedReadRequest, 1> Slice(IIOEngine::TReadRequest request, TMutableRef buffer) const;
    std::array<IIOEngine::TWriteRequest, 1> Slice(IIOEngine::TWriteRequest request) const;
    std::array<IIOEngine::TFlushFileRangeRequest, 1> Slice(IIOEngine::TFlushFileRangeRequest request) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
