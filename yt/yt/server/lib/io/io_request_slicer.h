#pragma once

#include "io_engine.h"

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

    std::vector<TSlicedReadRequest> Slice(IIOEngine::TReadRequest request, TMutableRef buffer);

    std::vector<IIOEngine::TWriteRequest> Slice(IIOEngine::TWriteRequest request);
    std::vector<IIOEngine::TFlushFileRangeRequest> Slice(IIOEngine::TFlushFileRangeRequest request);

private:
    template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
    std::vector<TSlicedRequest> SliceRequest(const TInputRequest& request, TSliceHandler handler);

private:
    const i64 DesiredRequestSize_;
    const i64 MinRequestSize_;
};

////////////////////////////////////////////////////////////////////////////////

class TDummyRequestSlicer
{
public:
    TDummyRequestSlicer(i64 /*desiredBlockSize*/, i64 /*minBlockSize*/)
    { }
    
    std::vector<TSlicedReadRequest> Slice(IIOEngine::TReadRequest request, TMutableRef buffer);
    std::vector<IIOEngine::TWriteRequest> Slice(IIOEngine::TWriteRequest request);
    std::vector<IIOEngine::TFlushFileRangeRequest> Slice(IIOEngine::TFlushFileRangeRequest request);
};

////////////////////////////////////////////////////////////////////////////////

}
