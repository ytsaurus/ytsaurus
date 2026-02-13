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
    TIORequestSlicer(i64 desiredSize, i64 minSize, bool enableSlicing);

    std::vector<TSlicedReadRequest> Slice(TReadRequest request, const TSharedMutableRef& buffer, int directIoBlockSize) const;

    std::vector<TWriteRequest> Slice(TWriteRequest request, int directIoBlockSize) const;
    std::vector<TFlushFileRangeRequest> Slice(TFlushFileRangeRequest request, int directIoBlockSize) const;

private:
    template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
    std::vector<TSlicedRequest> SliceRequest(const TInputRequest& request, int directIoBlockSize, TSliceHandler handler) const;

private:
    const i64 DesiredRequestSize_;
    const i64 MinRequestSize_;
    const bool EnableSlicing_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
