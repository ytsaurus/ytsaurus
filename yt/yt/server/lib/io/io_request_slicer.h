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

    std::vector<TSlicedReadRequest> Slice(TReadRequest request, const TSharedMutableRef& buffer) const;

    std::vector<TWriteRequest> Slice(TWriteRequest request) const;
    std::vector<TFlushFileRangeRequest> Slice(TFlushFileRangeRequest request) const;

private:
    template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
    std::vector<TSlicedRequest> SliceRequest(const TInputRequest& request, TSliceHandler handler) const;

private:
    const i64 DesiredRequestSize_;
    const i64 MinRequestSize_;
    const bool EnableSlicing_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
