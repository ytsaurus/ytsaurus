#include "io_request_slicer.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

i64 GetByteCount(const IIOEngine::TWriteRequest& request)
{
    return GetByteSize(request.Buffers);
}

template <typename TRequest>
i64 GetByteCount(const TRequest& request)
{
    return request.Size;
}

class TBuffersIterator
{
public:
    explicit TBuffersIterator(const std::vector<TSharedRef>& buffers)
        : Buffers_(buffers)
    { }

    std::vector<TSharedRef> Take(i64 desired)
    {
        std::vector<TSharedRef> result;
        while (desired > 0 && Index_ < std::ssize(Buffers_)) {
            i64 currentBufferRemainder = std::ssize(Buffers_[Index_]) - Offset_;
            i64 sliceSize = std::min(desired, currentBufferRemainder);
            result.push_back(Buffers_[Index_].Slice(Offset_, Offset_ + sliceSize));
            desired -= sliceSize;
            Offset_ += sliceSize;
            if (Offset_ == std::ssize(Buffers_[Index_])) {
                ++Index_;
                Offset_ = 0;
            }
        }
        return result;
    }

private:
    const std::vector<TSharedRef>& Buffers_;
    i64 Index_ = 0;
    i64 Offset_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

TIORequestSlicer::TIORequestSlicer(i64 desiredSize, i64 minSize)
    : DesiredRequestSize_(desiredSize)
    , MinRequestSize_(minSize)
{ }

std::vector<TSlicedReadRequest> TIORequestSlicer::Slice(
    IIOEngine::TReadRequest request,
    TMutableRef buffer) const
{
    YT_VERIFY(std::ssize(buffer) >= request.Size);
    return SliceRequest<TSlicedReadRequest>(request, [&] (TSlicedReadRequest& slice, i64 offset, i64 sliceSize) {
        auto bufferOffset = offset - request.Offset;
        slice.Request.Offset = offset;
        slice.Request.Handle = request.Handle;
        slice.Request.Size = sliceSize;
        slice.OutputBuffer = buffer.Slice(bufferOffset, bufferOffset + sliceSize);
    });
}

std::vector<IIOEngine::TWriteRequest> TIORequestSlicer::Slice(IIOEngine::TWriteRequest request) const
{
    NDetail::TBuffersIterator iterator(request.Buffers);
    return SliceRequest<IIOEngine::TWriteRequest>(request, [&] (IIOEngine::TWriteRequest& slice, i64 offset, i64 sliceSize) {
        slice.Offset = offset;
        slice.Handle = request.Handle;
        slice.Flush = request.Flush;
        slice.Buffers = iterator.Take(sliceSize);
    });
}

std::vector<IIOEngine::TFlushFileRangeRequest> TIORequestSlicer::Slice(IIOEngine::TFlushFileRangeRequest request) const
{
    return SliceRequest<IIOEngine::TFlushFileRangeRequest>(request, [&] (IIOEngine::TFlushFileRangeRequest& slice, i64 offset, i64 sliceSize) {
        slice.Handle = request.Handle;
        slice.Offset = offset;
        slice.Size = sliceSize;
    });
}

template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
std::vector<TSlicedRequest> TIORequestSlicer::SliceRequest(const TInputRequest& request, TSliceHandler handleSlice) const
{
    i64 offset = request.Offset;
    i64 remainingSize = NDetail::GetByteCount(request);
    const i64 indivisibleBlockSize = DesiredRequestSize_ + MinRequestSize_;

    std::vector<TSlicedRequest> results;
    while (remainingSize > 0) {
        auto sliceSize = (remainingSize > indivisibleBlockSize) ?  DesiredRequestSize_ : remainingSize;
        auto& slice = results.emplace_back();
        handleSlice(slice, offset, sliceSize);
        remainingSize -= sliceSize;
        offset += sliceSize;
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

TDummyRequestSlicer::TDummyRequestSlicer(i64 /*desiredSize*/, i64 /*minSize*/)
{ }

std::array<TSlicedReadRequest, 1> TDummyRequestSlicer::Slice(IIOEngine::TReadRequest request, TMutableRef buffer) const
{
    return {
        TSlicedReadRequest{
            .Request = std::move(request),
            .OutputBuffer = std::move(buffer)
        }
    };
}

std::array<IIOEngine::TWriteRequest, 1> TDummyRequestSlicer::Slice(IIOEngine::TWriteRequest request) const
{
    return {std::move(request)};
}

std::array<IIOEngine::TFlushFileRangeRequest, 1> TDummyRequestSlicer::Slice(IIOEngine::TFlushFileRangeRequest request) const
{
    return {std::move(request)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
