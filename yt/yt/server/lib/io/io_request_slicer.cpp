#include "io_request_slicer.h"


namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

i64 GetBytesCount(const IIOEngine::TWriteRequest& request)
{
    i64 bytes = 0;
    for (const auto& buffer : request.Buffers) {
        bytes += buffer.size();
    }
    return bytes;
}

template <typename TRequest>
i64 GetBytesCount(const TRequest& request)
{
    return request.Size;
}

class TBufferstIterator {
public:
    explicit TBufferstIterator(const std::vector<TSharedRef>& buffers)
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

TIoRequestSlicer::TIoRequestSlicer(i64 desiredSize, i64 minSize)
    : DesiredRequestSize_(desiredSize)
    , MinRequestSize_(minSize)
{ }

std::vector<TSlicedReadRequest> TIoRequestSlicer::Slice(
    IIOEngine::TReadRequest request,
    TMutableRef buffer)
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

std::vector<IIOEngine::TWriteRequest> TIoRequestSlicer::Slice(IIOEngine::TWriteRequest request)
{
    NDetail::TBufferstIterator iterator(request.Buffers);
    return SliceRequest<IIOEngine::TWriteRequest>(request, [&] (IIOEngine::TWriteRequest& slice, i64 offset, i64 sliceSize) {
        slice.Offset = offset;
        slice.Handle = request.Handle; 
        slice.Buffers = iterator.Take(sliceSize);
    });
}

std::vector<IIOEngine::TFlushFileRangeRequest> TIoRequestSlicer::Slice(IIOEngine::TFlushFileRangeRequest request)
{
    return SliceRequest<IIOEngine::TFlushFileRangeRequest>(request, [&] (IIOEngine::TFlushFileRangeRequest& slice, i64 offset, i64 sliceSize) {
        slice.Handle = request.Handle;
        slice.Offset = offset; 
        slice.Size = sliceSize;
    });
}

template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
std::vector<TSlicedRequest> TIoRequestSlicer::SliceRequest(const TInputRequest& request, TSliceHandler handleSlice)
{
    i64 offset = request.Offset;
    i64 remainingSize = NDetail::GetBytesCount(request);
    std::vector<TSlicedRequest> results;
    const auto indivisibleBlockSize = DesiredRequestSize_ + MinRequestSize_;

    while (remainingSize) {
        auto sliceSize = (remainingSize > indivisibleBlockSize) ?  DesiredRequestSize_ : remainingSize;
        auto& slice = results.emplace_back();
        handleSlice(slice, offset, sliceSize);
        remainingSize -= sliceSize;
        offset += sliceSize;
    }

    return results;
}

std::vector<TSlicedReadRequest> TDummyRequestSlicer::Slice(IIOEngine::TReadRequest request, TMutableRef buffer)
{
    return {
        TSlicedReadRequest{
            .Request = std::move(request),
            .OutputBuffer = std::move(buffer)
        }
    };
}

std::vector<IIOEngine::TWriteRequest> TDummyRequestSlicer::Slice(IIOEngine::TWriteRequest request)
{
    return {std::move(request)};
}

std::vector<IIOEngine::TFlushFileRangeRequest> TDummyRequestSlicer::Slice(IIOEngine::TFlushFileRangeRequest request)
{
    return {std::move(request)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
