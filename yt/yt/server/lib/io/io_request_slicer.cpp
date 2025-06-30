#include "io_request_slicer.h"
#include "io_engine_base.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

i64 GetByteCount(const TWriteRequest& request)
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

TIORequestSlicer::TIORequestSlicer(i64 desiredSize, i64 minSize, bool enableSlicing)
    : DesiredRequestSize_(desiredSize)
    , MinRequestSize_(minSize)
    , EnableSlicing_(enableSlicing)
{ }

std::vector<TSlicedReadRequest> TIORequestSlicer::Slice(
    TReadRequest request,
    const TSharedMutableRef& buffer) const
{
    if (!EnableSlicing_) {
        return {
            TSlicedReadRequest{
                .Request = std::move(request),
                .OutputBuffer = std::move(buffer),
            }
        };
    }

    i64 blockSize = request.Handle->IsOpenForDirectIO() ? DefaultBlockSize : 1;

    YT_VERIFY(std::ssize(buffer) >= request.Size);
    return SliceRequest<TSlicedReadRequest>(request, [&] (TSlicedReadRequest& slice, i64 offset, i64 sliceSize) {
        YT_VERIFY(offset % blockSize == 0);
        YT_VERIFY(sliceSize % blockSize == 0);

        auto bufferOffset = offset - AlignDown(request.Offset, blockSize);
        slice.Request.Offset = offset;
        slice.Request.Handle = request.Handle;
        slice.Request.Size = sliceSize;
        slice.OutputBuffer = buffer.Slice(bufferOffset, bufferOffset + sliceSize);
    });
}

std::vector<TWriteRequest> TIORequestSlicer::Slice(TWriteRequest request) const
{
    if (!EnableSlicing_) {
        return {std::move(request)};
    }

    NDetail::TBuffersIterator iterator(request.Buffers);
    return SliceRequest<TWriteRequest>(request, [&] (TWriteRequest& slice, i64 offset, i64 sliceSize) {
        slice.Offset = offset;
        slice.Handle = request.Handle;
        slice.Flush = request.Flush;
        slice.Buffers = iterator.Take(sliceSize);
    });
}

std::vector<TFlushFileRangeRequest> TIORequestSlicer::Slice(TFlushFileRangeRequest request) const
{
    if (!EnableSlicing_) {
        return {std::move(request)};
    }

    return SliceRequest<TFlushFileRangeRequest>(request, [&] (TFlushFileRangeRequest& slice, i64 offset, i64 sliceSize) {
        slice.Handle = request.Handle;
        slice.Offset = offset;
        slice.Size = sliceSize;
    });
}

template <typename TSlicedRequest, typename TInputRequest, typename TSliceHandler>
std::vector<TSlicedRequest> TIORequestSlicer::SliceRequest(const TInputRequest& request, TSliceHandler handleSlice) const
{
    i64 blockSize = request.Handle->IsOpenForDirectIO() ? DefaultBlockSize : 1;
    i64 desiredSize = AlignUp(DesiredRequestSize_, blockSize);
    i64 minSize = AlignUp(MinRequestSize_, blockSize);

    i64 offset = AlignDown(request.Offset, blockSize);
    i64 remainingSize = AlignUp<i64>(request.Offset + NDetail::GetByteCount(request), blockSize) - offset;
    const i64 indivisibleBlockSize = desiredSize + minSize;

    std::vector<TSlicedRequest> results;
    while (remainingSize > 0) {
        auto sliceSize = (remainingSize >= indivisibleBlockSize) ?  desiredSize : remainingSize;
        auto& slice = results.emplace_back();
        handleSlice(slice, offset, sliceSize);
        remainingSize -= sliceSize;
        offset += sliceSize;
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
