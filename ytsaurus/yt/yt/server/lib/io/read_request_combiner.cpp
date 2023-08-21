#include "read_request_combiner.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TIORequest
{
    i64 Offset = 0;
    i64 Size = 0;
    int Index = 0;
};

template<typename TRequest>
i64 AlignRequest(TRequest* request, i64 pageSize)
{
    auto originalOffset = request->Offset;
    i64 offset = AlignDown(request->Offset, pageSize);
    request->Size = AlignUp(request->Offset + request->Size, pageSize) - offset;
    request->Offset = offset;
    return originalOffset - request->Offset;
}

bool TryCollapseRequest(TIORequest* collapsed, const TIORequest& current)
{
    YT_VERIFY(collapsed->Offset <= current.Offset);
    if (collapsed->Offset + collapsed->Size < current.Offset) {
        return false;
    }

    collapsed->Size = Max(collapsed->Size, current.Offset + current.Size - collapsed->Offset);
    return true;
}

} // namespace

class TReadRequestCombiner
    : public IReadRequestCombiner
{
public:
    std::vector<IReadRequestCombiner::TCombinedRequest> Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        TRefCountedTypeCookie tagCookie) override
    {
        if (requests.empty()) {
            return {};
        }

        std::vector<TIORequest> ioRequests;
        ioRequests.reserve(requests.size());

        for (int index = 0; index < std::ssize(requests); ++index) {
            ioRequests.push_back({
                .Offset = requests[index].Offset,
                .Size = requests[index].Size,
                .Index = index
            });

            if (requests[index].Handle->IsOpenForDirectIO()) {
                AlignRequest(&ioRequests.back(), pageSize);
            }
        }

        std::sort(
            ioRequests.begin(),
            ioRequests.end(),
            [&] (const TIORequest& lhs, const TIORequest& rhs) {
                const auto& lhsHandle = requests[lhs.Index].Handle;
                const auto& rhsHandle = requests[rhs.Index].Handle;

                if (lhsHandle == rhsHandle) {
                    return lhs.Offset < rhs.Offset;
                } else if (lhsHandle->IsOpenForDirectIO() != rhsHandle->IsOpenForDirectIO()) {
                    // Make sure direct IO requests precede non-direct IO to make alignment simpler.
                    return lhsHandle->IsOpenForDirectIO() > rhsHandle->IsOpenForDirectIO();
                } else {
                    return lhsHandle < rhsHandle;
                }
            });

        i64 totalSize = 0;
        {
            TIORequest collapsed;
            for (const auto& current : ioRequests) {
                if (requests[collapsed.Index].Handle != requests[current.Index].Handle ||
                    !TryCollapseRequest(&collapsed, current))
                {
                    totalSize += collapsed.Size;
                    collapsed = current;
                }
            }
            totalSize += collapsed.Size;
        }

        std::vector<TCombinedRequest> results;
        results.reserve(ioRequests.size());

        {
            auto buffer = TSharedMutableRef::AllocatePageAligned(totalSize, {.InitializeStorage = false}, tagCookie);

            OutputRefs_.resize(ioRequests.size());

            std::optional<TIORequest> collapsed;
            i64 globalBufferOffset = 0;
            for (const auto& current : ioRequests) {
                if (!collapsed) {
                    collapsed = current;
                } else if (
                    requests[collapsed->Index].Handle != requests[current.Index].Handle ||
                    !TryCollapseRequest(&collapsed.value(), current))
                {
                    auto resultBuffer = TMutableRef(buffer).Slice(
                        globalBufferOffset,
                        globalBufferOffset + collapsed->Size);

                    PushResult(&results, resultBuffer, requests[collapsed->Index], *collapsed);
                    globalBufferOffset += collapsed->Size;
                    collapsed = current;
                }

                i64 bufferOffset = globalBufferOffset + requests[current.Index].Offset - collapsed->Offset;
                auto outputBuffer = buffer.Slice(
                    bufferOffset,
                    bufferOffset + requests[current.Index].Size);

    // Workaround for memory sanitizer as it does not seem to play well with buffers filled by uring.
    #ifdef _msan_enabled_
                std::fill(outputBuffer.Begin(), outputBuffer.End(), 0);
    #endif // _msan_enabled_

                OutputRefs_[current.Index] = std::move(outputBuffer);
            }

            auto resultBuffer = TMutableRef(buffer).Slice(
                globalBufferOffset,
                globalBufferOffset + collapsed->Size);

            PushResult(&results, resultBuffer, requests[collapsed->Index], *collapsed);
        }

        return results;
    }

    void PushResult(
        std::vector<TCombinedRequest>* results,
        TMutableRef buffer,
        const IIOEngine::TReadRequest& original,
        const TIORequest& combined)
    {
        auto& resultRequest = results->emplace_back();

        resultRequest.ResultBuffer = std::move(buffer);
        resultRequest.ReadRequest = original;
        resultRequest.ReadRequest.Offset = combined.Offset;
        resultRequest.ReadRequest.Size = combined.Size;
    }

    TError CheckEof(const TMutableRef bufferTail) const override
    {
        if (bufferTail.Empty()) {
            return {};
        }

        auto it = std::partition_point(
            OutputRefs_.begin(),
            OutputRefs_.end(),
            [&] (const TSharedRef& outBuffer) {
                return outBuffer.Begin() + outBuffer.Size() <= bufferTail.Begin();
            });

        if (it == OutputRefs_.end() || it->Begin() >= bufferTail.Begin() + bufferTail.Size()) {
            return {};
        }

        return TError(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request");
    }

    std::vector<TSharedRef>&& ReleaseOutputBuffers() override
    {
        return std::move(OutputRefs_);
    }

private:
    std::vector<TSharedRef> OutputRefs_;
};

IReadRequestCombinerPtr CreateReadRequestCombiner()
{
    return New<TReadRequestCombiner>();
}

////////////////////////////////////////////////////////////////////////////////

class TDummyReadRequestCombiner
    : public IReadRequestCombiner
{
public:
    std::vector<TCombinedRequest> Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        TRefCountedTypeCookie tagCookie) override
    {
        std::vector<TCombinedRequest> ioRequests;
        ioRequests.reserve(requests.size());

        OutputRefs_.reserve(ioRequests.size());

        for (int index = 0; index < std::ssize(requests); ++index) {
            ioRequests.push_back({
                .ReadRequest = requests[index]
            });

            auto& resultRequest = ioRequests.back();

            resultRequest.ReadRequest.Offset = requests[index].Offset;
            resultRequest.ReadRequest.Size = requests[index].Size;

            i64 offsetDiff = 0;

            if (requests[index].Handle->IsOpenForDirectIO()) {
                offsetDiff = AlignRequest(&resultRequest.ReadRequest, pageSize);
            }

            auto buffer = TSharedMutableRef::AllocatePageAligned(
                resultRequest.ReadRequest.Size,
                {.InitializeStorage = false},
                tagCookie);

            resultRequest.ResultBuffer = buffer;
            OutputRefs_.push_back(buffer.Slice(offsetDiff, offsetDiff + requests[index].Size));
        }

        return ioRequests;
    }

    TError CheckEof(const TMutableRef bufferTail) const override
    {
        if (bufferTail.Empty()) {
            return {};
        }

        bool noOverlap = std::all_of(OutputRefs_.begin(), OutputRefs_.end(), [&] (const TSharedRef& outBuffer) {
            return CheckNoOverlap(bufferTail, outBuffer);
        });

        if (noOverlap) {
            return {};
        }

        return TError(NFS::EErrorCode::IOError, "Unexpected end-of-file in read request");
    }

    static bool CheckNoOverlap(TRef left, TRef right)
    {
        return left.End() <= right.Begin() || left.Begin() >= right.End();
    }

    std::vector<TSharedRef>&& ReleaseOutputBuffers() override
    {
        return std::move(OutputRefs_);
    }

private:
    std::vector<TSharedRef> OutputRefs_;
};

IReadRequestCombinerPtr CreateDummyReadRequestCombiner()
{
    return New<TDummyReadRequestCombiner>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
