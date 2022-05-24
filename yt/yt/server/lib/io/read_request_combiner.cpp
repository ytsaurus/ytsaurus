#include "read_request_combiner.h"

#include <yt/yt/core/ytalloc/memory_zone.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

using namespace NYTAlloc;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const bool DontInitializeMemory = false;

////////////////////////////////////////////////////////////////////////////////

i64 AlignRequest(
    IReadRequestCombiner::TIORequest* request,
    i64 pageSize)
{
    auto originalOffset = request->Offset;
    i64 offset = AlignDown(request->Offset, pageSize);
    request->Size = AlignUp(request->Offset + request->Size, pageSize) - offset;
    request->Offset = offset;
    return originalOffset - request->Offset;
}

bool TryCollapseRequest(
    IReadRequestCombiner::TIORequest* collapsed,
    const IReadRequestCombiner::TIORequest& current)
{
    YT_VERIFY(collapsed->Offset <= current.Offset);
    if (collapsed->Offset + collapsed->Size < current.Offset) {
        return false;
    }

    collapsed->Size = Max(collapsed->Size, current.Offset + current.Size - collapsed->Offset);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TReadRequestCombiner
    : public IReadRequestCombiner
{
public:
    TCombineResult Combine(
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
        int collapsedRequestCount = 0;
        {
            TIORequest collapsed;
            for (const auto& current : ioRequests) {
                if (requests[collapsed.Index].Handle != requests[current.Index].Handle ||
                    !TryCollapseRequest(&collapsed, current))
                {
                    totalSize += collapsed.Size;
                    collapsed = current;
                    ++collapsedRequestCount;
                }
            }
            totalSize += collapsed.Size;
            ++collapsedRequestCount;
        }

        std::vector<TIOEngineHandlePtr> handles;
        handles.reserve(collapsedRequestCount);

        {
            TSharedMutableRef buffer = TSharedMutableRef::AllocatePageAligned(totalSize, DontInitializeMemory, tagCookie);

            OutputRefs_.resize(ioRequests.size());

            TIORequest collapsed;
            int outIndex = -1;
            i64 globalBufferOffset = 0;
            for (const auto& current : ioRequests) {
                if (outIndex == -1) {
                    collapsed = current;
                    outIndex = 0;
                } else if (
                    requests[collapsed.Index].Handle != requests[current.Index].Handle ||
                    !TryCollapseRequest(&collapsed, current))
                {
                    collapsed.ResultBuffer = TMutableRef(buffer).Slice(
                        globalBufferOffset,
                        globalBufferOffset + collapsed.Size);
                    ioRequests[outIndex] = collapsed;
                    ++outIndex;
                    handles.push_back(std::move(requests[collapsed.Index].Handle));
                    globalBufferOffset += collapsed.Size;
                    collapsed = current;
                }

                i64 bufferOffset = globalBufferOffset + requests[current.Index].Offset - collapsed.Offset;
                auto outputBuffer = buffer.Slice(
                    bufferOffset,
                    bufferOffset + requests[current.Index].Size);

    // Workaround for memory sanitizer as it does not seem to play well with buffers filled by uring.
    #ifdef _msan_enabled_
                std::fill(outputBuffer.Begin(), outputBuffer.End(), 0);
    #endif // _msan_enabled_

                OutputRefs_[current.Index] = std::move(outputBuffer);
            }

            collapsed.ResultBuffer = TMutableRef(buffer).Slice(
                globalBufferOffset,
                globalBufferOffset + collapsed.Size);
            ioRequests[outIndex] = collapsed;
            ioRequests.resize(outIndex + 1);
            handles.push_back(std::move(requests[collapsed.Index].Handle));
        }

        return {
            std::move(handles),
            std::move(ioRequests)
        };
    }

    TError CheckEof(const TMutableRef& bufferTail) override
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
    return std::make_unique<TReadRequestCombiner>();
}

////////////////////////////////////////////////////////////////////////////////

class TDummyReadRequestCombiner
    : public IReadRequestCombiner
{
public:
    TCombineResult Combine(
        std::vector<IIOEngine::TReadRequest> requests,
        i64 pageSize,
        TRefCountedTypeCookie tagCookie) override
    {
        std::vector<TIORequest> ioRequests;
        ioRequests.reserve(requests.size());

        std::vector<TIOEngineHandlePtr> handles;
        handles.reserve(requests.size());

        OutputRefs_.reserve(ioRequests.size());

        for (int index = 0; index < std::ssize(requests); ++index) {
            ioRequests.push_back({
                .Offset = requests[index].Offset,
                .Size = requests[index].Size,
                .Index = index
            });

            i64 offsetDiff = 0;

            if (requests[index].Handle->IsOpenForDirectIO()) {
                offsetDiff = AlignRequest(&ioRequests.back(), pageSize);
            }

            handles.push_back(requests[index].Handle);

            auto& ioRequest = ioRequests.back();
            TSharedMutableRef buffer = TSharedMutableRef::AllocatePageAligned(ioRequest.Size, DontInitializeMemory, tagCookie);
            ioRequest.ResultBuffer = buffer;
            OutputRefs_.push_back(std::move(buffer.Slice(offsetDiff, offsetDiff + requests[index].Size)));
        }

        return {
            std::move(handles),
            std::move(ioRequests)
        };
    }

    TError CheckEof(const TMutableRef& bufferTail) override
    {
        if (bufferTail.Empty()) {
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

IReadRequestCombinerPtr CreateDummyReadRequestCombiner()
{
    return std::make_unique<TDummyReadRequestCombiner>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
