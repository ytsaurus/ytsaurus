#include "read_request_combiner.h"

#include <yt/yt/core/ytalloc/memory_zone.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

using namespace NYTAlloc;

namespace {

////////////////////////////////////////////////////////////////////////////////

void AlignRequest(
    TReadRequestCombiner::TIORequest* request,
    i64 pageSize)
{
    i64 offset = AlignDown(request->Offset, pageSize);
    request->Size = AlignUp(request->Offset + request->Size, pageSize) - offset;
    request->Offset = offset;
}

bool TryCollapseRequest(
    TReadRequestCombiner::TIORequest* collapsed,
    const TReadRequestCombiner::TIORequest& current)
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

TReadRequestCombiner::TCombineResult TReadRequestCombiner::Combine(
    std::vector<IIOEngine::TReadRequest> requests,
    i64 pageSize,
    EMemoryZone memoryZone,
    TRefCountedTypeCookie tagCookie)
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
        TSharedMutableRef buffer;
        {
            TMemoryZoneGuard zoneGuard(memoryZone);
            buffer = TSharedMutableRef::AllocatePageAligned(totalSize, false, tagCookie);
        }

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

TError TReadRequestCombiner::CheckEOF(const TMutableRef& bufferTail)
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

std::vector<TSharedRef>&& TReadRequestCombiner::ReleaseOutputBuffers()
{
    return std::move(OutputRefs_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
