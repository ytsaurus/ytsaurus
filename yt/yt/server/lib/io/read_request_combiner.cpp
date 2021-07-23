#include "read_request_combiner.h"

#include <yt/yt/core/ytalloc/memory_zone.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

using namespace NYTAlloc;

namespace {

////////////////////////////////////////////////////////////////////////////////

TReadRequestCombiner::TIORequest AlignRequest(
    TReadRequestCombiner::TIORequest request,
    i64 pageSize)
{
    if (request.Handle->IsOpenForDirectIO()) {
        i64 offset = AlignDown(request.Offset, pageSize);
        request.Size = AlignUp(request.Offset + request.Size, pageSize) - offset;
        request.Offset = offset;
    }
    return request;
}

bool TryCollapseRequest(
    TReadRequestCombiner::TIORequest* collapsed,
    const TReadRequestCombiner::TIORequest& current)
{
    YT_VERIFY(collapsed);
    if (collapsed->Handle != current.Handle) {
        return false;
    }

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

void TReadRequestCombiner::Combine(
    const std::vector<IIOEngine::TReadRequest>& requests,
    i64 pageSize,
    EMemoryZone memoryZone,
    TRefCountedTypeCookie tagCookie)
{
    if (requests.empty()) {
        return;
    }

    IORequests_.reserve(requests.size());
    for (int index = 0; index < std::ssize(requests); ++index) {
        IORequests_.push_back({
            .Handle = requests[index].Handle,
            .Offset = requests[index].Offset,
            .Size = requests[index].Size,
            .Index = index,
        });
    }

    std::sort(
        IORequests_.begin(),
        IORequests_.end(),
        [] (const TIORequest& lhs, const TIORequest& rhs) {
            if (lhs.Handle == rhs.Handle) {
                return lhs.Offset < rhs.Offset;
            } else if (lhs.Handle->IsOpenForDirectIO() != rhs.Handle->IsOpenForDirectIO()) {
                // Make sure direct IO requests precede non-direct IO to make alignment simpler.
                return lhs.Handle->IsOpenForDirectIO() > rhs.Handle->IsOpenForDirectIO();
            } else {
                return lhs.Handle < rhs.Handle;
            }
        });

    i64 totalSize = 0;
    {
        TIORequest collapsed;
        for (const auto& current : IORequests_) {
            auto aligned = AlignRequest(current, pageSize);
            if (!TryCollapseRequest(&collapsed, aligned)) {
                totalSize += collapsed.Size;
                collapsed = aligned;
            }
        }
        totalSize += collapsed.Size;
    }

    {
        TSharedMutableRef buffer;
        {
            TMemoryZoneGuard zoneGuard(memoryZone);
            buffer = TSharedMutableRef::AllocatePageAligned(totalSize, false, tagCookie);
        }
        OutputRefs_.resize(IORequests_.size());

        TIORequest collapsed;
        int outIndex = -1;
        for (const auto& current : IORequests_) {
            auto aligned = AlignRequest(current, pageSize);
            if (outIndex < 0) {
                collapsed = aligned;
                outIndex = 0;
            } else if (!TryCollapseRequest(&collapsed, aligned)) {
                collapsed.ResultBuffer = buffer.Slice(0, collapsed.Size);
                IORequests_[outIndex] = collapsed;
                ++outIndex;
                buffer = buffer.Slice(collapsed.Size, buffer.Size());
                collapsed = aligned;
            }

            i64 bufferOffset = current.Offset - collapsed.Offset;
            auto outputRef = buffer.Slice(bufferOffset, bufferOffset + current.Size);
            OutputRefs_[current.Index] = outputRef;

// Workaround for memory sanitizer as it does not seem to play well with buffers filled by uring.
#ifdef _msan_enabled_
            std::fill(outputRef.Begin(), outputRef.End(), 0);
#endif // _msan_enabled_
        }
        collapsed.ResultBuffer = buffer.Slice(0, collapsed.Size);
        IORequests_[outIndex] = collapsed;
        IORequests_.resize(outIndex + 1);
    }
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

const std::vector<TReadRequestCombiner::TIORequest>& TReadRequestCombiner::GetIORequests()
{
    return IORequests_;
}

const std::vector<TSharedRef>& TReadRequestCombiner::GetOutputBuffers()
{
    return OutputRefs_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
