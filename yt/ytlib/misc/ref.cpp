#include "stdafx.h"
#include "ref.h"
#include "ref_counted_tracker.h"

#include <util/stream/str.h>

#include <util/system/info.h>

#include <util/ysaveload.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

size_t RoundUpToPage(size_t bytes)
{
    static const size_t PageSize = NSystemInfo::GetPageSize();
    YASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

void AppendToBlob(TBlob& blob, const void* buffer, size_t size)
{
    blob.resize(blob.size() + size);
    ::memcpy(&*(blob.end() - size), buffer, size);
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef::TData::TData(TBlob&& blob)
#ifdef ENABLE_REF_COUNTED_TRACKING
    : Cookie(nullptr)
#endif
{
    Blob.swap(blob);
}

TSharedRef::TData::~TData()
{
#ifdef ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING

void TSharedRef::TData::InitializeTracking(void* cookie)
{
    YASSERT(!Cookie);
    Cookie = cookie;
    TRefCountedTracker::Get()->Allocate(Cookie, Blob.size());
}

void TSharedRef::TData::FinalizeTracking()
{
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Free(Cookie, Blob.size());
}

#endif

////////////////////////////////////////////////////////////////////////////////

TSharedRef TSharedRef::AllocateImpl(size_t size)
{
    TBlob blob(size);
    return FromBlobImpl(std::move(blob));
}

TSharedRef TSharedRef::FromBlobImpl(TBlob&& blob)
{
    auto ref = TRef::FromBlob(blob);
    auto data = New<TData>(std::move(blob));
    return TSharedRef(data, ref);
}

void Save(TOutputStream* output, const NYT::TSharedRef& ref)
{
    if (ref == TSharedRef()) {
        ::Save(output, static_cast<i64>(-1));
    } else {
        ::Save(output, static_cast<i64>(ref.Size()));
        output->Write(ref.Begin(), ref.Size());
    }
}

void Load(TInputStream* input, NYT::TSharedRef& ref)
{
    i64 size;
    ::Load(input, size);
    if (size == -1) {
        ref = NYT::TSharedRef();
    } else {
        YASSERT(size >= 0);
        struct TLoadedBlockTag { };
        ref = TSharedRef::Allocate<TLoadedBlockTag>(size);
        YCHECK(input->Load(ref.Begin(), size) == size);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
