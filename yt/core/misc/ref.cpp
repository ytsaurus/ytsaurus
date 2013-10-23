#include "stdafx.h"
#include "ref.h"
#include "ref_counted_tracker.h"

#include <util/system/info.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

bool TRef::AreBitwiseEqual(const TRef& lhs, const TRef& rhs)
{
    if (lhs.Size() != rhs.Size())
        return false;
    if (lhs.Size() == 0)
        return true;
    return memcmp(lhs.Begin(), rhs.Begin(), lhs.Size()) == 0;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef::TBlobHolder::TBlobHolder(TBlob&& blob)
    : Blob(std::move(blob))
#ifdef ENABLE_REF_COUNTED_TRACKING
    , Cookie(nullptr)
#endif
{ }

TSharedRef::TBlobHolder::~TBlobHolder()
{
#ifdef ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING

void TSharedRef::TBlobHolder::InitializeTracking(void* cookie)
{
    YASSERT(!Cookie);
    Cookie = cookie;
    TRefCountedTracker::Get()->Allocate(Cookie, Blob.Size());
}

void TSharedRef::TBlobHolder::FinalizeTracking()
{
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Free(Cookie, Blob.Size());
}

#endif

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRef& ref)
{
    return Stroka(ref.Begin(), ref.End());
}

size_t GetPageSize()
{
    static const size_t PageSize = NSystemInfo::GetPageSize();
    return PageSize;
}

size_t RoundUpToPage(size_t bytes)
{
    static const size_t PageSize = NSystemInfo::GetPageSize();
    YASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
