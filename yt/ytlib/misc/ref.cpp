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

TSharedRef::TSharedData::TSharedData(TBlob&& blob)
    : Blob(std::move(blob))
#ifdef ENABLE_REF_COUNTED_TRACKING
    , Cookie(nullptr)
#endif
{ }

TSharedRef::TSharedData::~TSharedData()
{
#ifdef ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING

void TSharedRef::TSharedData::InitializeTracking(void* cookie)
{
    YASSERT(!Cookie);
    Cookie = cookie;
    TRefCountedTracker::Get()->Allocate(Cookie, Blob.Capacity());
}

void TSharedRef::TSharedData::FinalizeTracking()
{
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Free(Cookie, Blob.Capacity());
}

#endif

////////////////////////////////////////////////////////////////////////////////

TSharedRef TSharedRef::AllocateImpl(size_t size, bool initializeStorage)
{
    TBlob blob(size, initializeStorage);
    return FromBlobImpl(std::move(blob));
}

TSharedRef TSharedRef::FromBlobImpl(TBlob&& blob)
{
    auto ref = TRef::FromBlob(blob);
    auto data = New<TSharedData>(std::move(blob));
    return TSharedRef(data, ref);
}

Stroka ToString(const TRef& ref)
{
    return Stroka(ref.Begin(), ref.End());
}

////////////////////////////////////////////////////////////////////////////////

size_t RoundUpToPage(size_t bytes)
{
    static const size_t PageSize = NSystemInfo::GetPageSize();
    YASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
