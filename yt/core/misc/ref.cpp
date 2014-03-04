#include "stdafx.h"
#include "ref.h"
#include "ref_counted_tracker.h"
#include "small_vector.h"
#include "serialize.h"

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

TSharedRef::TStringHolder::TStringHolder(const Stroka& string)
    : String(string)
#ifdef ENABLE_REF_COUNTED_TRACKING
    , Cookie(nullptr)
#endif
{ }

TSharedRef::TStringHolder::~TStringHolder()
{
#ifdef ENABLE_REF_COUNTED_TRACKING
    FinalizeTracking();
#endif
}

#ifdef ENABLE_REF_COUNTED_TRACKING

void TSharedRef::TStringHolder::InitializeTracking(void* cookie)
{
    YASSERT(!Cookie);
    Cookie = cookie;
    TRefCountedTracker::Get()->Allocate(Cookie, String.length());
}

void TSharedRef::TStringHolder::FinalizeTracking()
{
    YASSERT(Cookie);
    TRefCountedTracker::Get()->Free(Cookie, String.length());
}

#endif

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRef& ref)
{
    return Stroka(ref.Begin(), ref.End());
}

Stroka ToString(const TSharedRef& ref)
{
    return ToString(TRef(ref));
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

class TSharedRefArray::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl()
    { }

    explicit TImpl(int size)
        : Parts(size)
    { }

    explicit TImpl(const TSharedRef& part)
    {
        Parts.push_back(part);
    }

    explicit TImpl(TSharedRef&& part)
        : Parts(1)
    {
        Parts[0] = std::move(part);
    }

    explicit TImpl(const std::vector<TSharedRef>& parts)
        : Parts(parts.begin(), parts.end())
    { }

    explicit TImpl(std::vector<TSharedRef>&& parts)
        : Parts(parts.size())
    {
        for (int index = 0; index < static_cast<int>(parts.size()); ++index) {
            Parts[index] = std::move(parts[index]);
        }
    }


    int Size() const
    {
        return static_cast<int>(Parts.size());
    }

    bool Empty() const
    {
        return Parts.empty();
    }

    const TSharedRef& operator [] (int index) const
    {
        YASSERT(index >= 0 && index < Size());
        return Parts[index];
    }


    const TSharedRef* Begin() const
    {
        return Parts.data();
    }

    const TSharedRef* End() const
    {
        return Parts.data() + Parts.size();
    }


    std::vector<TSharedRef> ToVector() const
    {
        return std::vector<TSharedRef>(Parts.begin(), Parts.end());
    }


    TSharedRef Pack() const
    {
        return PackRefs(Parts);
    }

    static TIntrusivePtr<TImpl> Unpack(const TSharedRef& packedRef)
    {
        if (!packedRef) {
            return nullptr;
        }

        std::vector<TSharedRef> parts;
        UnpackRefs(packedRef, &parts);
        return New<TImpl>(std::move(parts));
    }

private:
    SmallVector<TSharedRef, 4> Parts;

};

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray::TSharedRefArray(TIntrusivePtr<TImpl> impl)
    : Impl(std::move(impl))
{ }

TSharedRefArray::TSharedRefArray()
{ }

TSharedRefArray::TSharedRefArray(const TSharedRefArray& other)
    : Impl(other.Impl)
{ }

TSharedRefArray::TSharedRefArray(TSharedRefArray&& other)
    : Impl(std::move(other.Impl))
{ }

TSharedRefArray::~TSharedRefArray()
{ }

TSharedRefArray::TSharedRefArray(const TSharedRef& part)
    : Impl(New<TImpl>(part))
{ }

TSharedRefArray::TSharedRefArray(TSharedRef&& part)
    : Impl(New<TImpl>(std::move(part)))
{ }

TSharedRefArray::TSharedRefArray(const std::vector<TSharedRef>& parts)
    : Impl(New<TImpl>(parts))
{ }

TSharedRefArray::TSharedRefArray(std::vector<TSharedRef>&& parts)
    : Impl(New<TImpl>(std::move(parts)))
{ }

void TSharedRefArray::Reset()
{
    Impl.Reset();
}

TSharedRefArray::operator bool() const
{
    return Impl != nullptr;
}

int TSharedRefArray::Size() const
{
    return Impl ? Impl->Size() : 0;
}

bool TSharedRefArray::Empty() const
{
    return Impl ? Impl->Empty() : true;
}

const TSharedRef& TSharedRefArray::operator[](int index) const
{
    YASSERT(Impl);
    return (*Impl)[index];
}

const TSharedRef* TSharedRefArray::Begin() const
{
    return Impl ? Impl->Begin() : nullptr;
}

const TSharedRef* TSharedRefArray::End() const
{
    return Impl ? Impl->End() : nullptr;
}

TSharedRef TSharedRefArray::Pack() const
{
    return Impl ? Impl->Pack() : TSharedRef();
}

TSharedRefArray TSharedRefArray::Unpack(const TSharedRef& packedRef)
{
    return TSharedRefArray(TImpl::Unpack(packedRef));
}

std::vector<TSharedRef> TSharedRefArray::ToVector() const
{
    return Impl ? Impl->ToVector() : std::vector<TSharedRef>();
}

const TSharedRef* begin(const TSharedRefArray& array)
{
    return array.Begin();
}

const TSharedRef* end(const TSharedRefArray& array)
{
    return array.End();
}

void swap(TSharedRefArray& lhs, TSharedRefArray& rhs)
{
    using std::swap;
    swap(lhs.Impl, rhs.Impl);
}

TSharedRefArray& TSharedRefArray::operator=(TSharedRefArray other)
{
    swap(*this, other);
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
