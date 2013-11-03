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
        i64 size = 0;

        size += sizeof(i32);                // array size
        size += sizeof(i64) * Parts.size(); // parts sizes
        for (const auto& part : Parts) {    // part bodies
            size += part.Size();
        }

        struct TPackedSharedRefArrayTag { };
        auto result = TSharedRef::Allocate<TPackedSharedRefArrayTag>(size, false);
        TMemoryOutput output(result.Begin(), result.Size());

        WritePod(output, static_cast<i32>(Parts.size()));
        for (const auto& part : Parts) {
            WritePod(output, static_cast<i64>(part.Size()));
            Write(output, part);
        }

        return result;
    }

    static TIntrusivePtr<TImpl> Unpack(const TSharedRef& packedRef)
    {
        if (!packedRef) {
            return nullptr;
        }

        TMemoryInput input(packedRef.Begin(), packedRef.Size());

        i32 size;
        ::Load(&input, size);
        YCHECK(size >= 0);

        auto impl = New<TImpl>(size);
        for (int index = 0; index < size; ++index) {
            i64 partSize;
            ::Load(&input, partSize);
            YCHECK(partSize >= 0);

            TRef partRef(const_cast<char*>(input.Buf()), static_cast<size_t>(partSize));
            impl->Parts[index] = packedRef.Slice(partRef);

            input.Skip(partSize);
        }

        return impl;
    }

private:
    TSmallVector<TSharedRef, 4> Parts;

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

TSharedRefArray& TSharedRefArray::operator=(const TSharedRefArray& other)
{
    Impl = other.Impl;
    return *this;
}

TSharedRefArray& TSharedRefArray::operator=(TSharedRefArray&& other)
{
    Impl = std::move(other.Impl);
    return *this;
}

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
