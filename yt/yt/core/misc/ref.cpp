#include "ref.h"
#include "serialize.h"
#include "small_vector.h"

#include <util/system/info.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const char EmptyRefData[0] = {};
char MutableEmptyRefData[0] = {};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TBlobHolder
    : public TRefCounted
{
public:
    explicit TBlobHolder(TBlob&& blob)
        : Blob_(std::move(blob))
    { }

private:
    const TBlob Blob_;
};

////////////////////////////////////////////////////////////////////////////////

class TStringHolder
    : public TRefCounted
{
public:
    TStringHolder(TString&& string, TRefCountedTypeCookie cookie)
        : String_(std::move(string))
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        , Cookie_(cookie)
#endif
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::AllocateTagInstance(Cookie_);
        TRefCountedTrackerFacade::AllocateSpace(Cookie_, String_.length());
#endif
    }
    ~TStringHolder()
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
        TRefCountedTrackerFacade::FreeSpace(Cookie_, String_.length());
#endif
    }

private:
    const TString String_;
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    const TRefCountedTypeCookie Cookie_;
#endif
};

////////////////////////////////////////////////////////////////////////////////

class TAllocationHolder
    : public TRefCounted
    , public TWithExtraSpace<TAllocationHolder>
{
public:
    TAllocationHolder(size_t size, bool initializeStorage, TRefCountedTypeCookie cookie)
        : Size_(size)
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        , Cookie_(cookie)
#endif
    {
        if (initializeStorage) {
            ::memset(GetExtraSpacePtr(), 0, Size_);
        }
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::AllocateTagInstance(Cookie_);
        TRefCountedTrackerFacade::AllocateSpace(Cookie_, Size_);
#endif
    }
    ~TAllocationHolder()
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
        TRefCountedTrackerFacade::FreeSpace(Cookie_, Size_);
#endif
    }

    TMutableRef GetRef()
    {
        return TMutableRef(GetExtraSpacePtr(), Size_);
    }

private:
    const size_t Size_;
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    const TRefCountedTypeCookie Cookie_;
#endif
};

////////////////////////////////////////////////////////////////////////////////

bool TRef::AreBitwiseEqual(TRef lhs, TRef rhs)
{
    if (lhs.Size() != rhs.Size()) {
        return false;
    }
    if (lhs.Size() == 0) {
        return true;
    }
    return ::memcmp(lhs.Begin(), rhs.Begin(), lhs.Size()) == 0;
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef TSharedRef::FromString(TString str, TRefCountedTypeCookie tagCookie)
{
    auto ref = TRef::FromString(str);
    auto holder = New<TStringHolder>(std::move(str), tagCookie);
    return TSharedRef(ref, std::move(holder));
}

TSharedRef TSharedRef::FromBlob(TBlob&& blob)
{
    auto ref = TRef::FromBlob(blob);
    auto holder = New<TBlobHolder>(std::move(blob));
    return TSharedRef(ref, std::move(holder));
}

TSharedRef TSharedRef::MakeCopy(TRef ref, TRefCountedTypeCookie tagCookie)
{
    if (!ref) {
        return {};
    }
    if (ref.Empty()) {
        return TSharedRef::MakeEmpty();
    }
    auto result = TSharedMutableRef::Allocate(ref.Size(), false, tagCookie);
    ::memcpy(result.Begin(), ref.Begin(), ref.Size());
    return result;
}

std::vector<TSharedRef> TSharedRef::Split(size_t partSize) const
{
    YT_VERIFY(partSize > 0);
    std::vector<TSharedRef> result;
    result.reserve(Size() / partSize + 1);
    auto sliceBegin = Begin();
    while (sliceBegin < End()) {
        auto sliceEnd = sliceBegin + partSize;
        if (sliceEnd < sliceBegin || sliceEnd > End()) {
            sliceEnd = End();
        }
        result.push_back(Slice(sliceBegin, sliceEnd));
        sliceBegin = sliceEnd;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TSharedMutableRef TSharedMutableRef::Allocate(size_t size, bool initializeStorage, TRefCountedTypeCookie tagCookie)
{
    auto holder = NewWithExtraSpace<TAllocationHolder>(size, size, initializeStorage, tagCookie);
    auto ref = holder->GetRef();
    return TSharedMutableRef(ref, std::move(holder));
}

TSharedMutableRef TSharedMutableRef::FromBlob(TBlob&& blob)
{
    auto ref = TMutableRef::FromBlob(blob);
    auto holder = New<TBlobHolder>(std::move(blob));
    return TSharedMutableRef(ref, std::move(holder));
}

TSharedMutableRef TSharedMutableRef::MakeCopy(TRef ref, TRefCountedTypeCookie tagCookie)
{
    if (!ref) {
        return {};
    }
    if (ref.Empty()) {
        return TSharedMutableRef::MakeEmpty();
    }
    auto result = Allocate(ref.Size(), false, tagCookie);
    ::memcpy(result.Begin(), ref.Begin(), ref.Size());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(TRef ref)
{
    return TString(ref.Begin(), ref.End());
}

TString ToString(const TMutableRef& ref)
{
    return ToString(TRef(ref));
}

TString ToString(const TSharedRef& ref)
{
    return ToString(TRef(ref));
}

TString ToString(const TSharedMutableRef& ref)
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
    YT_ASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

size_t GetByteSize(const TSharedRefArray& array)
{
    size_t size = 0;
    if (array) {
        for (const auto& part : array) {
            size += part.Size();
        }
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

i64 TSharedRefArray::ByteSize() const
{
    i64 result = 0;
    if (*this) {
        for (const auto& part : *this) {
            result += part.Size();
        }
    }
    return result;
}

TSharedRef TSharedRefArray::Pack() const
{
    if (!Impl_) {
        return {};
    }

    return PackRefs(MakeRange(Begin(), End()));
}

TSharedRefArray TSharedRefArray::Unpack(const TSharedRef& packedRef)
{
    if (!packedRef) {
        return {};
    }

    auto parts = UnpackRefs(packedRef);
    return TSharedRefArray(NewImpl(
        static_cast<int>(parts.size()),
        0,
        GetRefCountedTypeCookie<TSharedRefArrayTag>(),
        std::move(parts),
        TMoveParts{}));
}

std::vector<TSharedRef> TSharedRefArray::ToVector() const
{
    if (!Impl_) {
        return {};
    }

    return std::vector<TSharedRef>(Begin(), End());
}

////////////////////////////////////////////////////////////////////////////////

TSharedRefArrayBuilder::TSharedRefArrayBuilder(
    size_t size,
    size_t poolCapacity,
    TRefCountedTypeCookie tagCookie)
    : AllocationCapacity_(poolCapacity)
    , Impl_(TSharedRefArray::NewImpl(
        size,
        poolCapacity,
        tagCookie,
        size))
    , CurrentAllocationPtr_(Impl_->GetBeginAllocationPtr())
{ }

void TSharedRefArrayBuilder::Add(TSharedRef part)
{
    YT_ASSERT(CurrentPartIndex_ < Impl_->Size());
    Impl_->MutableBegin()[CurrentPartIndex_++] = std::move(part);
}

TMutableRef TSharedRefArrayBuilder::AllocateAndAdd(size_t size)
{
    YT_ASSERT(CurrentPartIndex_ < Impl_->Size());
    YT_ASSERT(CurrentAllocationPtr_ + size <= Impl_->GetBeginAllocationPtr() + AllocationCapacity_);
    TMutableRef ref(CurrentAllocationPtr_, size);
    CurrentAllocationPtr_ += size;
    TRefCountedPtr holder(Impl_.Get(), false);
    TSharedRef sharedRef(ref, std::move(holder));
    Add(std::move(sharedRef));
    return ref;
}

TSharedRefArray TSharedRefArrayBuilder::Finish()
{
    return TSharedRefArray(std::move(Impl_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
