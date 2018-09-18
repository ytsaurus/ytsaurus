#include "ref.h"
#include "serialize.h"
#include "small_vector.h"

#include <util/system/info.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const char EmptyRefData[0] = {};
const TRef EmptyRef(EmptyRefData, static_cast<size_t>(0));
const TSharedRef EmptySharedRef(EmptyRef, nullptr);

////////////////////////////////////////////////////////////////////////////////

TSharedRef::TBlobHolder::TBlobHolder(TBlob&& blob)
    : Blob_(std::move(blob))
{ }

////////////////////////////////////////////////////////////////////////////////

TSharedMutableRef::TBlobHolder::TBlobHolder(TBlob&& blob)
    : Blob_(std::move(blob))
{ }

////////////////////////////////////////////////////////////////////////////////

TSharedMutableRef::TAllocationHolder::TAllocationHolder(
    size_t size,
    bool initializeStorage,
    TRefCountedTypeCookie cookie)
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

TSharedMutableRef::TAllocationHolder::~TAllocationHolder()
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
    TRefCountedTrackerFacade::FreeSpace(Cookie_, Size_);
#endif
}

TMutableRef TSharedMutableRef::TAllocationHolder::GetRef()
{
    return TMutableRef(GetExtraSpacePtr(), Size_);
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef::TStringHolder::TStringHolder(TString&& string, TRefCountedTypeCookie cookie)
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

TSharedRef::TStringHolder::~TStringHolder()
{
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
    TRefCountedTrackerFacade::FreeSpace(Cookie_, String_.length());
#endif
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRef& ref)
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
    Y_ASSERT((PageSize & (PageSize - 1)) == 0);
    return (bytes + PageSize - 1) & (~(PageSize - 1));
}

////////////////////////////////////////////////////////////////////////////////

class TSharedRefArray::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl() = default;

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
        Y_ASSERT(index >= 0 && index < Size());
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
    : Impl_(std::move(impl))
{ }

TSharedRefArray::TSharedRefArray()
{ }

TSharedRefArray::TSharedRefArray(const TSharedRefArray& other)
    : Impl_(other.Impl_)
{ }

TSharedRefArray::TSharedRefArray(TSharedRefArray&& other) noexcept
    : Impl_(std::move(other.Impl_))
{ }

TSharedRefArray::~TSharedRefArray()
{ }

TSharedRefArray::TSharedRefArray(const TSharedRef& part)
    : Impl_(New<TImpl>(part))
{ }

TSharedRefArray::TSharedRefArray(TSharedRef&& part)
    : Impl_(New<TImpl>(std::move(part)))
{ }

TSharedRefArray::TSharedRefArray(const std::vector<TSharedRef>& parts)
    : Impl_(New<TImpl>(parts))
{ }

TSharedRefArray::TSharedRefArray(std::vector<TSharedRef>&& parts)
    : Impl_(New<TImpl>(std::move(parts)))
{ }

TSharedRefArray& TSharedRefArray::operator=(const TSharedRefArray& other)
{
    Impl_ = other.Impl_;
    return *this;
}

TSharedRefArray& TSharedRefArray::operator=(TSharedRefArray&& other)
{
    Impl_ = std::move(other.Impl_);
    return *this;
}

void TSharedRefArray::Reset()
{
    Impl_.Reset();
}

TSharedRefArray::operator bool() const
{
    return Impl_.operator bool();
}

int TSharedRefArray::Size() const
{
    return Impl_ ? Impl_->Size() : 0;
}

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

bool TSharedRefArray::Empty() const
{
    return Impl_ ? Impl_->Empty() : true;
}

const TSharedRef& TSharedRefArray::operator[](int index) const
{
    Y_ASSERT(Impl_);
    return (*Impl_)[index];
}

const TSharedRef* TSharedRefArray::Begin() const
{
    return Impl_ ? Impl_->Begin() : nullptr;
}

const TSharedRef* TSharedRefArray::End() const
{
    return Impl_ ? Impl_->End() : nullptr;
}

TSharedRef TSharedRefArray::Pack() const
{
    return Impl_ ? Impl_->Pack() : TSharedRef();
}

TSharedRefArray TSharedRefArray::Unpack(const TSharedRef& packedRef)
{
    return TSharedRefArray(TImpl::Unpack(packedRef));
}

std::vector<TSharedRef> TSharedRefArray::ToVector() const
{
    return Impl_ ? Impl_->ToVector() : std::vector<TSharedRef>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
