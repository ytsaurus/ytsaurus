#pragma once
#ifndef REF_INL_H_
#error "Direct inclusion of this file is not allowed, include ref.h"
// For the sake of sane code completion.
#include "ref.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TRef::TRef(const void* data, size_t size)
    : TRange<char>(static_cast<const char*>(data), size)
{ }

Y_FORCE_INLINE TRef::TRef(const void* begin, const void* end)
    : TRange<char>(static_cast<const char*>(begin), static_cast<const char*>(end))
{ }

Y_FORCE_INLINE TRef TRef::FromBlob(const TBlob& blob)
{
    return TRef(blob.Begin(), blob.Size());
}

Y_FORCE_INLINE TRef TRef::FromString(const TString& str)
{
    return TRef(str.data(), str.length());
}

template <class T>
Y_FORCE_INLINE TRef TRef::FromPod(const T& data)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_pod<T>::value, "T must be a pod-type.");
    return TRef(&data, sizeof (data));
}

Y_FORCE_INLINE TRef TRef::Slice(size_t startOffset, size_t endOffset) const
{
    Y_ASSERT(endOffset >= startOffset && endOffset <= Size());
    return TRef(Begin() + startOffset, endOffset - startOffset);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TMutableRef::TMutableRef(void* data, size_t size)
    : TMutableRange<char>(static_cast<char*>(data), size)
{ }

Y_FORCE_INLINE TMutableRef::TMutableRef(void* begin, void* end)
    : TMutableRange<char>(static_cast<char*>(begin), static_cast<char*>(end))
{ }

Y_FORCE_INLINE TMutableRef::operator TRef() const
{
    return TRef(Begin(), Size());
}

Y_FORCE_INLINE TMutableRef TMutableRef::FromBlob(TBlob& blob)
{
    return TMutableRef(blob.Begin(), blob.Size());
}

template <class T>
Y_FORCE_INLINE TMutableRef TMutableRef::FromPod(T& data)
{
    static_assert(TTypeTraits<T>::IsPod || std::is_pod<T>::value, "T must be a pod-type.");
    return TMutableRef(&data, sizeof (data));
}

Y_FORCE_INLINE TMutableRef TMutableRef::FromString(TString& str)
{
    // NB: begin() invokes CloneIfShared().
    return TMutableRef(str.begin(), str.length());
}

Y_FORCE_INLINE TMutableRef TMutableRef::Slice(size_t startOffset, size_t endOffset) const
{
    Y_ASSERT(endOffset >= startOffset && endOffset <= Size());
    return TMutableRef(Begin() + startOffset, endOffset - startOffset);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TSharedRef::TSharedRef(TRef ref, TSharedRange<char>::THolderPtr holder)
    : TSharedRange<char>(ref, std::move(holder))
{ }

Y_FORCE_INLINE TSharedRef::TSharedRef(const void* data, size_t length, TSharedRange<char>::THolderPtr holder)
    : TSharedRange<char>(static_cast<const char*>(data), length, std::move(holder))
{ }

Y_FORCE_INLINE TSharedRef::TSharedRef(const void* begin, const void* end, TSharedRange<char>::THolderPtr holder)
    : TSharedRange<char>(static_cast<const char*>(begin), static_cast<const char*>(end), std::move(holder))
{ }

Y_FORCE_INLINE TSharedRef::operator TRef() const
{
    return TRef(Begin(), Size());
}

template <class TTag>
Y_FORCE_INLINE TSharedRef TSharedRef::FromString(TString str)
{
    return FromString(std::move(str), GetRefCountedTypeCookie<TTag>());
}

Y_FORCE_INLINE TSharedRef TSharedRef::FromString(TString str)
{
    return FromString<TDefaultSharedBlobTag>(std::move(str));
}

template <class TTag>
Y_FORCE_INLINE TSharedRef TSharedRef::MakeCopy(TRef ref)
{
    return MakeCopy(ref, GetRefCountedTypeCookie<TTag>());
}

Y_FORCE_INLINE TSharedRef TSharedRef::Slice(size_t startOffset, size_t endOffset) const
{
    Y_ASSERT(endOffset >= startOffset && endOffset <= Size());
    return TSharedRef(Begin() + startOffset, endOffset - startOffset, Holder_);
}

Y_FORCE_INLINE  TSharedRef TSharedRef::Slice(const void* begin, const void* end) const
{
    Y_ASSERT(begin >= Begin());
    Y_ASSERT(end <= End());
    return TSharedRef(begin, end, Holder_);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TSharedMutableRef::TSharedMutableRef(const TMutableRef& ref, TSharedMutableRange<char>::THolderPtr holder)
    : TSharedMutableRange<char>(ref, std::move(holder))
{ }

Y_FORCE_INLINE TSharedMutableRef::TSharedMutableRef(void* data, size_t length, TSharedMutableRange<char>::THolderPtr holder)
    : TSharedMutableRange<char>(static_cast<char*>(data), length, std::move(holder))
{ }

Y_FORCE_INLINE TSharedMutableRef::TSharedMutableRef(void* begin, void* end, TSharedMutableRange<char>::THolderPtr holder)
    : TSharedMutableRange<char>(static_cast<char*>(begin), static_cast<char*>(end), std::move(holder))
{ }

Y_FORCE_INLINE TSharedMutableRef::operator TMutableRef() const
{
    return TMutableRef(Begin(), Size());
}

Y_FORCE_INLINE TSharedMutableRef::operator TSharedRef() const
{
    return TSharedRef(Begin(), Size(), Holder_);
}

Y_FORCE_INLINE TSharedMutableRef::operator TRef() const
{
    return TRef(Begin(), Size());
}

Y_FORCE_INLINE TSharedMutableRef TSharedMutableRef::Allocate(size_t size, bool initializeStorage)
{
    return Allocate<TDefaultSharedBlobTag>(size, initializeStorage);
}

template <class TTag>
Y_FORCE_INLINE TSharedMutableRef TSharedMutableRef::MakeCopy(TRef ref)
{
    return MakeCopy(ref, GetRefCountedTypeCookie<TTag>());
}

Y_FORCE_INLINE TSharedMutableRef TSharedMutableRef::Slice(size_t startOffset, size_t endOffset) const
{
    Y_ASSERT(endOffset >= startOffset && endOffset <= Size());
    return TSharedMutableRef(Begin() + startOffset, endOffset - startOffset, Holder_);
}

Y_FORCE_INLINE TSharedMutableRef TSharedMutableRef::Slice(void* begin, void* end) const
{
    Y_ASSERT(begin >= Begin());
    Y_ASSERT(end <= End());
    return TSharedMutableRef(begin, end, Holder_);
}

template <class TTag>
Y_FORCE_INLINE TSharedMutableRef TSharedMutableRef::Allocate(size_t size, bool initializeStorage)
{
    return Allocate(size, initializeStorage, GetRefCountedTypeCookie<TTag>());
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE size_t GetByteSize(TRef ref)
{
    return ref ? ref.Size() : 0;
}

template <class T>
size_t GetByteSize(const std::vector<T>& parts)
{
    size_t size = 0;
    for (const auto& part : parts) {
        size += part.Size();
    }
    return size;
}

////////////////////////////////////////////////////////////////////////////////

class TSharedRefArray::TImpl
    : public TIntrinsicRefCounted
    , public TWithExtraSpace<TImpl>
{
public:
    explicit TImpl(const TSharedRef& part)
        : Size_(1)
    {
        new (MutableBegin()) TSharedRef(part);
    }

    explicit TImpl(TSharedRef&& part)
        : Size_(1)
    {
        new (MutableBegin()) TSharedRef(std::move(part));
    }

    template <class TParts>
    TImpl(const TParts& parts, TCopyParts)
        : Size_(parts.size())
    {
        for (size_t index = 0; index < Size_; ++index) {
            new (MutableBegin() + index) TSharedRef(parts[index]);
        }
    }

    template <class TParts>
    TImpl(TParts&& parts, TMoveParts)
        : Size_(parts.size())
    {
        for (size_t index = 0; index < Size_; ++index) {
            new (MutableBegin() + index) TSharedRef(std::move(parts[index]));
        }
    }

    ~TImpl()
    {
        for (size_t index = 0; index < Size_; ++index) {
            (MutableBegin() + index)->TSharedRef::~TSharedRef();
        }
    }


    size_t Size() const
    {
        return Size_;
    }

    bool Empty() const
    {
        return Size_ == 0;
    }

    const TSharedRef& operator [] (size_t index) const
    {
        Y_ASSERT(index < Size());
        return Begin()[index];
    }


    const TSharedRef* Begin() const
    {
        return static_cast<const TSharedRef*>(GetExtraSpacePtr());
    }

    const TSharedRef* End() const
    {
        return Begin() + Size_;
    }

private:
    size_t Size_;

    TSharedRef* MutableBegin()
    {
        return static_cast<TSharedRef*>(GetExtraSpacePtr());
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(TIntrusivePtr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(const TSharedRefArray& other)
    : Impl_(other.Impl_)
{ }

Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(TSharedRefArray&& other) noexcept
    : Impl_(std::move(other.Impl_))
{ }

Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(const TSharedRef& part)
    : Impl_(NewImpl(1, part))
{ }

Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(TSharedRef&& part)
    : Impl_(NewImpl(1, std::move(part)))
{ }

template <class TParts>
Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(const TParts& parts, TCopyParts)
    : Impl_(NewImpl(parts.size(), parts, TCopyParts{}))
{ }

template <class TParts>
Y_FORCE_INLINE TSharedRefArray::TSharedRefArray(TParts&& parts, TMoveParts)
    : Impl_(NewImpl(parts.size(), std::move(parts), TMoveParts{}))
{ }

Y_FORCE_INLINE TSharedRefArray& TSharedRefArray::operator=(const TSharedRefArray& other)
{
    Impl_ = other.Impl_;
    return *this;
}

Y_FORCE_INLINE TSharedRefArray& TSharedRefArray::operator=(TSharedRefArray&& other)
{
    Impl_ = std::move(other.Impl_);
    return *this;
}

Y_FORCE_INLINE void TSharedRefArray::Reset()
{
    Impl_.Reset();
}

Y_FORCE_INLINE TSharedRefArray::operator bool() const
{
    return Impl_.operator bool();
}

Y_FORCE_INLINE size_t TSharedRefArray::Size() const
{
    return Impl_ ? Impl_->Size() : 0;
}

Y_FORCE_INLINE bool TSharedRefArray::Empty() const
{
    return Impl_ ? Impl_->Empty() : true;
}

Y_FORCE_INLINE const TSharedRef& TSharedRefArray::operator[](size_t index) const
{
    Y_ASSERT(Impl_);
    return (*Impl_)[index];
}

Y_FORCE_INLINE const TSharedRef* TSharedRefArray::Begin() const
{
    return Impl_ ? Impl_->Begin() : nullptr;
}

Y_FORCE_INLINE const TSharedRef* TSharedRefArray::End() const
{
    return Impl_ ? Impl_->End() : nullptr;
}

template <class... As>
TIntrusivePtr<TSharedRefArray::TImpl> TSharedRefArray::NewImpl(size_t size, As... args)
{
    return NewWithExtraSpace<TImpl>(
        sizeof (TSharedRef) * size,
        std::forward<As>(args)...);
}

Y_FORCE_INLINE const TSharedRef* begin(const TSharedRefArray& array)
{
    return array.Begin();
}

Y_FORCE_INLINE const TSharedRef* end(const TSharedRefArray& array)
{
    return array.End();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
