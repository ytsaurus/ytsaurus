#pragma once

#include "common.h"
#include "range.h"
#include "blob.h"
#include "new.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A non-owning reference to a range of memory.
class TRef
    : public TRange<char>
{
public:
    //! Creates a null TRef.
    TRef()
    { }

    //! Creates a TRef for a given block of memory.
    TRef(const void* data, size_t size)
        : TRange<char>(static_cast<const char*>(data), size)
    { }

    //! Creates a TRef for a given range of memory.
    TRef(const void* begin, const void* end)
        : TRange<char>(static_cast<const char*>(begin), static_cast<const char*>(end))
    { }

    
    //! Creates a non-owning TRef for a given blob.
    static TRef FromBlob(const TBlob& blob)
    {
        return TRef(blob.Begin(), blob.Size());
    }

    //! Creates a non-owning TRef for a given string.
    static TRef FromString(const Stroka& str)
    {
        return TRef(str.data(), str.length());
    }

    //! Creates a non-owning TRef for a given pod structure.
    template <class T>
    static TRef FromPod(const T& data)
    {
        static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
        return TRef(&data, sizeof (data));
    }

    //! Creates a TRef for a part of existing range.
    TRef Slice(size_t startOffset, size_t endOffset) const
    {
        YASSERT(endOffset >= startOffset && endOffset <= Size());
        return TRef(Begin() + startOffset, endOffset - startOffset);
    }

    //! Compares the content for bitwise equality.
    static bool AreBitwiseEqual(const TRef& lhs, const TRef& rhs)
    {
        if (lhs.Size() != rhs.Size()) {
            return false;
        }
        if (lhs.Size() == 0) {
            return true;
        }
        return ::memcmp(lhs.Begin(), rhs.Begin(), lhs.Size()) == 0;
    }
};

extern const TRef EmptyRef;

////////////////////////////////////////////////////////////////////////////////

//! A non-owning reference to a mutable range of memory.
//! Use with caution :)
class TMutableRef
    : public TMutableRange<char>
{
public:
    //! Creates a null TMutableRef.
    TMutableRef()
    { }

    //! Creates a TMutableRef for a given block of memory.
    TMutableRef(void* data, size_t size)
        : TMutableRange<char>(static_cast<char*>(data), size)
    { }

    //! Creates a TMutableRef for a given range of memory.
    TMutableRef(void* begin, void* end)
        : TMutableRange<char>(static_cast<char*>(begin), static_cast<char*>(end))
    { }

    //! Converts a TMutableRef to TRef.
    operator TRef() const
    {
        return TRef(Begin(), Size());
    }


    //! Creates a non-owning TMutableRef for a given blob.
    static TMutableRef FromBlob(TBlob& blob)
    {
        return TMutableRef(blob.Begin(), blob.Size());
    }

    //! Creates a non-owning TMutableRef for a given pod structure.
    template <class T>
    static TMutableRef FromPod(T& data)
    {
        static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
        return TMutableRef(&data, sizeof (data));
    }

    //! Creates a non-owning TMutableRef for a given string.
    //! Ensures that the string is not shared.
    static TMutableRef FromString(Stroka& str)
    {
        // NB: begin() invokes CloneIfShared().
        return TMutableRef(str.begin(), str.length());
    }

    //! Creates a TMutableRef for a part of existing range.
    TMutableRef Slice(size_t startOffset, size_t endOffset) const
    {
        YASSERT(endOffset >= startOffset && endOffset <= Size());
        return TMutableRef(Begin() + startOffset, endOffset - startOffset);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Default tag type for memory blocks allocated via TSharedRef.
/*!
 *  Each newly allocated TSharedRef blob is associated with a tag type that
 *  appears in ref-counted statistics.
 */
struct TDefaultSharedBlobTag { };

//! A reference to a range of memory with shared ownership.
class TSharedRef
    : public TSharedRange<char>
{
public:
    //! Creates a null TSharedRef.
    TSharedRef()
    { }

    //! Creates a TSharedRef with a given holder.
    TSharedRef(const TRef& ref, THolderPtr holder)
        : TSharedRange<char>(ref, std::move(holder))
    { }

    //! Creates a TSharedRef from a pointer and length.
    TSharedRef(const void* data, size_t length, THolderPtr holder)
        : TSharedRange<char>(static_cast<const char*>(data), length, std::move(holder))
    { }

    //! Creates a TSharedRange from a range.
    TSharedRef(const void* begin, const void* end, THolderPtr holder)
        : TSharedRange<char>(static_cast<const char*>(begin), static_cast<const char*>(end), std::move(holder))
    { }

    //! Converts a TSharedRef to TRef.
    operator TRef() const
    {
        return TRef(Begin(), Size());
    }


    //! Creates a TSharedRef from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedRef FromString(const Stroka& str)
    {
        return FromString(str, GetRefCountedTypeCookie<TTag>());
    }

    //! Creates a TSharedRef from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with TDefaultSharedBlobTag.
    static TSharedRef FromString(const Stroka& str)
    {
        return FromString<TDefaultSharedBlobTag>(str);
    }

    //! Creates a TSharedRef reference from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with a given tag.
    static TSharedRef FromString(const Stroka& str, TRefCountedTypeCookie tagCookie)
    {
        auto ref = TRef::FromString(str);
        auto holder = New<TStringHolder>(str);
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        holder->InitializeTracking(tagCookie);
#endif
        return TSharedRef(ref, std::move(holder));
    }

    //! Creates a TSharedRef for a given blob taking ownership of its content.
    static TSharedRef FromBlob(TBlob&& blob)
    {
        auto ref = TRef::FromBlob(blob);
        auto holder = New<TBlobHolder>(std::move(blob));
        return TSharedRef(ref, std::move(holder));
    }

    //! Creates a copy of a given TRef.
    //! The memory is marked with a given tag.
    static TSharedRef MakeCopy(const TRef& ref, TRefCountedTypeCookie tagCookie)
    {
        auto blob = TBlob(tagCookie, ref.Size(), false);
        ::memcpy(blob.Begin(), ref.Begin(), ref.Size());
        return FromBlob(std::move(blob));
    }

    //! Creates a copy of a given TRef.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedRef MakeCopy(const TRef& ref)
    {
        return MakeCopy(ref, GetRefCountedTypeCookie<TTag>());
    }

    //! Creates a TSharedRef for a part of existing range.
    TSharedRef Slice(size_t startOffset, size_t endOffset) const
    {
        YASSERT(endOffset >= startOffset && endOffset <= Size());
        return TSharedRef(Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    //! Creates a TMutableRef for a part of existing range.
    TSharedRef Slice(const void* begin, const void* end) const
    {
        YASSERT(begin >= Begin());
        YASSERT(end <= End());
        return TSharedRef(begin, end, Holder_);
    }

    //! Creates a vector of slices with specified size.
    std::vector<TSharedRef> Split(size_t partSize) const
    {
        YCHECK(partSize > 0);
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

private:
    struct TBlobHolder
        : public TIntrinsicRefCounted
    {
        explicit TBlobHolder(TBlob&& blob);

        TBlob Blob;
    };

    struct TStringHolder
        : public TIntrinsicRefCounted
    {
        explicit TStringHolder(const Stroka& string);
        ~TStringHolder();

        Stroka Data;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTypeCookie Cookie;
        void InitializeTracking(TRefCountedTypeCookie cookie);
        void FinalizeTracking();
#endif
    };
};

extern const TSharedRef EmptySharedRef;

////////////////////////////////////////////////////////////////////////////////

//! A reference to a mutable range of memory with shared ownership.
//! Use with caution :)
class TSharedMutableRef
    : public TSharedMutableRange<char>
{
public:
    //! Creates a null TSharedMutableRef.
    TSharedMutableRef()
    { }

    //! Creates a TSharedMutableRef with a given holder.
    TSharedMutableRef(const TMutableRef& ref, THolderPtr holder)
        : TSharedMutableRange<char>(ref, std::move(holder))
    { }

    //! Creates a TSharedMutableRef from a pointer and length.
    TSharedMutableRef(void* data, size_t length, THolderPtr holder)
        : TSharedMutableRange<char>(static_cast<char*>(data), length, std::move(holder))
    { }

    //! Creates a TSharedMutableRange from a range.
    TSharedMutableRef(void* begin, void* end, THolderPtr holder)
        : TSharedMutableRange<char>(static_cast<char*>(begin), static_cast<char*>(end), std::move(holder))
    { }

    //! Converts a TSharedMutableRef to TMutableRef.
    operator TMutableRef() const
    {
        return TMutableRef(Begin(), Size());
    }

    //! Converts a TSharedMutableRef to TSharedRef.
    operator TSharedRef() const
    {
        return TSharedRef(Begin(), Size(), Holder_);
    }

    //! Converts a TSharedMutableRef to TRef.
    operator TRef() const
    {
        return TRef(Begin(), Size());
    }


    //! Allocates a new shared block of memory.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedMutableRef Allocate(size_t size, bool initializeStorage = true)
    {
        return Allocate(size, initializeStorage, GetRefCountedTypeCookie<TTag>());
    }

    //! Allocates a new shared block of memory.
    //! The memory is marked with TDefaultSharedBlobTag.
    static TSharedMutableRef Allocate(size_t size, bool initializeStorage = true)
    {
        return Allocate<TDefaultSharedBlobTag>(size, initializeStorage);
    }

    //! Allocates a new shared block of memory.
    //! The memory is marked with a given tag.
    static TSharedMutableRef Allocate(size_t size, bool initializeStorage, TRefCountedTypeCookie tagCookie)
    {
        auto blob = TBlob(tagCookie, size, initializeStorage);
        return FromBlob(std::move(blob));
    }

    //! Creates a TSharedMutableRef for the whole blob taking ownership of its content.
    static TSharedMutableRef FromBlob(TBlob&& blob)
    {
        auto ref = TMutableRef::FromBlob(blob);
        auto holder = New<TBlobHolder>(std::move(blob));
        return TSharedMutableRef(ref, std::move(holder));
    }

    //! Creates a copy of a given TRef.
    //! The memory is marked with a given tag.
    static TSharedMutableRef MakeCopy(const TRef& ref, TRefCountedTypeCookie tagCookie)
    {
        auto blob = TBlob(tagCookie, ref.Size(), false);
        ::memcpy(blob.Begin(), ref.Begin(), ref.Size());
        return FromBlob(std::move(blob));
    }

    //! Creates a copy of a given TRef.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedMutableRef MakeCopy(const TRef& ref)
    {
        return MakeCopy(ref, GetRefCountedTypeCookie<TTag>());
    }

    //! Creates a reference for a part of existing range.
    TSharedMutableRef Slice(size_t startOffset, size_t endOffset) const
    {
        YASSERT(endOffset >= startOffset && endOffset <= Size());
        return TSharedMutableRef(Begin() + startOffset, endOffset - startOffset, Holder_);
    }

    //! Creates a reference for a part of existing range.
    TSharedMutableRef Slice(void* begin, void* end) const
    {
        YASSERT(begin >= Begin());
        YASSERT(end <= End());
        return TSharedMutableRef(begin, end, Holder_);
    }

private:
    struct TBlobHolder
        : public TIntrinsicRefCounted
    {
        explicit TBlobHolder(TBlob&& blob);

        TBlob Blob;
    };

};

////////////////////////////////////////////////////////////////////////////////

//! A smart-pointer to a ref-counted immutable sequence of TSharedRef-s.
class TSharedRefArray
{
public:
    TSharedRefArray();
    TSharedRefArray(const TSharedRefArray& other);
    TSharedRefArray(TSharedRefArray&& other);
    ~TSharedRefArray();

    explicit TSharedRefArray(const TSharedRef& part);
    explicit TSharedRefArray(TSharedRef&& part);
    explicit TSharedRefArray(const std::vector<TSharedRef>& parts);
    explicit TSharedRefArray(std::vector<TSharedRef>&& parts);

    TSharedRefArray& operator = (const TSharedRefArray& other);
    TSharedRefArray& operator = (TSharedRefArray&& other);

    explicit operator bool() const;

    void Reset();

    int Size() const;
    i64 ByteSize() const;
    bool Empty() const;
    const TSharedRef& operator [] (int index) const;

    const TSharedRef* Begin() const;
    const TSharedRef* End() const;

    std::vector<TSharedRef> ToVector() const;

    TSharedRef Pack() const;
    static TSharedRefArray Unpack(const TSharedRef& packedRef);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    explicit TSharedRefArray(TIntrusivePtr<TImpl> impl);

};

// STL interop.
inline const TSharedRef* begin(const TSharedRefArray& array)
{
    return array.Begin();
}

inline const TSharedRef* end(const TSharedRefArray& array)
{
    return array.End();
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRef& ref);
Stroka ToString(const TMutableRef& ref);
Stroka ToString(const TSharedRef& ref);
Stroka ToString(const TSharedMutableRef& ref);

size_t GetPageSize();
size_t RoundUpToPage(size_t bytes);

template <class T>
size_t GetByteSize(const std::vector<T>& parts)
{
    size_t size = 0;
    for (const auto& part : parts) {
        size += part.Size();
    }
    return size;
}

inline size_t GetByteSize(const TRef& ref)
{
    return ref.Size();
}

inline size_t GetByteSize(const TSharedRefArray& array)
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

} // namespace NYT

