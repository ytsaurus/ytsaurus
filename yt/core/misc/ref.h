#pragma once

#include "common.h"
#include "blob.h"
#include "new.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A non-owning reference to a block of memory.
/*!
 *  This is merely a |(start, size)| pair.
 */
class TRef
{
public:
    //! Creates a null reference with zero size.
    FORCED_INLINE TRef()
        : Data_(nullptr)
        , Size_(0)
    { }

    //! Creates a reference for a given block of memory.
    FORCED_INLINE TRef(void* data, size_t size)
    {
        YASSERT(data || size == 0);
        Data_ = reinterpret_cast<char*>(data);
        Size_ = size;
    }

    //! Creates a reference for a given range of memory.
    FORCED_INLINE TRef(void* begin, void* end)
    {
        Data_ = reinterpret_cast<char*>(begin);
        Size_ = reinterpret_cast<char*>(end) - Data_;
    }

    //! Creates a non-owning reference for a given blob.
    static FORCED_INLINE TRef FromBlob(const TBlob& blob)
    {
        return TRef(const_cast<char*>(&*blob.Begin()), blob.Size());
    }

    //! Creates a non-owning reference for a given string.
    static FORCED_INLINE TRef FromString(const Stroka& str)
    {
        return TRef(const_cast<char*>(str.data()), str.length());
    }

    //! Creates a non-owning reference for a given pod structure.
    template <class T>
    static FORCED_INLINE TRef FromPod(const T& data)
    {
        static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
        return TRef(const_cast<T*>(&data), sizeof (data));
    }

    FORCED_INLINE char* Begin() const
    {
        return Data_;
    }

    FORCED_INLINE char* End() const
    {
        return Data_ + Size_;
    }

    FORCED_INLINE bool Empty() const
    {
        return Size_ == 0;
    }

    FORCED_INLINE size_t Size() const
    {
        return Size_;
    }

    //! Compares the pointer (not the content!) for equality.
    FORCED_INLINE bool operator == (const TRef& other) const
    {
        return Data_ == other.Data_ && Size_ == other.Size_;
    }

    //! Compares the pointer (not the content!) for inequality.
    FORCED_INLINE bool operator != (const TRef& other) const
    {
        return !(*this == other);
    }

    //! Compares the content for bitwise equality.
    static bool AreBitwiseEqual(const TRef& lhs, const TRef& rhs);

    typedef char* TRef::*TUnspecifiedBoolType;
    //! Implicit conversion to bool.
    FORCED_INLINE operator TUnspecifiedBoolType() const
    {
        return Data_ ? &TRef::Data_ : nullptr;
    }

private:
    char* Data_;
    size_t Size_;

};

////////////////////////////////////////////////////////////////////////////////

//! Default tag type for memory blocks allocated via TSharedRef.
/*!
 *  Each newly allocated TSharedRef blob is associated with a tag type that
 *  appears in ref-counted statistics.
 */
struct TDefaultSharedBlobTag { };

//! A reference to a range of memory with shared ownership.
/*!
 *  Internally it is represented by a pointer to a ref-counted polymorphic holder
 *  (solely responsible for resource release) and a TRef pointing inside the blob.
 *
 *  If the holder is |nullptr| then no ownership is tracked and TSharedRef reduces to just TRef.
 */
class TSharedRef
{
public:
    typedef TIntrinsicRefCounted THolder;
    typedef TIntrusivePtr<THolder> THolderPtr;

    //! Creates a null reference.
    TSharedRef()
    { }

    //! Creates a reference with a given holder.
    TSharedRef(THolderPtr holder, const TRef& ref)
        : Holder_(std::move(holder))
        , Ref_(ref)
    { }


    //! Allocates a new shared block of memory.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedRef Allocate(size_t size, bool initializeStorage = true)
    {
        return Allocate(size, initializeStorage, GetRefCountedTypeCookie<TDefaultSharedBlobTag>());
    }

    //! Allocates a new shared block of memory.
    //! The memory is marked with TDefaultSharedBlobTag.
    static TSharedRef Allocate(size_t size, bool initializeStorage = true)
    {
        return Allocate<TDefaultSharedBlobTag>(size, initializeStorage);
    }

    //! Allocates a new shared block of memory.
    //! The memory is marked with a given tag.
    static TSharedRef Allocate(size_t size, bool initializeStorage, TRefCountedTypeCookie tagCookie)
    {
        TBlob blob(size, initializeStorage);
        return FromBlob(std::move(blob), tagCookie);
    }


    //! Creates a non-owning reference from TRef. Use it with caution!
    static TSharedRef FromRefNonOwning(const TRef& ref)
    {
        return TSharedRef(nullptr, ref);
    }


    //! Creates an owning reference from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedRef FromString(const Stroka& str)
    {
        return FromString(str, GetRefCountedTypeCookie<TTag>());
    }

    //! Creates an owning reference from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with TDefaultSharedBlobTag.
    static TSharedRef FromString(const Stroka& str)
    {
        return FromString<TDefaultSharedBlobTag>(str);
    }

    //! Creates an owning reference from a string.
    //! Since strings are ref-counted, no data is copied.
    //! The memory is marked with a given tag.
    static TSharedRef FromString(const Stroka& str, TRefCountedTypeCookie tagCookie)
    {
        auto holder = New<TStringHolder>(str);
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        holder->InitializeTracking(tagCookie);
#endif
        auto ref = TRef::FromString(holder->Data_);
        return TSharedRef(std::move(holder), ref);
    }


    //! Creates a reference to the whole blob taking ownership of its content.
    //! The memory is marked with a given tag.
    template <class TTag>
    static TSharedRef FromBlob(TBlob&& blob)
    {
        return FromBlob(std::move(blob), GetRefCountedTypeCookie<TTag>());
    }

    //! Creates a reference to the whole blob taking ownership of its content.
    //! The memory is marked with TDefaultSharedBlobTag.
    static TSharedRef FromBlob(TBlob&& blob)
    {
        return FromBlob<TDefaultSharedBlobTag>(std::move(blob));
    }

    //! Creates a reference to the whole blob taking ownership of its content.
    //! The memory is marked with a given tag.
    static TSharedRef FromBlob(TBlob&& blob, TRefCountedTypeCookie tagCookie)
    {
        auto holder = New<TBlobHolder>(std::move(blob));
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        holder->InitializeTracking(tagCookie);
#endif
        auto ref = TRef::FromBlob(holder->Blob_);
        return TSharedRef(std::move(holder), ref);
    }


    //! Creates a reference to a portion of currently held data.
    TSharedRef Slice(const TRef& sliceRef) const
    {
        YASSERT(sliceRef.Begin() >= Ref_.Begin() && sliceRef.End() <= Ref_.End());
        return TSharedRef(Holder_, sliceRef);
    }

    FORCED_INLINE operator const TRef&() const
    {
        return Ref_;
    }

    FORCED_INLINE const char* Begin() const
    {
        return Ref_.Begin();
    }

    FORCED_INLINE char* Begin()
    {
        return Ref_.Begin();
    }

    FORCED_INLINE const char* operator ~ () const
    {
        return Begin();
    }

    FORCED_INLINE const char* End() const
    {
        return Ref_.End();
    }

    FORCED_INLINE char* End()
    {
        return Ref_.End();
    }

    FORCED_INLINE size_t Size() const
    {
        return Ref_.Size();
    }

    FORCED_INLINE bool Empty() const
    {
        return Ref_.Empty();
    }

    //! Compares the pointer (not the content!) for equality.
    FORCED_INLINE bool operator == (const TSharedRef& other) const
    {
        return Holder_ == other.Holder_ && Ref_ == other.Ref_;
    }

    //! Compares the pointer (not the content!) for inequality.
    FORCED_INLINE bool operator != (const TSharedRef& other) const
    {
        return !(*this == other);
    }

    // Implicit conversion to bool.
    typedef TRef TSharedRef::*TUnspecifiedBoolType;
    FORCED_INLINE operator TUnspecifiedBoolType() const
    {
        return Ref_ ? &TSharedRef::Ref_ : nullptr;
    }

    friend void swap(TSharedRef& lhs, TSharedRef& rhs)
    {
        using std::swap;
        swap(lhs.Holder_, rhs.Holder_);
        swap(lhs.Ref_, rhs.Ref_);
    }

    TSharedRef& operator = (TSharedRef other)
    {
        swap(*this, other);
        return *this;
    }

    void Reset()
    {
        Holder_.Reset();
        Ref_ = TRef();
    }

    template <class TTag>
    void EnsureNonShared()
    {
        if (Holder_ && Holder_->GetRefCount() > 1) {
            auto other = Allocate<TTag>(Size(), false);
            memcpy(other.Begin(), Begin(), Size());
            swap(*this, other);
        }
    }

private:
    class TBlobHolder
        : public THolder
    {
    public:
        explicit TBlobHolder(TBlob&& blob);
        ~TBlobHolder();

    private:
        friend class TSharedRef;

        TBlob Blob_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTypeCookie Cookie_;
        void InitializeTracking(TRefCountedTypeCookie cookie);
        void FinalizeTracking();
#endif
    };

    class TStringHolder
        : public THolder
    {
    public:
        explicit TStringHolder(const Stroka& string);
        ~TStringHolder();

    private:
        friend class TSharedRef;

        Stroka Data_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        TRefCountedTypeCookie Cookie_;
        void InitializeTracking(TRefCountedTypeCookie cookie);
        void FinalizeTracking();
#endif
    };

    THolderPtr Holder_;
    TRef Ref_;

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

    void Reset();

    explicit operator bool() const;

    int Size() const;
    i64 ByteSize() const;
    bool Empty() const;
    const TSharedRef& operator [] (int index) const;

    const TSharedRef* Begin() const;
    const TSharedRef* End() const;

    std::vector<TSharedRef> ToVector() const;

    TSharedRef Pack() const;
    static TSharedRefArray Unpack(const TSharedRef& packedRef);

    friend void swap(TSharedRefArray& lhs, TSharedRefArray& rhs);
    TSharedRefArray& operator = (TSharedRefArray other);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    explicit TSharedRefArray(TIntrusivePtr<TImpl> impl);

};

// Range-for interop.
const TSharedRef* begin(const TSharedRefArray& array);
const TSharedRef* end(const TSharedRefArray& array);

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRef& ref);
Stroka ToString(const TSharedRef& ref);
size_t GetPageSize();
size_t RoundUpToPage(size_t bytes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

