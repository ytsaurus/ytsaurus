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
        : Data(nullptr)
        , Size_(0)
    { }

    //! Creates a reference for a given block of memory.
    FORCED_INLINE TRef(void* data, size_t size)
    {
        YASSERT(data || size == 0);
        Data = reinterpret_cast<char*>(data);
        Size_ = size;
    }

    //! Creates a reference for a given range of memory.
    FORCED_INLINE TRef(void* begin, void* end)
    {
        Data = reinterpret_cast<char*>(begin);
        Size_ = reinterpret_cast<char*>(end) - Data;
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
        return Data;
    }

    FORCED_INLINE char* End() const
    {
        return Data + Size_;
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
        return Data == other.Data && Size_ == other.Size_;
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
        return Data ? &TRef::Data : nullptr;
    }

private:
    char* Data;
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
        : Holder(std::move(holder))
        , Ref(ref)
    { }

    //! Allocates a new shared block of memory.
    template <class TTag>
    static TSharedRef Allocate(size_t size, bool initializeStorage = true)
    {
        TBlob blob(size, initializeStorage);
        return FromBlob<TTag>(std::move(blob));
    }

    static TSharedRef Allocate(size_t size, bool initializeStorage = true)
    {
        return Allocate<TDefaultSharedBlobTag>(size, initializeStorage);
    }

    //! Creates a non-owning reference from TPtr. Use it with caution!
    static TSharedRef FromRefNonOwning(const TRef& ref)
    {
        return TSharedRef(nullptr, ref);
    }

    //! Creates an owning reference by copying data from a given string.
    template <class TTag>
    static TSharedRef FromString(const Stroka& str)
    {
        auto result = TSharedRef::Allocate<TTag>(str.length());
        std::copy(str.begin(), str.end(), result.Begin());
        return result;
    }

    static TSharedRef FromString(const Stroka& str)
    {
        return FromString<TDefaultSharedBlobTag>(str);
    }

    //! Creates a reference to the whole blob taking the ownership of its content.
    template <class TTag>
    static TSharedRef FromBlob(TBlob&& blob)
    {
        auto holder = New<TBlobHolder>(std::move(blob));
#ifdef ENABLE_REF_COUNTED_TRACKING
        void* cookie = ::NYT::NDetail::GetRefCountedTrackerCookie<TTag>();
        holder->InitializeTracking(cookie);
#endif
        auto ref = TRef::FromBlob(holder->Blob);
        return TSharedRef(std::move(holder), ref);
    }

    static TSharedRef FromBlob(TBlob&& blob)
    {
        return FromBlob<TDefaultSharedBlobTag>(std::move(blob));
    }

    //! Creates a reference to a portion of currently held data.
    TSharedRef Slice(const TRef& sliceRef) const
    {
        YASSERT(sliceRef.Begin() >= Ref.Begin() && sliceRef.End() <= Ref.End());
        return TSharedRef(Holder, sliceRef);
    }

    FORCED_INLINE operator const TRef&() const
    {
        return Ref;
    }

    FORCED_INLINE const char* Begin() const
    {
        return Ref.Begin();
    }

    FORCED_INLINE char* Begin()
    {
        return Ref.Begin();
    }

    FORCED_INLINE const char* operator ~ () const
    {
        return Begin();
    }

    FORCED_INLINE const char* End() const
    {
        return Ref.End();
    }

    FORCED_INLINE char* End()
    {
        return Ref.End();
    }

    FORCED_INLINE size_t Size() const
    {
        return Ref.Size();
    }

    FORCED_INLINE bool Empty() const
    {
        return Ref.Empty();
    }

    //! Compares the pointer (not the content!) for equality.
    FORCED_INLINE bool operator == (const TSharedRef& other) const
    {
        return Holder == other.Holder && Ref == other.Ref;
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
        return Ref ? &TSharedRef::Ref : nullptr;
    }


    friend void swap(TSharedRef& lhs, TSharedRef& rhs)
    {
        using std::swap;
        swap(lhs.Holder, rhs.Holder);
        swap(lhs.Ref, rhs.Ref);
    }

    TSharedRef& operator = (TSharedRef other)
    {
        swap(*this, other);
        return *this;
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

        TBlob Blob;

#ifdef ENABLE_REF_COUNTED_TRACKING
        void* Cookie;
        void InitializeTracking(void* cookie);
        void FinalizeTracking();
#endif
    };

    THolderPtr Holder;
    TRef Ref;

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
    TIntrusivePtr<TImpl> Impl;

    explicit TSharedRefArray(TIntrusivePtr<TImpl> impl);

};

// Range-for interop.
const TSharedRef* begin(const TSharedRefArray& array);
const TSharedRef* end(const TSharedRefArray& array);

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TRef& ref);
size_t GetPageSize();
size_t RoundUpToPage(size_t bytes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

