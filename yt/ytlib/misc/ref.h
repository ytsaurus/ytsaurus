#pragma once

#include "common.h"
#include "new.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<char> TBlob;

size_t RoundUpToPage(size_t bytes);
void AppendToBlob(TBlob& blob, const void* buffer, size_t size);

////////////////////////////////////////////////////////////////////////////////

//! A non-owning reference to a block of memory.
/*!
 *  This is merely a |(start, size)| pair.
 */
class TRef
{
public:
    //! Creates a null reference with zero size.
    TRef()
        : Data(nullptr)
        , Size_(0)
    { }

    //! Creates a reference for a given block of memory.
    TRef(void* data, size_t size)
    {
        YASSERT(data || size == 0);
        Data = reinterpret_cast<char*>(data);
        Size_ = size;
    }

    //! Creates a reference for a given range of memory.
    TRef(void* begin, void* end)
    {
        Data = reinterpret_cast<char*>(begin);
        Size_ = reinterpret_cast<char*>(end) - Data;
    }

    //! Creates a non-owning reference for a given blob.
    static TRef FromBlob(const TBlob& blob)
    {
        return TRef(const_cast<char*>(&*blob.begin()), blob.size());
    }

    //! Creates a non-owning reference for a given string.
    static TRef FromString(const Stroka& str)
    {
        return TRef(const_cast<char*>(str.data()), str.length());
    }

    //! Creates a non-owning reference for a given pod structure.
    template <class T>
    static TRef FromPod(const T& data)
    {
        static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
        // TODO(ignat): get rid of const_cast
        return TRef(const_cast<T*>(&data), sizeof (data));
    }

    char* Begin() const
    {
        return Data;
    }

    char* End() const
    {
        return Data + Size_;
    }

    bool Empty() const
    {
        return Size_ == 0;
    }

    size_t Size() const
    {
        return Size_;
    }

    //! Compares the pointer (not the content!) for equality.
    bool operator == (const TRef& other) const
    {
        return Data == other.Data && Size_ == other.Size_;
    }

    //! Compares the pointer (not the content!) for inequality.
    bool operator != (const TRef& other) const
    {
        return !(*this == other);
    }

    //! Compares the content for equality.
    static inline bool CompareContent(const TRef& lhs, const TRef& rhs)
    {
        if (lhs.Size() != rhs.Size())
            return false;
        if (lhs.Size() == 0)
            return true;
        return memcmp(lhs.Begin(), rhs.Begin(), lhs.Size()) == 0;
    }

    typedef char* TRef::*TUnspecifiedBoolType;
    //! Implicit conversion to bool.
    operator TUnspecifiedBoolType() const
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
 *  Each newly allocated TSharedRef is associated with a tag type that
 *  appears in ref-counted statistics.
 */
struct TDefaultSharedRefTag { };

//! A reference of a shared block of memory.
/*!
 *  Internally it is represented a by a shared pointer to a TBlob holding the
 *  data and a TRef pointing inside the blob.
 */
class TSharedRef
{
public:
    //! Creates a null reference.
    TSharedRef()
    { }

    //! Allocates a new shared block of memory.
    template <class TTag>
    static TSharedRef Allocate(size_t size)
    {
        auto result = AllocateImpl(size);
#ifdef ENABLE_REF_COUNTED_TRACKING
        void* cookie = ::NYT::NDetail::GetRefCountedTrackerCookie<TTag>();
        result.Data->InitializeTracking(cookie);
#endif
        return result;
    }

    static TSharedRef Allocate(size_t size)
    {
        return Allocate<TDefaultSharedRefTag>(size);
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
        return FromString<TDefaultSharedRefTag>(str);
    }

    //! Creates a reference to the whole blob taking the ownership of its content.
    template <class TTag>
    static TSharedRef FromBlob(TBlob&& blob)
    {
        auto result = FromBlobImpl(std::move(blob));
#ifdef ENABLE_REF_COUNTED_TRACKING
        void* cookie = ::NYT::NDetail::GetRefCountedTrackerCookie<TTag>();
        result.Data->InitializeTracking(cookie);
#endif
        return result;
    }

    static TSharedRef FromBlob(TBlob&& blob)
    {
        return FromBlob<TDefaultSharedRefTag>(std::move(blob));
    }

    //! Creates a reference to a portion of currently held data.
    TSharedRef Slice(const TRef& sliceRef) const
    {
        YASSERT(sliceRef.Begin() >= Ref.Begin() && sliceRef.End() <= Ref.End());
        return TSharedRef(Data, sliceRef);
    }

    operator const TRef&() const
    {
        return Ref;
    }

    const char* Begin() const
    {
        return Ref.Begin();
    }

    char* Begin()
    {
        return Ref.Begin();
    }

    const char* operator ~ () const
    {
        return Begin();
    }

    const char* End() const
    {
        return Ref.End();
    }

    char* End()
    {
        return Ref.End();
    }

    size_t Size() const
    {
        return Ref.Size();
    }

    bool Empty() const
    {
        return Ref.Empty();
    }

    //! Compares the pointer (not the content!) for equality.
    bool operator == (const TSharedRef& other) const
    {
        return Data == other.Data && Ref == other.Ref;
    }

    //! Compares the pointer (not the content!) for inequality.
    bool operator != (const TSharedRef& other) const
    {
        return !(*this == other);
    }

    // Implicit conversion to bool.
    typedef TRef TSharedRef::*TUnspecifiedBoolType;
    operator TUnspecifiedBoolType() const
    {
        return Ref ? &TSharedRef::Ref : nullptr;
    }

private:
    struct TData
        : public TIntrinsicRefCounted
    {
        explicit TData(TBlob&& blob);
        ~TData();

        TBlob Blob;

#ifdef ENABLE_REF_COUNTED_TRACKING
        void* Cookie;

        void InitializeTracking(void* cookie);
        void FinalizeTracking();
#endif
    };

    typedef TIntrusivePtr<TData> TDataPtr;

    TDataPtr Data;
    TRef Ref;

    TSharedRef(TDataPtr data, const TRef& ref)
        : Data(std::move(data))
        , Ref(ref)
    { }

    static TSharedRef AllocateImpl(size_t size);
    static TSharedRef FromBlobImpl(TBlob&& blob);

};

void Save(TOutputStream* output, const NYT::TSharedRef& ref);
void Load(TInputStream* input, NYT::TSharedRef& ref);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

