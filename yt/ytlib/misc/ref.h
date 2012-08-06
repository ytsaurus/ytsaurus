#pragma once

#include "common.h"

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<char> TBlob;

//! A non-owning reference to a block of memory.
/*!
 *  This is merely a |(start, size)| pair.
 */
class TRef
{
public:
    //! Creates a NULL reference with zero size.
    TRef()
        : Data(NULL)
        , Size_(0)
    { }

    //! Creates a reference for a given block of memory.
    TRef(void* data, size_t size)
    {
        YASSERT(data || size == 0);
        Data = reinterpret_cast<char*>(data);
        Size_ = size;
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

    //! Creates a non-owning reference for a given pod structure
    template<class T>
    static TRef FromPod(const T& data)
    {
        // TODO(ignat): append constantibilty to TRef
        return TRef(const_cast<T*>(&data), sizeof(data));
    }

    const char* Begin() const
    {
        return Data;
    }

    char* Begin()
    {
        return Data;
    }

    const char* End() const
    {
        return Data + Size_;
    }

    char* End()
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

    // Implicit conversion to bool.
    typedef char* TRef::*TUnspecifiedBoolType;
    operator TUnspecifiedBoolType() const
    {
        return Data ? &TRef::Data : NULL;
    }

private:
    char* Data;
    size_t Size_;
};

////////////////////////////////////////////////////////////////////////////////

//! A reference of a shared block of memory.
/*!
 *  Internally it is represented a by a shared pointer to a TBlob holding the
 *  data and a TRef pointing inside the blob.
 */
class TSharedRef
{
public:
    //! Creates a NULL reference.
    TSharedRef()
    { }
    
    explicit TSharedRef(size_t size)
        : Blob(new TBlob(size))
        , Ref(TRef::FromBlob(*Blob))
    { }

    //! Creates a non-owning reference from TPtr. Use it with caution!
    static TSharedRef FromRefNonOwning(const TRef& ref)
    {
        return TSharedRef(NULL, ref);
    }

    //! Creates an owning reference by copying data from a given string.
    static TSharedRef FromString(const Stroka& str)
    {
        TSharedRef ref(str.length());
        std::copy(str.begin(), str.end(), ref.Begin());
        return ref;
    }


    //! Creates a reference to the whole blob taking the ownership of its content.
    TSharedRef(TBlob&& blob)
        : Blob(new TBlob())
    {
        Blob->swap(blob);
        Ref = TRef::FromBlob(*Blob);
    }

    //! Creates a reference from another shared reference a reference to a portion of its data.
    TSharedRef(const TSharedRef& sharedRef, const TRef& ref)
        : Blob(sharedRef.Blob)
        , Ref(ref)
    {
        YASSERT(Ref.Begin() >= &*Blob->begin() && Ref.End() <= &*Blob->end());
    }

    operator TRef() const
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
        return Blob == other.Blob && Ref == other.Ref;
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
        return Ref ? &TSharedRef::Ref : NULL;
    }

private:
    typedef TSharedPtr<TBlob, TAtomicCounter> TBlobPtr;

    TSharedRef(const TBlobPtr& blob, const TRef& ref)
        : Blob(blob)
        , Ref(ref)
    { }

    TBlobPtr Blob;
    TRef Ref;
};

void Save(TOutputStream* output, const NYT::TSharedRef& ref);

void Load(TInputStream* input, NYT::TSharedRef& ref);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

