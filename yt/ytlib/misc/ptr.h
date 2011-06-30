#pragma once

#include "common.h"
#include "lazy_ptr.h"

#ifdef ENABLE_REF_COUNTED_TRACKING
#include "ref_counted_tracker.h"
#endif

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#ifdef ENABLE_REF_COUNTED_TRACKING

class TRefCountedBase
    : private TNonCopyable
{
public:
    TRefCountedBase()
        : RefCounter(0)
        , TypeInfo(0)
    { }

    virtual ~TRefCountedBase()
    { }

    inline void Ref() throw()
    {
        if (AtomicIncrement(RefCounter) == 1) {
            YASSERT(TypeInfo == NULL);
            TypeInfo = &typeid (*this);
            TRefCountedTracker::Get()->Increment(TypeInfo);
        }
    }

    inline void UnRef() throw()
    {
        if (AtomicDecrement(RefCounter) == 0) {
            YASSERT(TypeInfo != NULL);
            TRefCountedTracker::Get()->Decrement(TypeInfo);
            delete this;
        }
    }

private:
    TAtomic RefCounter;
    const std::type_info* TypeInfo;
};

#else

class TRefCountedBase
    : public TRefCounted<TRefCountedBase, TAtomicCounter>
    , private TNonCopyable
{
public:
    virtual ~TRefCountedBase()
    { }
};

#endif

////////////////////////////////////////////////////////////////////////////////

template<class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    // Copied from util/generic/singleton.h.
    static TIntrusivePtr<T> instance;
    if (EXPECT_FALSE(!instance))
        instance = SingletonInt<T>();
    return instance;
}

////////////////////////////////////////////////////////////////////////////////

typedef yvector<char> TBlob;

class TRef
{
public:
    TRef()
        : Data(NULL)
        , Size_(0)
    { }

    TRef(void* data, size_t size)
        : Data(NULL)
        , Size_(0)
    {
        if (data != NULL && size != 0) {
            Data = reinterpret_cast<char*>(data);
            Size_ = size;
        }
    }

    TRef(const TBlob& blob)
        : Data(NULL)
        , Size_(0)
    {
        if (!blob.empty()) {
            Data = const_cast<char*>(blob.begin());
            Size_ = blob.size();
        }
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

    size_t Size() const
    {
        return Size_;
    }

    // Let's hope your compiler supports RVO.
    TBlob ToBlob()
    {
        return TBlob(Begin(), End());
    }

private:
    char* Data;
    size_t Size_;

};

////////////////////////////////////////////////////////////////////////////////

class TSharedRef
{
public:
    typedef TSharedPtr<TBlob, TAtomicCounter> TBlobPtr;

    TSharedRef(TBlob& blob)
        : Blob(new TBlob())
    {
        Blob->swap(blob);
        Ref = *Blob;
    }

    TSharedRef(TBlobPtr blob, TRef ref)
        : Blob(blob)
        , Ref(ref)
    { }

    TSharedRef()
    { }

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

    // Let's hope your compiler supports RVO.
    TBlob ToBlob()
    {
        return Ref.ToBlob();
    }

private:
    TBlobPtr Blob;
    TRef Ref;

};

////////////////////////////////////////////////////////////////////////////////

template<class T>
T* operator ~ (const TIntrusivePtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const TAutoPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const THolder<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const TLazyPtr<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
