#pragma once

#include "common.h"
#include "lazy_ptr.h"

#ifdef ENABLE_REF_COUNTED_TRACKING
#include "ref_counted_tracker.h"
#endif

#include <util/stream/str.h>
#include <util/system/atexit.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRefCountedBase
    : private TNonCopyable
{
public:
    TRefCountedBase()
        : RefCounter(0)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , TypeInfo(NULL)
#endif
    { }

    virtual ~TRefCountedBase()
    { }

    inline void Ref() throw()
    {
#ifdef ENABLE_REF_COUNTED_TRACKING
        if (AtomicIncrement(RefCounter) == 1) {
            // Failure within this line means
            // a zombie is being resurrected!
            YASSERT(TypeInfo == NULL);
            TypeInfo = &typeid (*this);
            TRefCountedTracker::Get()->Increment(TypeInfo);
        }
#else
        AtomicIncrement(RefCounter);
#endif
    }

    inline void UnRef() throw()
    {
        if (AtomicDecrement(RefCounter) == 0) {
#ifdef ENABLE_REF_COUNTED_TRACKING
            YASSERT(TypeInfo != NULL);
            TRefCountedTracker::Get()->Decrement(TypeInfo);
#endif
            delete this;
        }
    }

    template<class T>
    static TIntrusivePtr<T> DangerousGetPtr(T* obj)
    {
        while (true) {
            TAtomic counter = obj->RefCounter;

            YASSERT(counter >= 0);
            if (counter == 0)
                return NULL;

            TAtomic newCounter = counter + 1;
            if (AtomicCas(&obj->RefCounter, newCounter, counter)) {
                TIntrusivePtr<T> ptr(obj);
                AtomicDecrement(obj->RefCounter);
                return ptr;
            }    
        }
    }

private:
    TAtomic RefCounter;

#ifdef ENABLE_REF_COUNTED_TRACKING
    const std::type_info* TypeInfo;
#endif

};


////////////////////////////////////////////////////////////////////////////////

template<class T>
void RefCountedSingletonDestroyer(void* ctx)
{
    T** obj = reinterpret_cast<T**>(ctx);
    (*obj)->UnRef();
    *obj = reinterpret_cast<T*>(-1);
}

template<class T>
TIntrusivePtr<T> RefCountedSingleton()
{
    static T* volatile instance;

    YASSERT(instance != reinterpret_cast<T*>(-1));

    if (EXPECT_TRUE(instance != NULL)) {
        return instance;
    }

    static TSpinLock spinLock;
    TGuard<TSpinLock> guard(spinLock);

    if (instance != NULL) {
        return instance;
    }

    T* obj = new T();
    obj->Ref();

    instance = obj;

    AtExit(
        RefCountedSingletonDestroyer<T>,
        const_cast<T**>(&instance),
        TSingletonTraits<T>::Priority);

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

    bool operator == (const TRef& other) const
    {
        return Data == other.Data && Size_ == other.Size_;
    }

    bool operator != (const TRef& other) const
    {
        return !(*this == other);
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

    explicit TSharedRef(TBlob& blob)
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

    bool operator == (const TSharedRef& other) const
    {
        return Blob == other.Blob && Ref == other.Ref;
    }

    bool operator != (const TSharedRef& other) const
    {
        return !(*this == other);
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
