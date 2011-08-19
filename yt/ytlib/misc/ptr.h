#pragma once

#include "common.h"

#ifdef ENABLE_REF_COUNTED_TRACKING
#include "ref_counted_tracker.h"
#endif

#include <util/stream/str.h>
#include <util/system/atexit.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: consider making Ref, UnRef, and AfterConstruct private and
// declare appropriate friends.
//! Provides a common base for all reference-counted objects within YT.
class TRefCountedBase
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TRefCountedBase()
        // Counter is initially set to 1, see #AfterConstruct.
        : RefCounter(1)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , TypeInfo(NULL)
#endif
    { }

    //! Destroys the instance.
    virtual ~TRefCountedBase()
    {
        // Failure within this line means an object is being
        // destroyed by other than calling #UnRef, e.g. is was allocated on stack.
        YASSERT(RefCounter == 0);
    }

    //! Called from #New functions to kill the initial fake reference.
    void AfterConstruct()
    {
        YASSERT(RefCounter >= 1);
        UnRef();
    }

    //! Increments the reference counter.
    /*!
     *  When reference tracking is enabled, this call also registers the instance with the tracker
     *  if the counter reaches 2 (i.e. the first non-fake reference is created).
     */
    inline void Ref() throw()
    {
        // Failure within this line means a zombie is being resurrected.
        YASSERT(RefCounter > 0);

        AtomicIncrement(RefCounter);

#ifdef ENABLE_REF_COUNTED_TRACKING
        if (TypeInfo == NULL) {
            YASSERT(TypeInfo == NULL);
            TypeInfo = &typeid (*this);
            TRefCountedTracker::Get()->Increment(TypeInfo);
        }
#endif
    }

    //! Decrements the reference counter.
    /*!
     *  When this counter reaches 0, the object also kills itself by calling "delete this".
     *  When reference tracking is enabled, this call also unregisters the instance from the tracker.
     */
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

    //! Tries to obtain an intrusive pointer for an object that
    //! may had already lost all of its references and, thus, is about to be deleted.
    /*!
     *  You may call this method at any time provided that you have a valid raw pointer to an object.
     *  The call either returns an intrusive pointer for the object
     *  (thus ensuring that the object won't be destroyed until you're holding this pointer)
     *  or NULL indicating that the last reference had already been lost and the object is on
     *  its way to heavens. All these steps happen atomically.
     *  
     *  Under all circumstances it full caller's responsibility the make sure that the object
     *  is not destroyed during the call to #DangerousGetPtr. Typically this is achieved
     *  by taking a lock in object's destructor and unregistering it there.
     */
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

// TODO: generate with pump

template<class TResult>
inline TIntrusivePtr<TResult> New()
{
    TIntrusivePtr<TResult> result = new TResult();
    result->UnRef();
    return result;
}

template<
    class TResult,
    class TArg1
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2,
    class TArg3
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2,
    const TArg3& arg3)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2,
        arg3);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2,
    class TArg3,
    class TArg4
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2,
    const TArg3& arg3,
    const TArg4& arg4)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2,
        arg3,
        arg4);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2,
    class TArg3,
    class TArg4,
    class TArg5
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2,
    const TArg3& arg3,
    const TArg4& arg4,
    const TArg5& arg5)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2,
        arg3,
        arg4,
        arg5);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2,
    class TArg3,
    class TArg4,
    class TArg5,
    class TArg6
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2,
    const TArg3& arg3,
    const TArg4& arg4,
    const TArg5& arg5,
    const TArg6& arg6)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2,
        arg3,
        arg4,
        arg5,
        arg6);
    result->AfterConstruct();
    return result;
}

template<
    class TResult,
    class TArg1,
    class TArg2,
    class TArg3,
    class TArg4,
    class TArg5,
    class TArg6,
    class TArg7
>
inline TIntrusivePtr<TResult> New(
    const TArg1& arg1,
    const TArg2& arg2,
    const TArg3& arg3,
    const TArg4& arg4,
    const TArg5& arg5,
    const TArg6& arg6,
    const TArg7& arg7)
{
    TIntrusivePtr<TResult> result = new TResult(
        arg1,
        arg2,
        arg3,
        arg4,
        arg5,
        arg6,
        arg7);
    result->AfterConstruct();
    return result;
}

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
