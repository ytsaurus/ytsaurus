#pragma once

#include "ref_counted_tracker.h"
#include "new.h"

#include <util/stream/str.h>
#include <util/system/atexit.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: consider making Ref, UnRef, and AfterConstruction private and
// declare appropriate friends.

//! Provides a common base for all reference-counted objects within YT.
class TRefCountedBase
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TRefCountedBase()
        // Counter is initially set to 1, see #AfterConstruction.
        : RefCounter(1)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    //! Destroys the instance.
    virtual ~TRefCountedBase()
    { }

#ifdef ENABLE_REF_COUNTED_TRACKING
    //! Called from #New functions to kill the initial fake reference
    //! and initialize the #Cookie.
    /*!
     * When reference tracking is enabled, this call also registers the instance
     * with the tracker.
     */
    inline void AfterConstruction(TRefCountedTracker::TCookie cookie)
    {
        YASSERT(Cookie == NULL);
        Cookie = cookie;
        TRefCountedTracker::Register(cookie);

        YASSERT(RefCounter >= 1);
        UnRef();
    }
#else
    //! Called from #New functions to kill the initial fake reference.
    inline void AfterConstruction()
    {
        YASSERT(RefCounter >= 1);
        UnRef();
    }
#endif

    //! Increments the reference counter.
    inline void Ref() throw()
    {
        // Failure within this line means a zombie is being resurrected.
        YASSERT(RefCounter > 0);
        AtomicIncrement(RefCounter);
    }

    //! Decrements the reference counter.
    /*!
     * When this counter reaches zero, the object also kills itself by calling
     * "delete this". When reference tracking is enabled, this call also
     * unregisters the instance from the tracker.
     */
    inline void UnRef() throw()
    {
        if (AtomicDecrement(RefCounter) == 0) {
#ifdef ENABLE_REF_COUNTED_TRACKING
            YASSERT(Cookie != NULL);
            TRefCountedTracker::Unregister(Cookie);
#endif
            delete this;
        }
    }

    //! Tries to obtain an intrusive pointer for an object that may had
    //! already lost all of its references and, thus, is about to be deleted.
    /*!
     * You may call this method at any time provided that you have a valid
     * raw pointer to an object. The call either returns an intrusive pointer
     * for the object (thus ensuring that the object won't be destroyed until
     * you're holding this pointer) or NULL indicating that the last reference
     * had already been lost and the object is on its way to heavens.
     * All these steps happen atomically.
     *  
     * Under all circumstances it is caller's responsibility the make sure that
     * the object is not destroyed during the call to #DangerousGetPtr.
     * Typically this is achieved by keeping a (lock-protected) collection of
     * raw pointers, taking a lock in object's destructor, and unregistering
     * its raw pointer from the collection there.
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
    TRefCountedTracker::TCookie Cookie;
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
    TBlob ToBlob() const
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
    TBlob ToBlob() const
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
