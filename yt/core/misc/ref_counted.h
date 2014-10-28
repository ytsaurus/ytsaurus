#pragma once

#include "intrusive_ptr.h"

#include <util/generic/noncopyable.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRefCountedBase;
class TExtrinsicRefCounted;
class TIntrinsicRefCounted;

// This is a reasonable default.
// For performance-critical bits of code use TIntrinsicRefCounted instead.
typedef TExtrinsicRefCounted TRefCounted;

typedef int TRefCountedTypeCookie;
const int NullRefCountedTypeCookie = -1;

typedef const void* TRefCountedTypeKey;

///////////////////////////////////////////////////////////////////////////////

namespace NDetail {

typedef intptr_t TNonVolatileCounter;

static_assert(sizeof(TNonVolatileCounter) == sizeof(void*),
    "TNonVolatileCounter should be the same size as a raw pointer.");

static inline TNonVolatileCounter AtomicallyIncrementIfNonZero(std::atomic<TNonVolatileCounter> &p)
{
    // Atomically performs the following:
    // { auto v = *p; if (v != 0) ++(*p); return v; }
    auto v = p.load();

    for (;;) {
        if (v == 0) {
            return v;
        }

        if (p.compare_exchange_strong(v, v+1)) {
            return v;
        } else {
            v = p.load();
        }
    }
}

//! An atomic reference counter for extrinsic reference counting.
class TRefCounter
{
public:
    TRefCounter(TExtrinsicRefCounted* object)
        : StrongCount(1)
        , WeakCount(1)
        , that(object)
    { }

    ~TRefCounter()
    { }

    //! This method is called when there are no strong references remaining
    //! and the object have to be disposed (technically this means that
    //! there are no more strong references being held).
    void Dispose();

    //! This method is called when the counter is about to be destroyed
    //! (technically this means that there are neither strong
    //! nor weak references being held).
    void Destroy();

    //! Adds a strong reference to the counter.
    inline void Ref() // noexcept
    {
        auto sc = StrongCount++;
        YASSERT(sc > 0 && WeakCount.load() > 0);

    }

    //! Removes a strong reference from the counter.
    inline void Unref() // noexcept
    {
        auto sc = StrongCount--;
        YASSERT(sc > 0);

        if (sc == 1) {
            Dispose();

            auto wc = WeakCount--;
            YASSERT(wc > 0);

            if (wc == 1) {
                Destroy();
            }
        }
    }

    //! Tries to add a strong reference to the counter.
    inline bool TryRef() // noexcept
    {
        YASSERT(WeakCount.load() > 0);
        return AtomicallyIncrementIfNonZero(StrongCount) > 0;
    }

    //! Adds a weak reference to the counter.
    inline void WeakRef() // noexcept
    {
        auto wc = WeakCount++;
        YASSERT(wc > 0);
    }

    //! Removes a weak reference from the counter.
    inline void WeakUnref() // noexcept
    {
        auto wc = WeakCount--;
        YASSERT(wc > 0);
        if (wc == 1) {
            Destroy();
        }
    }

    //! Returns the current number of strong references.
    int GetRefCount() const // noexcept
    {
        return StrongCount.load();
    }

    //! Returns the current number of weak references.
    int GetWeakRefCount() const // noexcept
    {
        return WeakCount.load();
    }

private:
    // Explicitly prohibit forbidden constructors and operators.
    TRefCounter();
    TRefCounter(const TRefCounter&);
    TRefCounter(const TRefCounter&&);
    TRefCounter& operator=(const TRefCounter&);
    TRefCounter& operator=(const TRefCounter&&);

    //! Number of strong references.
    std::atomic<TNonVolatileCounter> StrongCount;
    //! Number of weak references plus one if there is at least one strong reference.
    std::atomic<TNonVolatileCounter> WeakCount;
    //! The object.
    TExtrinsicRefCounted* that;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

//! A helper called by #New to register the just-created instance.
void InitializeTracking(TRefCountedBase* object, TRefCountedTypeCookie typeCookie, size_t instanceSize);

#endif

//! Base class for all reference-counted objects.
class TRefCountedBase
    : private TNonCopyable
{
protected:
    virtual ~TRefCountedBase();

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    void FinalizeTracking();
#endif

private:
    friend void InitializeTracking(TRefCountedBase* object, TRefCountedTypeCookie typeCookie, size_t instanceSize);

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie TypeCookie = NullRefCountedTypeCookie;
    size_t InstanceSize = 0;

    void InitializeTracking(TRefCountedTypeCookie typeCookie, size_t instanceSize);
#endif

};

#ifdef YT_ENABLE_REF_COUNTED_TRACKING

FORCED_INLINE void InitializeTracking(TRefCountedBase* object, TRefCountedTypeCookie typeCookie, size_t instanceSize)
{
    object->InitializeTracking(typeCookie, instanceSize);
}

#endif

////////////////////////////////////////////////////////////////////////////////

//! Base class for all reference-counted objects with extrinsic reference counting.
class TExtrinsicRefCounted
    : public TRefCountedBase
{
public:
    TExtrinsicRefCounted();
    virtual ~TExtrinsicRefCounted();

    //! Increments the reference counter.
    inline void Ref() const // noexcept
    {
#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        auto rc = RefCounter->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Ref(): %" PRId32 " -> %" PRId32, this, rc, rc + 1);
#endif
        RefCounter->Ref();
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        auto rc = RefCounter->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Unref(): %" PRId32 " -> %" PRId32, this, rc, rc - 1);
#endif
        RefCounter->Unref();
    }

    //! Returns current number of references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    inline NDetail::TNonVolatileCounter GetRefCount() const
    {
        return RefCounter->GetRefCount();
    }

    //! Returns pointer to the underlying reference counter of the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes and for TWeakPtr.
     */
    inline NDetail::TRefCounter* GetRefCounter() const
    {
        return RefCounter;
    }

    //! See #TIntrinsicRefCounted::DangerousGetPtr.
    template <class T>
    static ::NYT::TIntrusivePtr<T> DangerousGetPtr(T* object)
    {
        return
            object->RefCounter->TryRef()
            ? ::NYT::TIntrusivePtr<T>(object, false)
            : ::NYT::TIntrusivePtr<T>();
    }

private:
    mutable NDetail::TRefCounter* RefCounter;

};

//! Base class for all reference-counted objects with intrinsic reference counting.
class TIntrinsicRefCounted
    : public TRefCountedBase
{
public:
    TIntrinsicRefCounted();
    virtual ~TIntrinsicRefCounted();

    //! Increments the reference counter.
    inline void Ref() const // noexcept
    {
        auto rc = RefCounter++;

#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        ::std::fprintf(stderr, "=== %p === Ref(): %" PRId64 " -> %" PRId64, this, rc, rc + 1);
#endif
        YASSERT(rc > 0);
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
        auto rc = RefCounter--;

#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        ::std::fprintf(stderr, "=== %p === Unref(): %" PRId64 " -> %" PRId64, this, rc, rc - 1);
#endif
        YASSERT(rc > 0);
        if (rc == 1) {
            delete this;
        }
    }

    //! Returns current number of references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    inline NDetail::TNonVolatileCounter GetRefCount() const // noexcept
    {
        return RefCounter.load();
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
    template <class T>
    static ::NYT::TIntrusivePtr<T> DangerousGetPtr(T* object)
    {
        return
            NDetail::AtomicallyIncrementIfNonZero(object->RefCounter) > 0
            ? ::NYT::TIntrusivePtr<T>(object, false)
            : ::NYT::TIntrusivePtr<T>();
    }

private:
    mutable std::atomic<NDetail::TNonVolatileCounter> RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
