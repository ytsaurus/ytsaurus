#pragma once

#include "intrusive_ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TExtrinsicRefCounted;
class TIntrinsicRefCounted;

namespace NDetail {
    typedef intptr_t TNonVolatileCounter;
    typedef volatile intptr_t TVolatileCounter;

    static_assert(sizeof(TNonVolatileCounter) == sizeof(void*),
        "TNonVolatileCounter should be the same size as a raw pointer.");
    static_assert(sizeof(TVolatileCounter) == sizeof(void*),
        "TVolatileCounter should be the same size as a raw pointer.");

#if defined(__GNUC__)
    // Note that returning previous value is a micro-tiny-bit faster.
    // (see http://lwn.net/Articles/256433/)

    static inline TNonVolatileCounter AtomicallyFetch(const TVolatileCounter* p)
    {
        return __sync_fetch_and_add(const_cast<TVolatileCounter*>(p), 0);
    }

    static inline TNonVolatileCounter AtomicallyIncrement(TVolatileCounter* p)
    {
        // Atomically performs the following:
        // { return (*p)++; }
        return __sync_fetch_and_add(p, +1);
    }

    static inline TNonVolatileCounter AtomicallyDecrement(TVolatileCounter* p)
    {
        // Atomically performs the following:
        // { return (*p)--; }
        return __sync_fetch_and_add(p, -1);
    }

    static inline TNonVolatileCounter AtomicallyIncrementIfNonZero(TVolatileCounter* p)
    {
        // Atomically performs the following:
        // { auto v = *p; if (v != 0) ++(*p); return v; }
        TNonVolatileCounter v = *p;

        for (;;) {
            if (v == 0) {
                return v;
            }

            TNonVolatileCounter w = __sync_val_compare_and_swap(p, v, v + 1);

            if (w == v) {
                return v; // CAS was successful.
            } else {
                v = w; // CAS was unsuccessful.
            }
        }
    }
#else
    // Fallback to Arcadia's implementation (efficiency is not crucial here).

    static inline TNonVolatileCounter AtomicallyFetch(const TVolatileCounter* p)
    {
        return AtomicAdd(*const_cast<TVolatileCounter*>(p), 0);
    }

    static inline TNonVolatileCounter AtomicallyIncrement(TVolatileCounter* p)
    {
        return AtomicIncrement(*p) - 1;
    }

    static inline TNonVolatileCounter AtomicallyDecrement(TVolatileCounter* p)
    {
        return AtomicDecrement(*p) + 1;
    }

    static inline TNonVolatileCounter AtomicallyIncrementIfNonZero(TVolatileCounter* p)
    {
        for (;;) {
            TNonVolatileCounter v = *p;

            if (v == 0) {
                return v;
            }

            if (AtomicCas(p, v + 1, v)) {
                return v;
            }
        }
    }
#endif

    //! An atomical reference counter for extrinsic reference counting.
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
            YASSERT(StrongCount > 0 && WeakCount > 0);
            AtomicallyIncrement(&StrongCount);
        }

        //! Removes a strong reference to the counter.
        inline void Unref() // noexcept
        {
            YASSERT(StrongCount > 0 && WeakCount > 0);
            if (AtomicallyDecrement(&StrongCount) == 1) {
                Dispose();

                if (AtomicallyDecrement(&WeakCount) == 1) {
                    Destroy();
                }
            }
        }

        //! Tries to add a strong reference to the counter.
        inline bool TryRef() // noexcept
        {
            YASSERT(WeakCount > 0);
            return AtomicallyIncrementIfNonZero(&StrongCount) > 0;
        }

        //! Adds a weak reference to the counter.
        inline void WeakRef() // noexcept
        {
            YASSERT(WeakCount > 0);
            AtomicallyIncrement(&WeakCount);
        }

        //! Removes a weak reference to the counter.
        inline void WeakUnref() // noexcept
        {
            YASSERT(WeakCount > 0);
            if (AtomicallyDecrement(&WeakCount) == 1) {
                Destroy();
            }
        }

        //! Returns current number of strong references.
        int GetRefCount() const // noexcept
        {
            return AtomicallyFetch(&StrongCount);
        }

        //! Returns current number of weak references.
        int GetWeakRefCount() const // noexcept
        {
            return AtomicallyFetch(&WeakCount);
        }

    private:
        // Explicitly prohibit forbidden constructors and operators.
        TRefCounter();
        TRefCounter(const TRefCounter&);
        TRefCounter(const TRefCounter&&);
        TRefCounter& operator=(const TRefCounter&);
        TRefCounter& operator=(const TRefCounter&&);

        //! Number of strong references.
        TVolatileCounter StrongCount;
        //! Number of weak references plus one if there is at least one strong reference.
        TVolatileCounter WeakCount;
        //! The object.
        TExtrinsicRefCounted* that;
    };
} // namespace NDetail

//! Base for reference-counted objects with extrinsic reference counting.
class TExtrinsicRefCounted
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TExtrinsicRefCounted()        
        : RefCounter(new NDetail::TRefCounter(this))
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    virtual ~TExtrinsicRefCounted();

#ifdef ENABLE_REF_COUNTED_TRACKING
    //! Called from #New functions to initialize the tracking cookie.
    void BindToCookie(void* cookie);
#endif

    //! Increments the reference counter.
    inline void Ref() const // noexcept
    {
#ifdef ENABLE_REF_COUNTED_DEBUGGING
        auto rc = RefCounter->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Ref(): %d -> %d", this, rc, rc + 1);
#endif
        RefCounter->Ref();
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
#ifdef ENABLE_REF_COUNTED_DEBUGGING
        auto rc = RefCounter->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Unref(): %d -> %d", this, rc, rc - 1);
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
#ifdef ENABLE_REF_COUNTED_TRACKING
    void* Cookie;
#endif

};

//! Base for reference-counted objects with intrinsic reference counting.
class TIntrinsicRefCounted
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TIntrinsicRefCounted()
        : RefCounter(1)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    //! Destroys the instance.
    virtual ~TIntrinsicRefCounted();

#ifdef ENABLE_REF_COUNTED_TRACKING
    //! Called from #New functions to initialize the tracking cookie.
    void BindToCookie(void* cookie);
#endif

    //! Increments the reference counter.
    inline void Ref() const // noexcept
    {
#ifdef ENABLE_REF_COUNTED_DEBUGGING
        auto rc = NDetail::AtomicallyFetch(&RefCounter);
        ::std::fprintf(stderr, "=== %p === Ref(): %"PRId64" -> %"PRId64, this, rc, rc + 1);
#endif
        YASSERT(NDetail::AtomicallyFetch(&RefCounter) > 0);
        NDetail::AtomicallyIncrement(&RefCounter);
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
#ifdef ENABLE_REF_COUNTED_DEBUGGING
        auto rc = NDetail::AtomicallyFetch(&RefCounter);
        ::std::fprintf(stderr, "=== %p === Unref(): %"PRId64" -> %"PRId64, this, rc, rc - 1);
#endif
        YASSERT(NDetail::AtomicallyFetch(&RefCounter) > 0);
        if (NDetail::AtomicallyDecrement(&RefCounter) == 1) {
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
        return NDetail::AtomicallyFetch(&RefCounter);
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
            NDetail::AtomicallyIncrementIfNonZero(&object->RefCounter) > 0
            ? ::NYT::TIntrusivePtr<T>(object, false)
            : ::NYT::TIntrusivePtr<T>();
    }

private:
    mutable NDetail::TVolatileCounter RefCounter;
#ifdef ENABLE_REF_COUNTED_TRACKING
    void* Cookie;
#endif

};

// This is a reasonable default.
// For performance-critical bits of code use TIntrinsicRefCounted instead.
typedef TExtrinsicRefCounted TRefCounted;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
