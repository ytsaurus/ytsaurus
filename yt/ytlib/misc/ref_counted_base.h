#pragma once

#include "common.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TExtrinsicRefCountedBase;
class TIntrinsicRefCountedBase;

namespace NDetail {
    typedef intptr_t TNonVolatileCounter;
    typedef volatile intptr_t TVolatileCounter;

    static_assert(sizeof(TNonVolatileCounter) == sizeof(void*),
        "TNonVolatileCounter should be the same size as raw pointer.");
    static_assert(sizeof(TVolatileCounter) == sizeof(void*),
        "TVolatileCounter should be the same size as raw pointer.");

#if defined(__GNUC__)
    // Note that returning previous value is a micro-tiny-bit faster.
    // (see http://lwn.net/Articles/256433/)

    inline TNonVolatileCounter AtomicallyFetch(const TVolatileCounter* p)
    {
        return __sync_fetch_and_add(const_cast<TVolatileCounter*>(p), 0);
    }

    inline TNonVolatileCounter AtomicallyIncrement(TVolatileCounter* p)
    {
        // Atomically performs the following:
        // { return (*p)++; }
        return __sync_fetch_and_add(p, +1);
    }

    inline TNonVolatileCounter AtomicallyDecrement(TVolatileCounter* p)
    {
        return __sync_fetch_and_add(p, -1);
    }

    inline TNonVolatileCounter AtomicallyIncrementIfNonZero(TVolatileCounter* p)
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
#elif defined(__MSVC__)
    // Fallback to Arcadia's implementation (just to preserve space).
    inline TNonVolatileCounter AtomicallyFetch(TVolatileCounter* p)
    {
        return AtomicAdd(*p, 0);
    }

    inline TNonVolatileCounter AtomicallyIncrement(TVolatileCounter* p)
    {
        return AtomicIncrement(*p) - 1;
    }

    inline TNonVolatileCounter AtomicallyDecrement(TVolatileCounter* p)
    {
        return AtomicDecrement(*p) + 1;
    }

    inline TNonVolatileCounter AtomicallyIncrementIfNonZero(TVolatileCounter* p)
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

    //! An atomical reference counter.
    /*!
     * Note that currently Dispose() and Destroy() methods are non-virtual
     * to avoid extra indirection. In case of further derivation from this class
     * one have to turn these methods to virtual ones.
     */
    class TRefCounter
    {
    public:
        TRefCounter(TExtrinsicRefCountedBase* object)
            : StrongCount(1)
            , WeakCount(1)
            , Object(object)
        { }

        /* virtual */ ~TRefCounter()
        { }

        //! This method is called when there are no strong references remain and
        //! all allocated resources should be disposed (technically this means
        //! that there are no more strong references).
        inline void Dispose();

        //! This method is called when the counter is about to be destroyed
        //! (technically this means that there are no more neither strong
        //! nor weak references).
        inline void Destroy();

        //! Adds a strong reference to the counter.
        inline void Ref() // noexcept
        {
            AtomicallyIncrement(&StrongCount);
        }

        //! Tries to add a strong reference to the counter.
        inline bool TryRef() _warn_unused_result // noexcept
        {
            return AtomicallyIncrementIfNonZero(&StrongCount) > 0;
        }

        //! Removes a strong reference to the counter.
        inline void UnRef() // noexcept
        {
            if (AtomicallyDecrement(&StrongCount) == 1) {
                Dispose();

                if (AtomicallyDecrement(&WeakCount) == 1) {
                    Destroy();
                }
            }
        }

        //! Adds a weak reference to the counter.
        inline void WeakRef() // noexcept
        {
            AtomicallyIncrement(&WeakCount);
        }

        //! Removes a weak reference to the counter.
        inline void WeakUnRef() // noexcept
        {
            if (AtomicallyDecrement(&WeakCount) == 1) {
                Destroy();
            }
        }

        //! Returns current number of strong references.
        int GetRefCount() const // noexcept
        {
            return AtomicallyFetch(&StrongCount);
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
        //! Object to be protected.
        TExtrinsicRefCountedBase* Object;
    };
} // namespace NDetail

//! Base for reference-counted objects with extrinsic reference counting.
class TExtrinsicRefCountedBase
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TExtrinsicRefCountedBase()        
        : RefCounter(new NDetail::TRefCounter(this))
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    virtual ~TExtrinsicRefCountedBase()
    {
        YASSERT(RefCounter->GetRefCount() == 0);
#ifdef ENABLE_REF_COUNTED_TRACKING
        YASSERT(Cookie);
        TRefCountedTracker::Get()->Unregister(Cookie);
#endif
    }

#ifdef ENABLE_REF_COUNTED_TRACKING
    inline void BindToCookie(TRefCountedTracker::TCookie cookie)
    {
        YASSERT(RefCounter->GetRefCount() > 0);
        YASSERT(!Cookie);
        Cookie = cookie;

        TRefCountedTracker::Get()->Register(Cookie);
    }
#endif

    //! Increments the reference counter.
    inline void Ref() // noexcept
    {
        YASSERT(RefCounter->GetRefCount() > 0);
        RefCounter->Ref();
    }

    //! Decrements the reference counter.
    inline void UnRef() // noexcept
    {
        YASSERT(RefCounter->GetRefCount() > 0);
        RefCounter->UnRef();
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

private:
    NDetail::TRefCounter* RefCounter;
#ifdef ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::TCookie Cookie;
#endif

};

//! Base for reference-counted objects with intrinsic reference counting.
class TIntrinsicRefCountedBase
    : private TNonCopyable
{
public:
    //! Constructs an instance.
    TIntrinsicRefCountedBase()
        : RefCounter(1)
#ifdef ENABLE_REF_COUNTED_TRACKING
        , Cookie(NULL)
#endif
    { }

    //! Destroys the instance.
    virtual ~TIntrinsicRefCountedBase()
    {
        YASSERT(NDetail::AtomicallyFetch(&RefCounter) == 0);
#ifdef ENABLE_REF_COUNTED_TRACKING
        YASSERT(Cookie);
        TRefCountedTracker::Get()->Unregister(Cookie);
#endif
    }

#ifdef ENABLE_REF_COUNTED_TRACKING
    //! Called from #New functions to initialize the tracking cookie.
    inline void BindToCookie(TRefCountedTracker::TCookie cookie)
    {
        YASSERT(NDetail::AtomicallyFetch(&RefCounter) > 0);
        YASSERT(!Cookie);
        Cookie = cookie;

        TRefCountedTracker::Get()->Register(Cookie);
    }
#endif

    //! Increments the reference counter.
    inline void Ref() // noexcept
    {
        YVERIFY(NDetail::AtomicallyIncrement(&RefCounter) > 0);
    }

    //! Decrements the reference counter.
    inline void UnRef() // noexcept
    {
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
    template<class T>
    static TIntrusivePtr<T> DangerousGetPtr(T* object)
    {
        if (NDetail::AtomicallyIncrementIfNonZero(&object->RefCounter)) {
            return TIntrusivePtr<T>(object, false);
        }
    }

private:
    NDetail::TVolatileCounter RefCounter;
#ifdef ENABLE_REF_COUNTED_TRACKING
    TRefCountedTracker::TCookie Cookie;
#endif

};

namespace NDetail {
    void TRefCounter::Dispose()
    {
        delete Object;
    }

    void TRefCounter::Destroy()
    {
        delete this;
    }
} // namespace NDetail

// TODO(sandello): This is compatibility line.
typedef TIntrinsicRefCountedBase TRefCountedBase;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
