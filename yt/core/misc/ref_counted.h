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

static inline int AtomicallyIncrementIfNonZero(std::atomic<int>& atomicCounter);

//! An atomic reference counter for extrinsic reference counting.
class TRefCounter
{
public:
    TRefCounter(TExtrinsicRefCounted* object)
        : StrongCount_(1)
        , WeakCount_(1)
        , That_(object)
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
        auto oldStrongCount = StrongCount_++;
        YASSERT(oldStrongCount > 0 && WeakCount_.load() > 0);
    }

    //! Removes a strong reference from the counter.
    inline void Unref() // noexcept
    {
        auto oldStrongCount = StrongCount_--;
        YASSERT(oldStrongCount > 0);

        if (oldStrongCount == 1) {
            Dispose();

            auto oldWeakCount = WeakCount_--;
            YASSERT(oldWeakCount > 0);

            if (oldWeakCount == 1) {
                Destroy();
            }
        }
    }

    //! Tries to add a strong reference to the counter.
    inline bool TryRef() // noexcept
    {
        YASSERT(WeakCount_.load() > 0);
        return AtomicallyIncrementIfNonZero(StrongCount_) > 0;
    }

    //! Adds a weak reference to the counter.
    inline void WeakRef() // noexcept
    {
        auto oldWeakCount = WeakCount_++;
        YASSERT(oldWeakCount > 0);
    }

    //! Removes a weak reference from the counter.
    inline void WeakUnref() // noexcept
    {
        auto oldWeakCount = WeakCount_--;
        YASSERT(oldWeakCount > 0);
        if (oldWeakCount == 1) {
            Destroy();
        }
    }

    //! Returns the current number of strong references.
    int GetRefCount() const // noexcept
    {
        return StrongCount_.load();
    }

    //! Returns the current number of weak references.
    int GetWeakRefCount() const // noexcept
    {
        return WeakCount_.load();
    }

private:
    // Explicitly prohibit forbidden constructors and operators.
    TRefCounter();
    TRefCounter(const TRefCounter&);
    TRefCounter(const TRefCounter&&);
    TRefCounter& operator=(const TRefCounter&);
    TRefCounter& operator=(const TRefCounter&&);

    //! Number of strong references.
    std::atomic<int> StrongCount_;
    //! Number of weak references plus one if there is at least one strong reference.
    std::atomic<int> WeakCount_;
    //! The object.
    TExtrinsicRefCounted* That_;
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
    TRefCountedTypeCookie TypeCookie_ = NullRefCountedTypeCookie;
    size_t InstanceSize_ = 0;

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
        auto refCount = RefCounter_->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Ref(): %" PRId32 " -> %" PRId32, this, refCount, refCount + 1);
#endif
        RefCounter_->Ref();
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        auto refCount = RefCounter_->GetRefCount();
        ::std::fprintf(stderr, "=== %p === Unref(): %" PRId32 " -> %" PRId32, this, refCount, refCount - 1);
#endif
        RefCounter_->Unref();
    }

    //! Returns current number of references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    inline int GetRefCount() const
    {
        return RefCounter_->GetRefCount();
    }

    //! Returns pointer to the underlying reference counter of the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes and for TWeakPtr.
     */
    inline NDetail::TRefCounter* GetRefCounter() const
    {
        return RefCounter_;
    }

    //! See #TIntrinsicRefCounted::DangerousGetPtr.
    template <class T>
    static ::NYT::TIntrusivePtr<T> DangerousGetPtr(T* object)
    {
        return
            object->RefCounter_->TryRef()
            ? ::NYT::TIntrusivePtr<T>(object, false)
            : ::NYT::TIntrusivePtr<T>();
    }

private:
    mutable NDetail::TRefCounter* RefCounter_;

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
        auto oldRefCount = RefCounter_++;

#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        ::std::fprintf(stderr, "=== %p === Ref(): %" PRId64 " -> %" PRId64, this, oldRefCount, oldRefCount + 1);
#endif
        YASSERT(oldRefCount > 0);
    }

    //! Decrements the reference counter.
    inline void Unref() const // noexcept
    {
        auto oldRefCount = RefCounter_--;

#ifdef YT_ENABLE_REF_COUNTED_DEBUGGING
        ::std::fprintf(stderr, "=== %p === Unref(): %" PRId64 " -> %" PRId64, this, oldRefCount, oldRefCount - 1);
#endif
        YASSERT(oldRefCount > 0);
        if (oldRefCount == 1) {
            delete this;
        }
    }

    //! Returns current number of references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    inline int GetRefCount() const // noexcept
    {
        return RefCounter_.load();
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
            NDetail::AtomicallyIncrementIfNonZero(object->RefCounter_) > 0
            ? ::NYT::TIntrusivePtr<T>(object, false)
            : ::NYT::TIntrusivePtr<T>();
    }

private:
    mutable std::atomic<int> RefCounter_;

};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl

namespace NDetail {

inline int AtomicallyIncrementIfNonZero(std::atomic<int>& atomicCounter)
{
    // Atomically performs the following:
    // { auto v = *p; if (v != 0) ++(*p); return v; }
    while (true) {
        auto value = atomicCounter.load();

        if (value == 0) {
            return value;
        }

        if (atomicCounter.compare_exchange_strong(value, value + 1)) {
            return value;
        }
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
