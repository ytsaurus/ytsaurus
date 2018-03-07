#pragma once

#include "intrusive_ptr.h"
#include "port.h"

#include <util/generic/noncopyable.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSourceLocation;

class TRefCountedBase;

template <bool EnableWeak>
class TRefCountedImpl;

//! Default base class for all ref-counted types.
/*!
 *  Supports weak pointers.
 *
 *  Instances are created with a single memory allocation.
 */
using TRefCounted = TRefCountedImpl<true>;

//! Lightweight version of TRefCounted.
/*!
 *  Does not support weak pointers.
 *
 *  Compared to TRefCounted, Ref/unref calls are somewhat cheaper.
 *  Also instances of TSimpleRefCounted have smaller memory footprint.
 */
using TIntrinsicRefCounted = TRefCountedImpl<false>;

using TRefCountedTypeCookie = int;
const int NullRefCountedTypeCookie = -1;

using TRefCountedTypeKey = const void*;

////////////////////////////////////////////////////////////////////////////////

// Used to avoid including heavy ref_counted_tracker.h
class TRefCountedTrackerFacade
{
public:
    static TRefCountedTypeCookie GetCookie(
        TRefCountedTypeKey typeKey,
        size_t instanceSize,
        const NYT::TSourceLocation& location);

    static void AllocateInstance(TRefCountedTypeCookie cookie);
    static void FreeInstance(TRefCountedTypeCookie cookie);

    static void AllocateTagInstance(TRefCountedTypeCookie cookie);
    static void FreeTagInstance(TRefCountedTypeCookie cookie);

    static void AllocateSpace(TRefCountedTypeCookie cookie, size_t size);
    static void FreeSpace(TRefCountedTypeCookie cookie, size_t size);
    static void ReallocateSpace(TRefCountedTypeCookie cookie, size_t freedSize, size_t allocatedSize);

    // Typically invoked from GDB console.
    // Dumps the ref-counted statistics sorted by "bytes alive".
    static void Dump();
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <bool EnableWeak>
class TRefCounter;

template <>
class TRefCounter<false>
{
public:
    // Default-constructable.
    TRefCounter() = default;

    // Non-copyable, non-assignable.
    TRefCounter(const TRefCounter&) = delete;
    TRefCounter(TRefCounter&&) = delete;
    TRefCounter& operator=(const TRefCounter&) = delete;
    TRefCounter& operator=(TRefCounter&&) = delete;

    //! Adds a strong reference to the counter.
    void Ref() noexcept;

    //! Removes a strong reference from the counter.
    void Unref(const TRefCountedBase* object);

    //! Tries to add a strong reference to the counter.
    bool TryRef() noexcept;

    //! Returns the current number of strong references.
    int GetRefCount() const noexcept;

private:
    //! Number of strong references.
    std::atomic<int> StrongCount_ = {1};

    //! This method is called when there are no strong references remaining
    //! and the object's dtor must to be called and the memory must be reclaimed.
    void DestroyAndDispose(const TRefCountedBase* object);
};

template <>
class TRefCounter<true>
{
public:
    // Default-constructable.
    TRefCounter() = default;

    // Non-copyable, non-assignable.
    TRefCounter(const TRefCounter&) = delete;
    TRefCounter(TRefCounter&&) = delete;
    TRefCounter& operator=(const TRefCounter&) = delete;
    TRefCounter& operator=(TRefCounter&&) = delete;

    //! Initializes the memory region pointer.
    void SetPtr(void* ptr) noexcept;

    //! Adds a strong reference to the counter.
    void Ref() noexcept;

    //! Removes a strong reference from the counter.
    void Unref(const TRefCountedBase* object);

    //! Tries to add a strong reference to the counter.
    bool TryRef() noexcept;

    //! Adds a weak reference to the counter.
    void WeakRef() noexcept;

    //! Removes a weak reference from the counter.
    void WeakUnref();

    //! Returns the current number of strong references.
    int GetRefCount() const noexcept;

    //! Returns the current number of weak references.
    int GetWeakRefCount() const noexcept;

private:
    //! Number of strong references.
    std::atomic<int> StrongCount_ = {1};

    //! Number of weak references plus one if there is at least one strong reference.
    std::atomic<int> WeakCount_ = {1};

    //! Pointer to the start of the memory region where the object is allocated.
    void* Ptr_ = nullptr;

    //! This method is called when there are no strong references remaining
    //! and the object's dtor must be called.
    void Destroy(const TRefCountedBase* object);

    //! This method is called when neither strong
    //! nor weak references being held and the memory must be reclaimed.
    void Dispose();
};

//! Normally delegates to #TRefCountedBase::InitializeTracking.
void InitializeRefCountedTracking(
    TRefCountedBase* object,
    TRefCountedTypeCookie typeCookie);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! A technical base class for TRefCountedImpl and promise states.
class TRefCountedBase
{
public:
    TRefCountedBase() = default;
    virtual ~TRefCountedBase() noexcept = default;

    void operator delete(void* ptr) noexcept;

private:
    template <bool EnableWeak>
    friend class NDetail::TRefCounter;

    TRefCountedBase(const TRefCountedBase&) = delete;
    TRefCountedBase(TRefCountedBase&&) = delete;

    TRefCountedBase& operator=(const TRefCountedBase&) = delete;
    TRefCountedBase& operator=(TRefCountedBase&&) = delete;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
protected:
    TRefCountedTypeCookie TypeCookie_ = NullRefCountedTypeCookie;

    void FinalizeTracking();

private:
    friend void NDetail::InitializeRefCountedTracking(
        TRefCountedBase* object,
        TRefCountedTypeCookie typeCookie);

    void InitializeTracking(TRefCountedTypeCookie typeCookie);
#endif
};

////////////////////////////////////////////////////////////////////////////////

//! Base class for all reference-counted objects.
template <bool EnableWeak = true>
class TRefCountedImpl
    : public TRefCountedBase
{
public:
    TRefCountedImpl() = default;
    ~TRefCountedImpl() noexcept;

    //! Increments the reference counter.
    void Ref() const noexcept;

    //! Decrements the reference counter.
    void Unref() const;

    //! Returns current number of references to the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes.
     */
    int GetRefCount() const noexcept;

    //! Returns pointer to the underlying reference counter of the object.
    /*!
     * Note that you should never ever use this method in production code.
     * This method is mainly for debugging purposes and for TWeakPtr.
     */
    NDetail::TRefCounter<EnableWeak>* GetRefCounter() const noexcept;

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
    static TIntrusivePtr<T> DangerousGetPtr(T* object);

private:
    mutable NDetail::TRefCounter<EnableWeak> RefCounter_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define REF_COUNTED_INL_H_
#include "ref_counted-inl.h"
#undef REF_COUNTED_INL_H_
