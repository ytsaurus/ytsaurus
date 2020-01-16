#pragma once
#ifndef ATOMIC_PTR_INL_H_
#error "Direct inclusion of this file is not allowed, include atomic_ptr.h"
// For the sake of sane code completion.
#include "atomic_ptr.h"
#endif
#undef ATOMIC_PTR_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

TAtomicRefCounter* GetAtomicRefCounter(void* ptr)
{
    return static_cast<TAtomicRefCounter*>(ptr) - 1;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* AcquireRef(T* ptr)
{
    if (ptr) {
        auto* refCounter = GetAtomicRefCounter(ptr);

        auto oldStrongCount = refCounter->Count.fetch_add(1, std::memory_order_relaxed);
        YT_ASSERT(oldStrongCount > 0);

        return ptr;
    }

    return nullptr;
}

template <class T>
T* AcquireRef(const THazardPtr<T>& ptr)
{
    if (ptr) {
        auto* refCounter = GetAtomicRefCounter(ptr.Get());

        auto count = refCounter->Count.load();
        while (count > 0 && !refCounter->Count.compare_exchange_weak(count, count + 1));

        if (count > 0) {
            return ptr.Get();
        } else {
            static const auto& Logger = LockFreePtrLogger;
            YT_LOG_TRACE("Failed to acquire ref (Ptr: %v)",
                ptr.Get());
        }
    }

    return nullptr;
}

template <class TTraits, class T>
void ReleaseRef(T* ptr)
{
    auto* refCounter = GetAtomicRefCounter(ptr);
    auto oldStrongCount = refCounter->Count.fetch_sub(1, std::memory_order_release);
    YT_ASSERT(oldStrongCount > 0);

    if (oldStrongCount == 1) {
        refCounter->Count.load(std::memory_order_acquire);

        // Destroy object.
        ptr->~T();

        // Free memory.
        ScheduleObjectDeletion(ptr, [] (void* ptr) {
            TTraits::Free(GetAtomicRefCounter(ptr));
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::TRefCountedPtr(std::nullptr_t)
{ }

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::TRefCountedPtr(T* obj, bool addReference)
    : Ptr_(addReference ? AcquireRef(obj) : obj)
{ }

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::TRefCountedPtr(const THazardPtr<T>& ptr)
    : Ptr_(AcquireRef(ptr))
{ }

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::TRefCountedPtr(const TRefCountedPtr<T, TTraits>& other)
    : TRefCountedPtr(other.Ptr_)
{ }

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::TRefCountedPtr(TRefCountedPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::~TRefCountedPtr()
{
    if (Ptr_) {
        ReleaseRef<TTraits>(Ptr_);
    }
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>& TRefCountedPtr<T, TTraits>::operator=(TRefCountedPtr other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>& TRefCountedPtr<T, TTraits>::operator=(std::nullptr_t)
{
    Exchange(TRefCountedPtr());
    return *this;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TRefCountedPtr<T, TTraits>::Exchange(TRefCountedPtr&& other)
{
    auto oldPtr = Ptr_;
    Ptr_ = other.Ptr_;
    other.Ptr_ = nullptr;
    return TRefCountedPtr(oldPtr, false);
}

template <class T, class TTraits>
T* TRefCountedPtr<T, TTraits>::Release()
{
    auto ptr = Ptr_;
    Ptr_ = nullptr;
    return ptr;
}

template <class T, class TTraits>
T* TRefCountedPtr<T, TTraits>::Get() const
{
    return Ptr_;
}

template <class T, class TTraits>
T& TRefCountedPtr<T, TTraits>::operator*() const
{
    YT_ASSERT(Ptr_);
    return *Ptr_;
}

template <class T, class TTraits>
T* TRefCountedPtr<T, TTraits>::operator->() const
{
    YT_ASSERT(Ptr_);
    return Ptr_;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits>::operator bool() const
{
    return Ptr_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class TTraits, class... As>
TRefCountedPtr<T, TTraits> ConstructObject(void* ptr, As&&... args)
{
    auto* refCounter = static_cast<TAtomicRefCounter*>(ptr);
    auto* object = reinterpret_cast<T*>(refCounter + 1);

    try {
        new (refCounter) TAtomicRefCounter();
        new (object) T(std::forward<As>(args)...);
    } catch (const std::exception& ex) {
        // Do not forget to free the memory.
        TTraits::Free(ptr);
        throw;
    }

    return TRefCountedPtr<T, TTraits>(object, false);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits, class... As>
TRefCountedPtr<T, TTraits> CreateObjectWithExtraSpace(
    TTraits* traits,
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = sizeof(TAtomicRefCounter) + sizeof(T) + extraSpaceSize;
    void* ptr = traits->Allocate(totalSize);
    return ConstructObject<T, TTraits>(ptr, std::forward<As>(args)...);
}

template <class T, class... As>
TRefCountedPtr<T> CreateObject(As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(TAtomicRefCounter) + sizeof(T)>();
    return ConstructObject<T, TDefaultAllocator>(ptr, std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

template <class U>
bool operator==(const THazardPtr<U>& lhs, const U* rhs)
{
    return lhs.Get() == rhs;
}

template <class U>
bool operator!=(const THazardPtr<U>& lhs, const U* rhs)
{
    return lhs.Get() != rhs;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
bool operator==(const TRefCountedPtr<T, TTraits>& lhs, const TRefCountedPtr<T, TTraits>& rhs)
{
    return lhs.Get() == rhs.Get();
}

template <class T, class TTraits>
bool operator!=(const TRefCountedPtr<T, TTraits>& lhs, const TRefCountedPtr<T, TTraits>& rhs)
{
    return lhs.Get() != rhs.Get();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
TAtomicPtr<T, TTraits>::TAtomicPtr(std::nullptr_t)
{ }

template <class T, class TTraits>
TAtomicPtr<T, TTraits>::TAtomicPtr(TRefCountedPtr<T, TTraits> other)
    : Ptr_(other.Release())
{ }

template <class T, class TTraits>
TAtomicPtr<T, TTraits>::TAtomicPtr(TAtomicPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T, class TTraits>
TAtomicPtr<T, TTraits>::~TAtomicPtr()
{
    auto ptr = Ptr_.load();
    if (ptr) {
        ReleaseRef<TTraits>(ptr);
    }
}

template <class T, class TTraits>
TAtomicPtr<T, TTraits>& TAtomicPtr<T, TTraits>::operator=(TRefCountedPtr<T, TTraits> other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T, class TTraits>
TAtomicPtr<T, TTraits>& TAtomicPtr<T, TTraits>::operator=(std::nullptr_t)
{
    Exchange(TRefCountedPtr<T, TTraits>());
    return *this;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::Release()
{
    return Exchange(TRefCountedPtr<T, TTraits>());
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::AcquireWeak() const
{
    auto hazardPtr = THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order_relaxed);
    });
    return TRefCountedPtr(hazardPtr);
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::Acquire() const
{
    while (auto hazardPtr = THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order_relaxed);
    })) {
        if (auto ptr = TRefCountedPtr(hazardPtr)) {
            return ptr;
        }
    }

    return nullptr;
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::Exchange(TRefCountedPtr<T, TTraits>&& other)
{
    auto oldPtr = Ptr_.exchange(other.Release());
    return TRefCountedPtr<T, TTraits>(oldPtr, false);
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::SwapIfCompare(THazardPtr<T>& compare, TRefCountedPtr<T, TTraits> target)
{
    auto comparePtr = compare.Get();
    auto targetPtr = target.Get();

    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        target.Release();
        return TRefCountedPtr<T, TTraits>(comparePtr, false);
    } else {
        compare.Reset();
        compare = THazardPtr<T>::Acquire([&] {
            return Ptr_.load(std::memory_order_relaxed);
        }, comparePtr);
    }

    return TRefCountedPtr<T, TTraits>();
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::SwapIfCompare(T* comparePtr, TRefCountedPtr<T, TTraits> target)
{
    auto targetPtr = target.Get();

    static const auto& Logger = LockFreePtrLogger;

    auto savedPtr = comparePtr;
    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        YT_LOG_TRACE("CAS succeeded (Compare: %v, Target: %v)",
            comparePtr,
            targetPtr);
        target.Release();
        return TRefCountedPtr<T, TTraits>(comparePtr, false);
    } else {
        YT_LOG_TRACE("CAS failed (Current: %v, Compare: %v, Target: %v)",
            comparePtr,
            savedPtr,
            targetPtr);
    }

    // TODO(lukyan): Use ptr if compare_exchange_strong fails?
    return TRefCountedPtr<T, TTraits>();
}

template <class T, class TTraits>
TRefCountedPtr<T, TTraits> TAtomicPtr<T, TTraits>::SwapIfCompare(const TRefCountedPtr<T, TTraits>& compare, TRefCountedPtr<T, TTraits> target)
{
    return SwapIfCompare(compare.Get(), std::move(target));
}

template <class T, class TTraits>
bool TAtomicPtr<T, TTraits>::SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T, TTraits>* target)
{
     auto ptr = compare.Get();
    if (Ptr_.compare_exchange_strong(ptr, target->Ptr_)) {
        target->Ptr_ = ptr;
        return true;
    }
    return false;
}

template <class T, class TTraits>
TAtomicPtr<T, TTraits>::operator bool() const
{
    return Ptr_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
bool operator==(const TAtomicPtr<T, TTraits>& lhs, const TRefCountedPtr<T, TTraits>& rhs)
{
    return lhs.Ptr_.load() == rhs.Get();
}

template <class T, class TTraits>
bool operator==(const TRefCountedPtr<T, TTraits>& lhs, const TAtomicPtr<T, TTraits>& rhs)
{
    return lhs.Get() == rhs.Ptr_.load();
}

template <class T, class TTraits>
bool operator!=(const TAtomicPtr<T, TTraits>& lhs, const TRefCountedPtr<T, TTraits>& rhs)
{
    return lhs.Ptr_.load() != rhs.Get();
}

template <class T, class TTraits>
bool operator!=(const TRefCountedPtr<T, TTraits>& lhs, const TAtomicPtr<T, TTraits>& rhs)
{
    return lhs.Get() != rhs.Ptr_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
