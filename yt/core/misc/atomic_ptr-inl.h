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

template <class T>
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
            auto* refCounter = GetAtomicRefCounter(ptr);
            (*refCounter->Deleter)(refCounter);
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRefCountedPtr<T>::TRefCountedPtr(std::nullptr_t)
{ }

template <class T>
TRefCountedPtr<T>::TRefCountedPtr(T* obj, bool addReference)
    : Ptr_(addReference ? AcquireRef(obj) : obj)
{ }

template <class T>
TRefCountedPtr<T>::TRefCountedPtr(const THazardPtr<T>& ptr)
    : Ptr_(AcquireRef(ptr))
{ }

template <class T>
TRefCountedPtr<T>::TRefCountedPtr(const TRefCountedPtr<T>& other)
    : TRefCountedPtr(other.Ptr_)
{ }

template <class T>
TRefCountedPtr<T>::TRefCountedPtr(TRefCountedPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T>
TRefCountedPtr<T>::~TRefCountedPtr()
{
    if (Ptr_) {
        ReleaseRef(Ptr_);
    }
}

template <class T>
TRefCountedPtr<T>& TRefCountedPtr<T>::operator=(TRefCountedPtr other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T>
TRefCountedPtr<T>& TRefCountedPtr<T>::operator=(std::nullptr_t)
{
    Exchange(TRefCountedPtr());
    return *this;
}

template <class T>
TRefCountedPtr<T> TRefCountedPtr<T>::Exchange(TRefCountedPtr&& other)
{
    auto oldPtr = Ptr_;
    Ptr_ = other.Ptr_;
    other.Ptr_ = nullptr;
    return TRefCountedPtr(oldPtr, false);
}

template <class T>
T* TRefCountedPtr<T>::Release()
{
    auto ptr = Ptr_;
    Ptr_ = nullptr;
    return ptr;
}

template <class T>
T* TRefCountedPtr<T>::Get() const
{
    return Ptr_;
}

template <class T>
T& TRefCountedPtr<T>::operator*() const
{
    YT_ASSERT(Ptr_);
    return *Ptr_;
}

template <class T>
T* TRefCountedPtr<T>::operator->() const
{
    YT_ASSERT(Ptr_);
    return Ptr_;
}

template <class T>
TRefCountedPtr<T>::operator bool() const
{
    return Ptr_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T, class... As>
TRefCountedPtr<T> ConstructObject(void* ptr, TDeleterBase* deleter, As&&... args)
{
    auto* refCounter = static_cast<TAtomicRefCounter*>(ptr);
    auto* object = reinterpret_cast<T*>(refCounter + 1);

    // Move TAtomicRefCounter out?
    new (refCounter) TAtomicRefCounter(deleter);

    try {
        new (object) T(std::forward<As>(args)...);
    } catch (const std::exception& ex) {
        // Do not forget to free the memory.
        (*deleter)(ptr);
        throw;
    }

    return TRefCountedPtr<T>(object, false);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class T, class TAllocator, class... As>
TRefCountedPtr<T> CreateObjectWithExtraSpace(
    TAllocator* allocator,
    size_t extraSpaceSize,
    As&&... args)
{
    auto totalSize = sizeof(TAtomicRefCounter) + sizeof(T) + extraSpaceSize;
    void* ptr = allocator->Allocate(totalSize);
    // Where to get deleter? Supply it with args or return from allocator?
    auto* deleter = allocator->GetDeleter(totalSize);
    return ConstructObject<T>(ptr, deleter, std::forward<As>(args)...);
}

template <class T, class... As>
TRefCountedPtr<T> CreateObject(As&&... args)
{
    auto* ptr = NYTAlloc::AllocateConstSize<sizeof(TAtomicRefCounter) + sizeof(T)>();
    return ConstructObject<T>(ptr, &DefaultDeleter, std::forward<As>(args)...);
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

template <class T>
bool operator==(const TRefCountedPtr<T>& lhs, const TRefCountedPtr<T>& rhs)
{
    return lhs.Get() == rhs.Get();
}

template <class T>
bool operator!=(const TRefCountedPtr<T>& lhs, const TRefCountedPtr<T>& rhs)
{
    return lhs.Get() != rhs.Get();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAtomicPtr<T>::TAtomicPtr(std::nullptr_t)
{ }

template <class T>
TAtomicPtr<T>::TAtomicPtr(TRefCountedPtr<T> other)
    : Ptr_(other.Release())
{ }

template <class T>
TAtomicPtr<T>::TAtomicPtr(TAtomicPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T>
TAtomicPtr<T>::~TAtomicPtr()
{
    auto ptr = Ptr_.load();
    if (ptr) {
        ReleaseRef(ptr);
    }
}

template <class T>
TAtomicPtr<T>& TAtomicPtr<T>::operator=(TRefCountedPtr<T> other)
{
    Exchange(std::move(other));
    return *this;
}

template <class T>
TAtomicPtr<T>& TAtomicPtr<T>::operator=(std::nullptr_t)
{
    Exchange(TRefCountedPtr<T>());
    return *this;
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::Release()
{
    return Exchange(TRefCountedPtr<T>());
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::AcquireWeak() const
{
    auto hazardPtr = THazardPtr<T>::Acquire([&] {
        return Ptr_.load(std::memory_order_relaxed);
    });
    return TRefCountedPtr(hazardPtr);
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::Acquire() const
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

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::Exchange(TRefCountedPtr<T>&& other)
{
    auto oldPtr = Ptr_.exchange(other.Release());
    return TRefCountedPtr<T>(oldPtr, false);
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::SwapIfCompare(THazardPtr<T>& compare, TRefCountedPtr<T> target)
{
    auto comparePtr = compare.Get();
    auto targetPtr = target.Get();

    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        target.Release();
        return TRefCountedPtr<T>(comparePtr, false);
    } else {
        compare.Reset();
        compare = THazardPtr<T>::Acquire([&] {
            return Ptr_.load(std::memory_order_relaxed);
        }, comparePtr);
    }

    return TRefCountedPtr<T>();
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::SwapIfCompare(T* comparePtr, TRefCountedPtr<T> target)
{
    auto targetPtr = target.Get();

    static const auto& Logger = LockFreePtrLogger;

    auto savedPtr = comparePtr;
    if (Ptr_.compare_exchange_strong(comparePtr, targetPtr)) {
        YT_LOG_TRACE("CAS succeeded (Compare: %v, Target: %v)",
            comparePtr,
            targetPtr);
        target.Release();
        return TRefCountedPtr<T>(comparePtr, false);
    } else {
        YT_LOG_TRACE("CAS failed (Current: %v, Compare: %v, Target: %v)",
            comparePtr,
            savedPtr,
            targetPtr);
    }

    // TODO(lukyan): Use ptr if compare_exchange_strong fails?
    return TRefCountedPtr<T>();
}

template <class T>
TRefCountedPtr<T> TAtomicPtr<T>::SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T> target)
{
    return SwapIfCompare(compare.Get(), std::move(target));
}

template <class T>
bool TAtomicPtr<T>::SwapIfCompare(const TRefCountedPtr<T>& compare, TRefCountedPtr<T>* target)
{
     auto ptr = compare.Get();
    if (Ptr_.compare_exchange_strong(ptr, target->Ptr_)) {
        target->Ptr_ = ptr;
        return true;
    }
    return false;
}

template <class T>
TAtomicPtr<T>::operator bool() const
{
    return Ptr_ != nullptr;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool operator==(const TAtomicPtr<T>& lhs, const TRefCountedPtr<T>& rhs)
{
    return lhs.Ptr_.load() == rhs.Get();
}

template <class T>
bool operator==(const TRefCountedPtr<T>& lhs, const TAtomicPtr<T>& rhs)
{
    return lhs.Get() == rhs.Ptr_.load();
}

template <class T>
bool operator!=(const TAtomicPtr<T>& lhs, const TRefCountedPtr<T>& rhs)
{
    return lhs.Ptr_.load() != rhs.Get();
}

template <class T>
bool operator!=(const TRefCountedPtr<T>& lhs, const TAtomicPtr<T>& rhs)
{
    return lhs.Get() != rhs.Ptr_.load();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
