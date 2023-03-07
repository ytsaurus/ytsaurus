#pragma once
#ifndef REF_COUNTED_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted.h"
// For the sake of sane code completion.
#include "ref_counted.h"
#endif

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr uint16_t PtrBits = 48;
constexpr uintptr_t PtrMask = (1ULL << PtrBits) - 1;

template <class T>
Y_FORCE_INLINE char* PackPointer(T* ptr, uint16_t data)
{
    return reinterpret_cast<char*>((static_cast<uintptr_t>(data) << PtrBits) | reinterpret_cast<uintptr_t>(ptr));
}

template <class T>
struct TPackedPointer
{
    uint16_t Data;
    T* Ptr;
};

template <class T>
Y_FORCE_INLINE TPackedPointer<T> UnpackPointer(void* packedPtr)
{
    auto castedPtr = reinterpret_cast<uintptr_t>(packedPtr);
    return {static_cast<uint16_t>(castedPtr >> PtrBits), reinterpret_cast<T*>(castedPtr & PtrMask)};
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class = void>
struct TMemoryReleaser
{
    static void Do(void* ptr, uint16_t /*offset*/)
    {
        TFreeMemory<T>::Do(ptr);
    }
};

using TDeleter = void (*)(void*);

void ScheduleObjectDeletion(void* ptr, TDeleter deleter);

template <class T>
struct TMemoryReleaser<T, TAcceptor<typename T::TEnableHazard>>
{
    static void Do(void* ptr, uint16_t offset)
    {
        // Base pointer is used in HazardPtr as the identity of object.
        auto* basePtr = PackPointer(static_cast<char*>(ptr) + offset, offset);

        ScheduleObjectDeletion(basePtr, [] (void* ptr) {
            // Base ptr and the beginning of allocated memory region may differ.
            auto [offset, basePtr] = UnpackPointer<char>(ptr);
            TFreeMemory<T>::Do(basePtr - offset);
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE int TRefCounter::GetRefCount() const noexcept
{
    return StrongCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE void TRefCounter::Ref() const noexcept
{
    // It is safe to use relaxed here, since new reference is always created from another live reference.
    StrongCount_.fetch_add(1, std::memory_order_relaxed);

    YT_ASSERT(WeakCount_.load(std::memory_order_relaxed) > 0);
}

Y_FORCE_INLINE bool TRefCounter::TryRef() const noexcept
{
    auto value = StrongCount_.load(std::memory_order_relaxed);
    YT_ASSERT(WeakCount_.load(std::memory_order_relaxed) > 0);

    while (value != 0 && !StrongCount_.compare_exchange_weak(value, value + 1));
    return value != 0;
}

Y_FORCE_INLINE bool TRefCounter::Unref() const
{
    auto oldStrongCount = StrongCount_.fetch_sub(1, std::memory_order_release);
    YT_ASSERT(oldStrongCount > 0);
    return oldStrongCount == 1;
}

Y_FORCE_INLINE int TRefCounter::GetWeakRefCount() const noexcept
{
    return WeakCount_.load(std::memory_order_relaxed);
}

Y_FORCE_INLINE void TRefCounter::WeakRef() const noexcept
{
    auto oldWeakCount = WeakCount_.fetch_add(1, std::memory_order_relaxed);
    YT_ASSERT(oldWeakCount > 0);
}

Y_FORCE_INLINE bool TRefCounter::WeakUnref() const
{
    auto oldWeakCount = WeakCount_.fetch_sub(1, std::memory_order_release);
    YT_ASSERT(oldWeakCount > 0);
    return oldWeakCount == 1;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, bool = std::is_base_of_v<TRefCountedBase, T>>
struct TRefCountedHelper
{
    static_assert(
        std::is_final_v<T>,
        "Ref-counted objects must be derived from TRefCountedBase or to be final");

    Y_FORCE_INLINE static const TRefCounter* GetRefCounter(const T* obj)
    {
        return reinterpret_cast<const TRefCounter*>(obj) - 1;
    }

    Y_FORCE_INLINE static void Destroy(T* obj)
    {
        auto* refCounter = GetRefCounter(obj);

        // No virtual call when T is final.
        obj->~T();

        void* ptr = const_cast<TRefCounter*>(refCounter);

        // Fast path. Weak refs cannot appear if there are neither strong nor weak refs.
        if (refCounter->GetWeakRefCount() == 1) {
            TMemoryReleaser<T>::Do(ptr, sizeof(TRefCounter));
            return;
        }

        if (refCounter->WeakUnref()) {
            TMemoryReleaser<T>::Do(ptr, sizeof(TRefCounter));
        }
    }

    Y_FORCE_INLINE static void Deallocate(T* obj)
    {
        auto* ptr = GetRefCounter(obj);
        TMemoryReleaser<T>::Do(ptr, sizeof(TRefCounter));
    }
};

template <class T>
struct TRefCountedHelper<T, true>
{
    Y_FORCE_INLINE static const TRefCounter* GetRefCounter(const T* obj)
    {
        return obj;
    }

    Y_FORCE_INLINE static void Destroy(const TRefCountedBase* obj)
    {
        const_cast<TRefCountedBase*>(obj)->DestroyRefCounted();
    }

    Y_FORCE_INLINE static void Deallocate(const TRefCountedBase* obj)
    {
        auto* ptr = reinterpret_cast<void**>(const_cast<TRefCountedBase*>(obj));
        auto [offset, ptrToDeleter] = UnpackPointer<void(void*, uint16_t)>(*ptr);

        // The most derived type is erased here. So we cannot call TMemoryReleaser with derived type.
        ptrToDeleter(reinterpret_cast<char*>(ptr) - offset, offset);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
Y_FORCE_INLINE const TRefCounter* GetRefCounter(T* obj)
{
    return TRefCountedHelper<T>::GetRefCounter(obj);
}

template <class T>
Y_FORCE_INLINE void DestroyRefCounted(T* obj)
{
    TRefCountedHelper<T>::Destroy(obj);
}

template <class T>
Y_FORCE_INLINE void DeallocateRefCounted(T* obj)
{
    TRefCountedHelper<T>::Deallocate(obj);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
Y_FORCE_INLINE void Ref(T* obj)
{
    GetRefCounter(obj)->Ref();
}

template <class T>
Y_FORCE_INLINE void Unref(T* obj)
{
    if (GetRefCounter(obj)->Unref()) {
        DestroyRefCounted(obj);
    }
}

template <class T>
Y_FORCE_INLINE void WeakRef(T* obj)
{
    GetRefCounter(obj)->WeakRef();
}

template <class T>
Y_FORCE_INLINE void WeakUnref(T* obj)
{
    if (GetRefCounter(obj)->WeakUnref()) {
        DeallocateRefCounted(obj);
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void TRefCounted::Unref() const
{
    ::NYT::Unref(this);
}

Y_FORCE_INLINE void TRefCounted::WeakUnref() const
{
    ::NYT::WeakUnref(this);
}

template <class T>
void TRefCounted::DestroyRefCountedImpl(T* ptr)
{
    // No standard way to statically calculate the base offset even if T is final.
    // static_cast<TFinalDerived*>(virtualBasePtr) does not work.

    auto* basePtr = static_cast<TRefCountedBase*>(ptr);
    auto offset = reinterpret_cast<uintptr_t>(basePtr) - reinterpret_cast<uintptr_t>(ptr);
    auto* refCounter = GetRefCounter(ptr);

    // No virtual call when T is final.
    ptr->~T();

    // Fast path. Weak refs cannot appear if there are neither strong nor weak refs.
    if (refCounter->GetWeakRefCount() == 1) {
        TMemoryReleaser<T>::Do(ptr, offset);
        return;
    }

    YT_ASSERT(offset < std::numeric_limits<uint16_t>::max());

    auto* vTablePtr = reinterpret_cast<char**>(basePtr);
    *vTablePtr = PackPointer(&TMemoryReleaser<T>::Do, offset);

    if (refCounter->WeakUnref()) {
        TMemoryReleaser<T>::Do(ptr, offset);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

