#pragma once

#include <yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

extern thread_local std::atomic<void*> HazardPointer;

using TDeleter = void (*)(void*);

template <class T, class TPtrLoader>
T* AcquireHazardPointer(const TPtrLoader& ptrLoader, T* localPtr);
void ReleaseHazardPointer();

void ScheduleObjectDeletion(void* ptr, TDeleter deleter);
bool ScanDeleteList();
void FlushDeleteList();

struct THazardPtrFlushGuard
{
    THazardPtrFlushGuard();
    ~THazardPtrFlushGuard();
};

////////////////////////////////////////////////////////////////////////////////

//! Protects an object from destruction (or deallocation) before CAS.
//! Destruction or deallocation depends on delete callback in ScheduleObjectDeletion.
template <class T>
class THazardPtr
{
public:
    using TRequireHazardEnabled = typename T::TEnableHazard;

    THazardPtr() = default;
    THazardPtr(const THazardPtr&) = delete;
    THazardPtr(THazardPtr&& other);

    THazardPtr& operator=(const THazardPtr&) = delete;
    THazardPtr& operator=(THazardPtr&& other);

    template <class TPtrLoader>
    static THazardPtr Acquire(const TPtrLoader& ptrLoader, T* localPtr);
    template <class TPtrLoader>
    static THazardPtr Acquire(const TPtrLoader& ptrLoader);

    void Reset();

    ~THazardPtr();

    T* Get() const;

    // Operators * and -> are allowed to use only when hazard ptr protects from object
    // destruction (ref count decrementation). Not memory deallocation.
    T& operator*() const;
    T* operator->() const;

    explicit operator bool() const;

private:
    explicit THazardPtr(std::nullptr_t);
    explicit THazardPtr(T* ptr);

    T* Ptr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HAZARD_PTR_INL_H_
#include "hazard_ptr-inl.h"
#undef HAZARD_PTR_INL_H_
