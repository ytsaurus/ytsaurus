#pragma once

#include <yt/core/misc/public.h>
#include <yt/core/misc/intrusive_linked_list.h>
#include <yt/core/misc/ring_queue.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger LockFreePtrLogger;

extern thread_local std::atomic<void*> HazardPointer;

using TDeleter = void (*)(void*);

template <class TPtrLoader>
void* AcquireHazardPointer(const TPtrLoader& ptrLoader, void* localPtr);
inline void ReleaseHazardPointer();

void ScheduleObjectDeletion(void* ptr, TDeleter deleter);
bool ScanDeleteList();

////////////////////////////////////////////////////////////////////////////////

// Useful to protect object from destruction (or deallocation) before CAS.
// Destruction or deallocation depends on delete callback in ScheduleObjectDeletion
template <class T>
class THazardPtr
{
public:
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
