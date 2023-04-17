#pragma once

#include "public.h"

#include <library/cpp/yt/threading/public.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TFreeListItemBase
{
    std::atomic<T*> Next = nullptr;
};

// DCAS is supported in Clang with option -mcx16, is not supported in GCC. See following links.
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=84522
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=80878

using TAtomicUint128 = volatile unsigned __int128  __attribute__((aligned(16)));

template <class T1, class T2>
Y_FORCE_INLINE bool CompareAndSet(
    TAtomicUint128* atomic,
    T1& expected1,
    T2& expected2,
    T1 new1,
    T2 new2)
{
#if defined(__x86_64__)
    bool success;
    __asm__ __volatile__
    (
        "lock cmpxchg16b %1\n"
        "setz %0"
        : "=q"(success)
        , "+m"(*atomic)
        , "+a"(expected1)
        , "+d"(expected2)
        : "b"(new1)
        , "c"(new2)
        : "cc"
    );
    return success;
#elif defined(__arm64__) || (defined(__aarch64__) && defined(RTE_ARM_FEATURE_ATOMICS))
    register ui64 x0 __asm("x0") = (ui64)expected1;
    register ui64 x1 __asm("x1") = (ui64)expected2;
    register ui64 x2 __asm("x2") = (ui64)new1;
    register ui64 x3 __asm("x3") = (ui64)new2;
    ui64 old1 = (ui64)expected1;
    ui64 old2 = (ui64)expected2;
    asm volatile
    (
#if defined(RTE_CC_CLANG)
        ".arch armv8-a+lse\n"
#endif
        "caspal %[old0], %[old1], %[upd0], %[upd1], [%[dst]]"
        : [old0] "+r" (x0)
        , [old1] "+r" (x1)
        : [upd0] "r" (x2)
        , [upd1] "r" (x3)
        , [dst] "r" (atomic)
        : "memory"
    );
    expected1 = (T1)x0;
    expected2 = (T2)x1;
    return x0 == old1 && x1 == old2;
#elif defined(__aarch64__)
    ui64 exp1 = reinterpret_cast<ui64>(expected1);
    ui64 exp2 = reinterpret_cast<ui64>(expected2);
    ui32 fail = 0;

    do {
        ui64 current1 = 0;
        ui64 current2 = 0;
        asm volatile (
            "ldaxp %[cur1], %[cur2], [%[src]]"
            : [cur1] "=r" (current1)
            , [cur2] "=r" (current2)
            : [src] "r" (atomic)
            : "memory"
        );

        if (current1 != exp1 || current2 != exp2) {
            expected1 = reinterpret_cast<T1>(current1);
            expected2 = reinterpret_cast<T2>(current2);
            return false;
        }

        asm volatile (
            "stlxp %w[fail], %[new1], %[new2], [%[dst]]"
            : [fail] "=&r" (fail)
            : [new1] "r" (new1)
            , [new2] "r" (new2)
            , [dst] "r" (atomic)
            : "memory"
        );

    } while (Y_UNLIKELY(fail));
    return true;
#else
#    error Unsupported platform
#endif
}

template <class TItem>
class TFreeList
{
private:
    struct THead
    {
        std::atomic<TItem*> Pointer = {nullptr};
        std::atomic<size_t> PopCount = 0;

        THead() = default;

        explicit THead(TItem* pointer);

    };

    union
    {
        THead Head_;
        TAtomicUint128 AtomicHead_;
    };

    // Avoid false sharing.
    char Padding[NThreading::CacheLineSize - sizeof(TAtomicUint128)];

public:
    TFreeList();

    TFreeList(TFreeList&& other);

    ~TFreeList();

    template <class TPredicate>
    bool PutIf(TItem* head, TItem* tail, TPredicate predicate);

    void Put(TItem* head, TItem* tail);

    void Put(TItem* item);

    TItem* Extract();

    TItem* ExtractAll();

    bool IsEmpty() const;

    void Append(TFreeList& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define FREE_LIST_INL_H_
#include "free_list-inl.h"
#undef FREE_LIST_INL_H_
