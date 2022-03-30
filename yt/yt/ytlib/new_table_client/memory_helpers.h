#pragma once

#include <yt/yt/core/misc/bitmap.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

// Simple memory holder for POD types.
// 1. Supports uninitialized allocations.
// 2. Compared to std::unique_ptr<T[]> TMemoryHolder supports .GetSize() and .Resize() methods.
// 3. Minimum size of container (sizeof(TMemoryHolder) == sizeof(void*)) allows to store
//    large number of holders in a compact way.
// sizeof(std::vector<T>) == 3 * sizeof(void*)
// sizeof(TBlob) == 6 * sizeof(void*)

// Type erased memory holder.
class TMemoryHolderBase
{
public:
    TMemoryHolderBase() = default;

    TMemoryHolderBase(const TMemoryHolderBase&) = delete;
    void operator=(const TMemoryHolderBase&) = delete;

    TMemoryHolderBase(TMemoryHolderBase&& other);

    void operator=(TMemoryHolderBase&& other);

    void Reset();

    // NB: Resize does not preserve current values.
    void* Resize(size_t total, TRefCountedTypeCookie cookie = NullRefCountedTypeCookie);

    ~TMemoryHolderBase();

protected:
    struct THeader
    {
        size_t Size;
        TRefCountedTypeCookie Cookie;
    };

    void* Ptr_ = nullptr;

    THeader* GetHeader() const;

    size_t GetSize() const;
};

template <class T>
class TMemoryHolder
    : public TMemoryHolderBase
{
public:
    T* Resize(size_t total, TRefCountedTypeCookie cookie = NullRefCountedTypeCookie);

    T* GetData();

    const T* GetData() const;

    T& operator[] (size_t index);

    const T& operator[] (size_t index) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TBit
{
    using TPtr = void*;
};

template <class>
using TSize = size_t;

inline size_t GetBitmapSize(size_t count);

template <class... Types>
auto AllocateCombined(TMemoryHolderBase* holder, TSize<Types>... count);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define YT_MEMORY_HELPERS_INL_H
#include "memory_helpers-inl.h"
#undef YT_MEMORY_HELPERS_INL_H
