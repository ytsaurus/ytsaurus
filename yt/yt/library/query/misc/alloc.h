#pragma once

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <type_traits>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAllocatorOverChunkProvider
{
public:
    // std::allocator_traits
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_copy_assignment = std::false_type;
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::false_type;
    using is_always_equal = std::false_type;

    explicit TAllocatorOverChunkProvider(
        IMemoryChunkProviderPtr memoryChunkProvider,
        TRefCountedTypeCookie tagCookie)
        : Provider_(std::move(memoryChunkProvider))
        , Cookie_(std::move(tagCookie))
    { }

    template <class U>
    struct rebind
    {
        using other = TAllocatorOverChunkProvider<U>;
    };

    TAllocatorOverChunkProvider(const TAllocatorOverChunkProvider& other) = default;

    TAllocatorOverChunkProvider<T>& operator=(const TAllocatorOverChunkProvider& other) = default;

    size_type max_size() const
    {
        return std::numeric_limits<size_type>::max() / sizeof(T);
    }

    // Allocation functions
    T* allocate(size_t n)
    {
        auto bytesRequired = n * sizeof(T);
        auto bytesAllocated = bytesRequired + sizeof(TAllocationHolder*);
        auto* allocationHolder = Provider_->Allocate(bytesAllocated, Cookie_).release();
        char* userDataBegin = allocationHolder->GetRef().data();
        char* userDataEnd = userDataBegin + bytesRequired;

        *reinterpret_cast<TAllocationHolder**>(userDataEnd) = allocationHolder;

        return reinterpret_cast<T*>(userDataBegin);
    }

    void deallocate(T* p, size_t n) noexcept
    {
        void* userDataEnd = p + n;
        auto* allocationHolder = *reinterpret_cast<TAllocationHolder**>(userDataEnd);
        delete allocationHolder;
    }

    template <class U>
    bool operator==(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept
    {
        return true;
    }

    template <class U>
    bool operator!=(const TAllocatorOverChunkProvider<U>& /*other*/) const noexcept
    {
        return false;
    }

private:
    IMemoryChunkProviderPtr Provider_;
    TRefCountedTypeCookie Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
