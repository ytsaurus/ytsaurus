#pragma once

#include <library/cpp/yt/memory/chunked_memory_pool_allocator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires std::is_trivially_copyable_v<T>
class TVectorOverMemoryChunkProvider
{
public:
    TVectorOverMemoryChunkProvider(
        TRefCountedTypeCookie cookie,
        IMemoryChunkProviderPtr memoryChunkProvider);

    TVectorOverMemoryChunkProvider(const TVectorOverMemoryChunkProvider& other);
    TVectorOverMemoryChunkProvider& operator=(const TVectorOverMemoryChunkProvider& other);
    ~TVectorOverMemoryChunkProvider() = default;

    T& operator[](i64 index);
    const T& operator[](i64 index) const;

    void PushBack(T value);
    void push_back(T value);

    i64 Size() const;
    i64 size() const;

    T* Begin();
    T* begin();
    const T* Begin() const;
    const T* begin() const;

    T* End();
    T* end();
    const T* End() const;
    const T* end() const;

    T& Back();
    T& back();

    bool Empty() const;
    bool empty() const;

    TRange<T> Slice(i64 startOffset, i64 endOffset) const;

    void Append(const std::vector<T>& other);
    void Append(std::initializer_list<T> other);

private:
    static inline constexpr i64 MinCapacity = 1024;

    i64 Size_ = 0;
    IMemoryChunkProviderPtr Provider_;
    TRefCountedTypeCookie Cookie_;
    std::unique_ptr<TAllocationHolder> DataHolder_;

    i64 Capacity() const;
    void Append(const T* data, i64 size);
    static void Clone(
        TVectorOverMemoryChunkProvider<T>* destination,
        const TVectorOverMemoryChunkProvider<T>& source);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define VECTOR_OVER_MEMORY_CHUNK_PROVIDER_INL_H
#include "vector_over_memory_chunk_provider-inl.h"
#undef VECTOR_OVER_MEMORY_CHUNK_PROVIDER_INL_H
