#ifndef CHUNKED_MEMORY_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include chunked_memory_pool.h"
#endif
#undef CHUNKED_MEMORY_POOL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline char* TChunkedMemoryPool::AllocateUnaligned(size_t size)
{
    // Fast path.
    if (CurrentPtr_ + size <= EndPtr_) {
        char* result = CurrentPtr_;
        CurrentPtr_ += size;
        Size_ += size;
        return result;
    }

    // Slow path.
    return AllocateUnalignedSlow(size);
}

inline char* TChunkedMemoryPool::AllocateAligned(size_t size, size_t align)
{
    CurrentPtr_ = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(CurrentPtr_) + align - 1) & ~(align - 1));
    return AllocateUnaligned(size);
}

template <class T>
inline T* TChunkedMemoryPool::AllocateUninitialized(size_t n, size_t align)
{
    return reinterpret_cast<T*>(AllocateAligned(sizeof(T) * n, align));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
