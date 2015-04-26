#ifndef CHUNKED_MEMORY_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include chunked_memory_pool.h"
#endif
#undef CHUNKED_MEMORY_POOL_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline char* TChunkedMemoryPool::AlignPtr(char* ptr, int align)
{
    return reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(ptr) + align - 1) & ~(align - 1));
}

inline char* TChunkedMemoryPool::AllocateUnaligned(i64 size)
{
    // Fast path.
    if (FreeZoneEnd_ >= FreeZoneBegin_ + size) {
        FreeZoneEnd_ -= size;
        Size_ += size;
        return FreeZoneEnd_;
    }

    // Slow path.
    return AllocateUnalignedSlow(size);
}

inline char* TChunkedMemoryPool::AllocateAligned(i64 size, int align)
{
    // NB: This can lead to FreeZoneBegin_ >= FreeZoneEnd_ in which case the chunk is full.
    FreeZoneBegin_ = AlignPtr(FreeZoneBegin_, align);

    // Fast path.
    if (FreeZoneBegin_ + size <= FreeZoneEnd_) {
        char* result = FreeZoneBegin_;
        Size_ += size;
        FreeZoneBegin_ += size;
        return result;
    }

    // Slow path.
    return AllocateAlignedSlow(size, align);
}

template <class T>
inline T* TChunkedMemoryPool::AllocateUninitialized(int n, int align)
{
    return reinterpret_cast<T*>(AllocateAligned(sizeof(T) * n, align));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
