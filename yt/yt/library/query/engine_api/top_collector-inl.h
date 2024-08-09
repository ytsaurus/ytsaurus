#ifndef TOP_COLLECTOR_INL_H
#error "Direct inclusion of this file is not allowed, top_collector.h"
// For the sake of sane code completion.
#include "top_collector.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires std::is_trivially_copyable_v<T>
TVectorOverMemoryChunkProvider<T>::TVectorOverMemoryChunkProvider(
        TRefCountedTypeCookie cookie,
        IMemoryChunkProviderPtr memoryChunkProvider)
    : Provider_(memoryChunkProvider)
    , Cookie_(cookie)
    , DataHolder_(Provider_->Allocate(sizeof(T) * MinCapacity, Cookie_))
{ }

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::PushBack(T value)
{
    if (Size_ == Capacity()) {
        i64 newCapacity = Capacity() * 2;
        auto newDataHolder = Provider_->Allocate(sizeof(T) * newCapacity, Cookie_);
        ::memcpy(static_cast<void*>(newDataHolder->GetRef().Begin()), static_cast<void*>(Begin()), Capacity());
        DataHolder_.swap(newDataHolder);
    }

    (*this)[Size_++] = value;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
i64 TVectorOverMemoryChunkProvider<T>::Size() const
{
    return Size_;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
i64 TVectorOverMemoryChunkProvider<T>::Capacity() const
{
    return DataHolder_->GetRef().Size();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T& TVectorOverMemoryChunkProvider<T>::operator[](i64 index)
{
    YT_ASSERT(index < Size_);
    return std::bit_cast<T*>(DataHolder_->GetRef().data())[index];
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
const T& TVectorOverMemoryChunkProvider<T>::operator[](i64 index) const
{
    YT_ASSERT(index < Size_);
    return std::bit_cast<T*>(DataHolder_->GetRef().data())[index];
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::Begin()
{
    return &std::bit_cast<T*>(DataHolder_->GetRef().data())[0];
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::End()
{
    return &std::bit_cast<T*>(DataHolder_->GetRef().data())[Size_];
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
bool TVectorOverMemoryChunkProvider<T>::Empty() const
{
    return Size_ == 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
