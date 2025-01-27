#ifndef VECTOR_OVER_MEMORY_CHUNK_PROVIDER_INL_H
#error "Direct inclusion of this file is not allowed, vector_over_memory_chunk_provider.h"
// For the sake of sane code completion.
#include "vector_over_memory_chunk_provider.h"
#endif

#include <util/system/align.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires std::is_trivially_copyable_v<T>
TVectorOverMemoryChunkProvider<T>::TVectorOverMemoryChunkProvider(const TVectorOverMemoryChunkProvider<T>& other)
{
    Clone(this, other);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
TVectorOverMemoryChunkProvider<T>& TVectorOverMemoryChunkProvider<T>::operator=(const TVectorOverMemoryChunkProvider<T>& other)
{
    Clone(this, other);
    return *this;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::Clone(TVectorOverMemoryChunkProvider<T>* destination, const TVectorOverMemoryChunkProvider<T>& source)
{
    destination->Size_ = source.Size_;
    destination->Provider_ = source.Provider_;
    destination->Cookie_ = source.Cookie_;
    destination->DataHolder_ = destination->Provider_->Allocate(sizeof(T) * source.Capacity(), destination->Cookie_);
    ::memcpy(destination->Begin(), source.Begin(), destination->Size_ * sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

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
TVectorOverMemoryChunkProvider<T>::TVectorOverMemoryChunkProvider(
    TRefCountedTypeCookie cookie,
    IMemoryChunkProviderPtr memoryChunkProvider)
    : Provider_(std::move(memoryChunkProvider))
    , Cookie_(cookie)
    , DataHolder_(Provider_->Allocate(sizeof(T) * MinCapacity, Cookie_))
{ }

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::PushBack(T value)
{
    YT_VERIFY(Size_ <= Capacity());
    if (Size_ == Capacity()) {
        i64 newCapacity = Capacity() * 2;
        auto newDataHolder = Provider_->Allocate(sizeof(T) * newCapacity, Cookie_);
        ::memcpy(static_cast<void*>(newDataHolder->GetRef().Begin()), static_cast<void*>(Begin()), Size_ * sizeof(T));
        DataHolder_.swap(newDataHolder);
    }

    (*this)[Size_++] = value;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::push_back(T value)
{
    return PushBack(value);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
i64 TVectorOverMemoryChunkProvider<T>::Size() const
{
    return Size_;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
i64 TVectorOverMemoryChunkProvider<T>::size() const
{
    return Size();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
i64 TVectorOverMemoryChunkProvider<T>::Capacity() const
{
    return DataHolder_->GetRef().Size() / sizeof(T);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::Begin()
{
    return std::bit_cast<T*>(DataHolder_->GetRef().data());
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::begin()
{
    return Begin();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
const T* TVectorOverMemoryChunkProvider<T>::Begin() const
{
    return std::bit_cast<T*>(DataHolder_->GetRef().data());
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
const T* TVectorOverMemoryChunkProvider<T>::begin() const
{
    return Begin();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::End()
{
    return std::bit_cast<T*>(DataHolder_->GetRef().data()) + Size_;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T* TVectorOverMemoryChunkProvider<T>::end()
{
    return End();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
const T* TVectorOverMemoryChunkProvider<T>::End() const
{
    return std::bit_cast<T*>(DataHolder_->GetRef().data()) + Size_;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
const T* TVectorOverMemoryChunkProvider<T>::end() const
{
    return End();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T& TVectorOverMemoryChunkProvider<T>::Back()
{
    YT_ASSERT(Size_ > 0);
    return *(End() - 1);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
T& TVectorOverMemoryChunkProvider<T>::back()
{
    return Back();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
bool TVectorOverMemoryChunkProvider<T>::Empty() const
{
    return Size_ == 0;
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
bool TVectorOverMemoryChunkProvider<T>::empty() const
{
    return Empty();
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
TRange<T> TVectorOverMemoryChunkProvider<T>::Slice(i64 startOffset, i64 endOffset) const
{
    return TRange(Begin(), End()).Slice(startOffset, endOffset);
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::Append(const std::vector<T>& other)
{
    Append(other.data(), std::ssize(other));
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::Append(std::initializer_list<T> other)
{
    Append(other.begin(), std::ssize(other));
}

template <typename T>
    requires std::is_trivially_copyable_v<T>
void TVectorOverMemoryChunkProvider<T>::Append(const T* data, i64 size)
{
    i64 newSize = Size_ + size;

    if (newSize > Capacity()) {
        i64 newCapacity = std::max(newSize, Capacity() * 2);
        auto newDataHolder = Provider_->Allocate(sizeof(T) * newCapacity, Cookie_);
        ::memcpy(static_cast<void*>(newDataHolder->GetRef().Begin()), static_cast<void*>(Begin()), Size_ * sizeof(T));
        DataHolder_.swap(newDataHolder);
    }

    ::memcpy(static_cast<void*>(Begin() + Size_), std::bit_cast<void*>(data), size * sizeof(T));
    Size_ = newSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
