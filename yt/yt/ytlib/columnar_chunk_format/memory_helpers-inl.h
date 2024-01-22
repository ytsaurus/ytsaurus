#ifndef YT_MEMORY_HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include memory_helpers.h"
// For the sake of sane code completion.
#include "memory_helpers.h"
#endif
#undef YT_MEMORY_HELPERS_INL_H

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* TMemoryHolder<T>::Resize(size_t total, TRefCountedTypeCookie cookie)
{
    return static_cast<T*>(TMemoryHolderBase::Resize(sizeof(T) * total, cookie));
}

template <class T>
T* TMemoryHolder<T>::GetData()
{
    return static_cast<T*>(Ptr_);
}

template <class T>
const T* TMemoryHolder<T>::GetData() const
{
    return static_cast<T*>(Ptr_);
}

template <class T>
T& TMemoryHolder<T>::operator[] (size_t index)
{
    return GetData()[index];
}

template <class T>
const T& TMemoryHolder<T>::operator[] (size_t index) const
{
    return GetData()[index];
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
size_t GetSizeHelper(size_t count)
{
    return AlignUp(sizeof(T) * count);
}

template <>
inline size_t GetSizeHelper<TBit>(size_t count)
{
    return sizeof(ui64) * ((count + 63) / 64);
}

template <class... Types, size_t... Index>
auto MakeResult(std::index_sequence<Index...>, char* data, size_t* offsets)
{
    return std::tuple<Types...>(Types(data + offsets[Index])...);
}

template <class T>
struct TTypeToPointer
{
    using TType = T*;
};

template <>
struct TTypeToPointer<TBit>
{
    using TType = TMutableBitmap;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

inline size_t GetBitmapSize(size_t count)
{
    return (NDetail::GetSizeHelper<TBit>(count) + sizeof(ui64) - 1) / sizeof(ui64);
}

template <class... Types>
auto AllocateCombined(TMemoryHolderBase* holder, TSize<Types>... count)
{
    static_assert((std::is_trivially_destructible_v<Types> && ...), "Types must be trivially destructible.");
    size_t offsets[] = {NDetail::GetSizeHelper<Types>(count)...};

    size_t total = 0;
    for (size_t index = 0; index < sizeof...(Types); ++index) {
        auto count = offsets[index];
        offsets[index] = total;
        total += count;
    }

    char* ptr = static_cast<char*>(holder->Resize(total));

    using TSequence = std::make_index_sequence<sizeof...(Types)>;
    return NDetail::MakeResult<typename NDetail::TTypeToPointer<Types>::TType...>(TSequence(), ptr, &offsets[0]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
