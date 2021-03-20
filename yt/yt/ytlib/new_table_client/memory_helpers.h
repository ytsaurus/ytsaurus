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

template <class T>
class TMemoryHolder
{
public:
    TMemoryHolder() = default;

    TMemoryHolder(const TMemoryHolder&) = delete;
    void operator=(const TMemoryHolder&) = delete;

    TMemoryHolder(TMemoryHolder&& other)
        : Ptr_(other.Ptr_)
    {
        other.Ptr_ = nullptr;
    }

    void operator=(TMemoryHolder&& other)
    {
        Reset();
        Ptr_ = other.Ptr_;
        other.Ptr_ = nullptr;
    }

    // NB: Resize does not preserve current values.
    T* Resize(size_t total, TRefCountedTypeCookie cookie = NullRefCountedTypeCookie)
    {
        if (total > GetSize()) {
            Reset();
            total *= 2;
            auto ptr = ::malloc(sizeof(THeader) + sizeof(T) * total);

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
            if (cookie != NullRefCountedTypeCookie) {
                TRefCountedTrackerFacade::AllocateTagInstance(cookie);
                TRefCountedTrackerFacade::AllocateSpace(cookie, sizeof(T) * total);
            }
#endif
            YT_VERIFY(reinterpret_cast<uintptr_t>(ptr) % 8 == 0);
            auto header = static_cast<THeader*>(ptr);
            *header = {total, cookie};
            Ptr_ = reinterpret_cast<T*>(header + 1);
        }

        return Ptr_;
    }

    size_t GetSize() const
    {
        return Ptr_ ? GetHeader()->Size : 0;
    }

    T* GetData()
    {
        return Ptr_;
    }

    const T* GetData() const
    {
        return Ptr_;
    }

    void Reset()
    {
        if (Ptr_) {
            auto* header = GetHeader();
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
            auto cookie = header->Cookie;
            if (cookie != NullRefCountedTypeCookie) {
                TRefCountedTrackerFacade::FreeTagInstance(cookie);
                TRefCountedTrackerFacade::FreeSpace(cookie, sizeof(T) * header->Size);
            }
#endif
            ::free(header);
            Ptr_ = nullptr;
        }
    }

    T& operator[] (size_t index)
    {
        return Ptr_[index];
    }

    const T& operator[] (size_t index) const
    {
        return Ptr_[index];
    }

    ~TMemoryHolder()
    {
        Reset();
    }

private:
    static_assert(std::is_trivially_destructible_v<T>, "Type must be trivially destructible.");

    struct THeader
    {
        size_t Size;
        TRefCountedTypeCookie Cookie;
    };

    THeader* GetHeader() const
    {
        return reinterpret_cast<THeader*>(Ptr_) - 1;
    }

    T* Ptr_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct TBit
{
    using TPtr = void*;
};

namespace NDetail {

template <class T>
size_t GetSizeHelper(size_t count)
{
    return sizeof(T) * count;
}

template <>
inline size_t GetSizeHelper<TBit>(size_t count)
{
    return sizeof(ui64) * ((count + 63) / 64);
}

template <class>
using TSize = size_t;

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

inline size_t GetBitmapSize(size_t count)
{
    return (NDetail::GetSizeHelper<TBit>(count) + sizeof(ui64) - 1) / sizeof(ui64);
}

template <class... Types>
auto AllocateCombined(TMemoryHolder<char>* holder, NDetail::TSize<Types>... count)
{
    static_assert((std::is_trivially_destructible_v<Types> && ...), "Types must be trivially destructible.");
    size_t offsets[] = {NDetail::GetSizeHelper<Types>(count)...};

    size_t total = 0;
    for (size_t index = 0; index < sizeof...(Types); ++index) {
        auto count = offsets[index];
        offsets[index] = total;
        total += count;
    }

    char* ptr = holder->Resize(total);

    using TSequence = std::make_index_sequence<sizeof...(Types)>;
    return NDetail::MakeResult<typename NDetail::TTypeToPointer<Types>::TType...>(TSequence(), ptr, &offsets[0]);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
