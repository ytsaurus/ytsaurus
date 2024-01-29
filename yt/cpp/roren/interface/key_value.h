#pragma once

#include "fwd.h"

#include <util/stream/str.h>
#include <util/system/byteorder.h>
#include <util/ysaveload.h>

#include <type_traits>
#include <utility>
#include <ostream>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

//
// NOTE ABOUT IMPLEMENTATION
//
// Sometimes Roren uses TKV objects without prior compile time knowledge of exact K,V types.
// To do so roren uses offsets to Key/Value subobjects. To get such offsets `offsetof` macro is useful,
// but it works only if type has standard-layout
//
//   https://en.cppreference.com/w/cpp/types/offsetof
//
// But some useful types (e.g. protobuf structures) are not of standard layout.
// To make our code work with such types we wrap them into `TPodifiedValue` class that makes them appear like POD type.
template <typename K, typename V>
class TKV
{
private:
    using TSelf = TKV<K, V>;

public:
    using TKey = K;
    using TValue = V;

public:
    static constexpr ssize_t KeyOffset = offsetof(TSelf, Key_);
    static constexpr ssize_t ValueOffset = offsetof(TSelf, Value_);

public:
    TKV() = default;

    TKV(const TKey& key, const TValue& value)
        : Key_(key)
        , Value_(value)
    { }

    template<typename KK, typename VV>
    TKV(KK&& key, VV&& value)
        : Key_(std::forward<KK>(key))
        , Value_(std::forward<VV>(value))
    { }

    TKey& Key() noexcept
    {
        return *Key_.Get();
    }

    const TKey& Key() const noexcept
    {
        return *Key_.Get();
    }

    TValue& Value() noexcept
    {
        return *Value_.Get();
    }

    const TValue& Value() const noexcept
    {
        return *Value_.Get();
    }

private:
    template <typename T>
    class alignas(T) TPodifiedValue
    {
    public:
        TPodifiedValue() noexcept(std::is_nothrow_default_constructible_v<T>)
        {
            new (ByteValue_) T;
        }

        TPodifiedValue(const TPodifiedValue<T>& other) noexcept(std::is_nothrow_copy_constructible_v<T>)
        {
            new (ByteValue_) T(*other.Get());
        }

        TPodifiedValue(TPodifiedValue<T>&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
        {
            new (ByteValue_) T(std::move(*other.Get()));
        }

        TPodifiedValue(T&& t) noexcept(std::is_nothrow_move_constructible_v<T>)
        {
            new (ByteValue_) T(std::move(t));
        }

        TPodifiedValue(const T& t) noexcept(std::is_nothrow_copy_constructible_v<T>)
        {
            new (ByteValue_) T(t);
        }

        ~TPodifiedValue() noexcept(std::is_nothrow_destructible_v<T>)
        {
            Get()->~T();
        }

        TPodifiedValue& operator=(const TPodifiedValue& other)
        {
            *Get() = *other.Get();
            return *this;
        }

        TPodifiedValue& operator=(TPodifiedValue&& other)
        {
            *Get() = std::move(*other.Get());
            return *this;
        }

        T* Get() noexcept
        {
            return reinterpret_cast<T*>(ByteValue_);
        }

        const T* Get() const noexcept
        {
            return reinterpret_cast<const T*>(ByteValue_);
        }

    private:
        std::byte ByteValue_[sizeof(T)];
    };

private:
    TPodifiedValue<TKey> Key_;
    TPodifiedValue<TValue> Value_;
};

template <typename K, typename V>
bool operator == (const TKV<K, V>& lhs, const TKV<K, V>& rhs)
{
    return lhs.Key() == rhs.Key() && lhs.Value() == rhs.Value();
}

template <typename K, typename V>
static inline IOutputStream& operator<<(IOutputStream& o, const TKV<K, V>& v)
{
    return o << "TKV{ " << v.Key() << ", " << v.Value() << '}';
}

template <int Idx, typename K, typename V>
const auto& get(const TKV<K, V>& kv) noexcept
{
    if constexpr (Idx == 0) {
        return kv.Key();
    } else if constexpr (Idx == 1) {
        return kv.Value();
    } else {
        static_assert(Idx == 0, "Valid values for Idx are: [0, 1]");
    }
}

template <int Idx, typename K, typename V>
auto& get(TKV<K, V>& kv) noexcept
{
    if constexpr (Idx == 0) {
        return kv.Key();
    } else if constexpr (Idx == 1) {
        return kv.Value();
    } else {
        static_assert(Idx == 0, "Valid values for Idx are: [0, 1]");
    }
}

template <typename K, typename V>
void PrintTo(const TKV<K, V>& kv, std::ostream* out)
{
    using namespace testing;
    (*out) << "TKV{" << PrintToString(kv.Key()) << ", " << PrintToString(kv.Value()) << "}";
}

////////////////////////////////////////////////////////////////////////////////

namespace NTraits {

////////////////////////////////////////////////////////////////////////////////

template<typename T>
struct TIsTKV
    : public std::false_type
{ };

template<typename K, typename V>
struct TIsTKV<TKV<K, V>>
    : public std::true_type
{ };

template <typename T>
inline constexpr bool IsTKV = TIsTKV<T>::value;

template <typename T>
concept CTKV = IsTKV<T>;

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename Enable = void>
struct TKeyOf
{
    using TType = void;
};

template <typename T>
struct TKeyOf<T, typename std::enable_if_t<IsTKV<T>>>
{
    using TType = typename T::TKey;
};

template <typename T>
using TKeyOfT = typename TKeyOf<T>::TType;

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename Enable = void>
struct TValueOf
{
    using TType = void;
};

template <typename T>
struct TValueOf<T, typename std::enable_if_t<IsTKV<T>>>
{
    using TType = typename T::TValue;
};

template <typename T>
using TValueOfT = typename TValueOf<T>::TType;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTraits

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename V>
class TCoder<TKV<K, V>>
{
public:
    inline void Encode(IOutputStream* out, const TKV<K, V>& kv)
    {
        KeyCoder_.Encode(out, kv.Key());
        ValueCoder_.Encode(out, kv.Value());
    }

    inline void Decode(IInputStream* in, TKV<K, V>& kv)
    {
        KeyCoder_.Decode(in, kv.Key());
        ValueCoder_.Decode(in, kv.Value());
    }

private:
    TCoder<K> KeyCoder_;
    TCoder<V> ValueCoder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

template <typename T>
struct TDumper;

template <typename K, typename V>
struct TDumper<NRoren::TKV<K, V>>
{
    template <class S>
    static inline void Dump(S& s, const NRoren::TKV<K, V>& v)
    {
        s << std::pair{v.Key(), v.Value()};
    }
};

template <typename K, typename V>
class TSerializer<NRoren::TKV<K, V>>
{
public:
    static void Save(IOutputStream* output, const NRoren::TKV<K, V>& kv)
    {
        ::Save(output, kv.Key());
        ::Save(output, kv.Value());
    }

    static void Load(IInputStream* input, NRoren::TKV<K, V>& kv)
    {
        ::Load(input, kv.Key());
        ::Load(input, kv.Value());
    }
};


namespace std
{
    template <typename K, typename V>
    struct tuple_size<NRoren::TKV<K, V>> : std::integral_constant<size_t, 2>
    { };

    template <typename K, typename V>
    struct tuple_element<0, NRoren::TKV<K, V>>
    {
        using type = K;
    };

    template <typename K, typename V>
    struct tuple_element<1, NRoren::TKV<K, V>>
    {
        using type = V;
    };
}
