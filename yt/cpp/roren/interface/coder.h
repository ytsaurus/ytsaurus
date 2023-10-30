#pragma once

/// @file Contains declaration of @ref NRoren::TCoder class template used to code/decode intermediate streams of row.

#include <util/stream/mem.h>
#include <util/ysaveload.h>

#include <optional>

namespace google::protobuf {
class Message;
}

//  Forward declaration like <yt/yt/client/table_client/public.h>. Required for RorenEncode/RorenDecode(TUnversionedRow)
namespace NYT {
    template <class T>
    class TSharedRange;

    namespace NTableClient {
        class TUnversionedRow;
    }  // namespace NYT::NTableClient

    class TNode;
}  // namespace NYT

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

//  realization in bigrt/writers.cpp
void RorenEncode(IOutputStream* out, const NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& rows);
void RorenDecode(IInputStream* in, NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& result);

template <typename TRow>
concept CHasRorenEncodeFreeFunctions = requires (IInputStream* in, IOutputStream* out, TRow& ref, const TRow& constRef)
{
    RorenEncode(out, constRef);
    RorenDecode(in, ref);
};

/// @brief Coder is a class that takes a row and serialized it into roren INTERNAL representation.
///
/// Roren internal representation is unspecified format. It is not backward compatible in any way.
/// It is not guaranteed to be compatible in any way
///   - for different versions of row;
///   - for different versions of roren library.
template <typename T>
class TCoder
{
public:
    inline void Encode(IOutputStream* out, const T& value)
    {
        if constexpr (CHasRorenEncodeFreeFunctions<T>) {
            RorenEncode(out, value);
        } else {
            ::Save(out, value);
        }
    }

    inline void Decode(IInputStream* in, T& value)
    {
        if constexpr (CHasRorenEncodeFreeFunctions<T>) {
            RorenDecode(in, value);
        } else {
            ::Load(in, value);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires std::is_base_of_v<::google::protobuf::Message, T>
class TCoder<T>
{
public:
    inline void Encode(IOutputStream* out, const T& value)
    {
        TString bytes;
        bool ok = value.SerializeToString(&bytes);
        if (!ok) {
            ythrow yexception() << "cannot serialize proto message";
        }
        ::Save(out, bytes);
    }

    inline void Decode(IInputStream* in, T& value)
    {
        TString bytes;
        ::Load(in, bytes);
        bool ok = value.ParseFromString(bytes);
        if (!ok) {
            ythrow yexception() << "cannot load proto message";
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename... Args>
class TCoder<std::tuple<Args...>>
{
public:
    void Encode(IOutputStream* out, const std::tuple<Args...>& value)
    {
        ApplyForAll(
            [&] (auto&& coder, auto&& value) {
                coder.Encode(out, value);
            },
            value,
            std::index_sequence_for<Args...>{}
        );
    }

    void Decode(IInputStream* in, std::tuple<Args...>& value)
    {
        ApplyForAll(
            [&] (auto&& coder, auto&& value) {
                coder.Decode(in, value);
            },
            value,
            std::index_sequence_for<Args...>{}
        );
    }

private:
    template <size_t... Idx, typename F, typename A>
    inline void ApplyForAll(F&& func, A&& args, std::index_sequence<Idx...>)
    {
        [[maybe_unused]] int dummy[] = {
            (func(std::get<Idx>(Coders_), std::get<Idx>(args)),0)...
        };
    }

private:
    std::tuple<TCoder<Args>...> Coders_;
};

template <typename T>
class TCoder<std::optional<T>>
{
public:
    inline void Encode(IOutputStream* out, const std::optional<T>& value)
    {
        if (value) {
            ::Save(out, true);
            ValueCoder_.Encode(out, *value);
        } else {
            ::Save(out, false);
        }
    }

    inline void Decode(IInputStream* in, std::optional<T>& value)
    {
        bool hasValue;
        ::Load(in, hasValue);
        if (hasValue) {
            value.emplace();
            ValueCoder_.Decode(in, *value);
        } else {
            value.reset();
        }
    }
private:
    TCoder<T> ValueCoder_;
};

template <typename T>
class TCoder<std::vector<T>>
{
public:
    inline void Encode(IOutputStream* out, const std::vector<T>& value)
    {
        i64 size = value.size();
        ::Save(out, size);
        for (const auto& item : value) {
            ItemCoder_.Encode(out, item);
        }
    }

    inline void Decode(IInputStream* in, std::vector<T>& value)
    {
        i64 size;
        ::Load(in, size);
        value.clear();
        value.reserve(size);
        T item;
        for (i64 i = 0; i < size; ++i) {
            ItemCoder_.Decode(in, item);
            value.push_back(std::move(item));
        }
    }

private:
    TCoder<T> ItemCoder_;
};

template <typename T>
class TCoder<std::shared_ptr<T>>
{
public:
    inline void Encode(IOutputStream* out, const std::shared_ptr<T>& value)
    {
        if (value) {
            ::Save(out, true);
            ValueCoder_.Encode(out, *value);
        } else {
            ::Save(out, false);
        }
    }

    inline void Decode(IInputStream* in, std::shared_ptr<T>& value)
    {
        bool hasValue;
        ::Load(in, hasValue);
        if (hasValue) {
            value = std::make_shared<T>();
            ValueCoder_.Decode(in, *value);
        } else {
            value.reset();
        }
    }
private:
    TCoder<T> ValueCoder_;
};

template <typename K, typename V>
class TCoder<std::pair<K, V>>
{
public:
    void Encode(IOutputStream* out, const std::pair<K, V>& value)
    {
        KeyCoder_.Encode(out, value.first);
        ValueCoder_.Encode(out, value.second);
    }

    void Decode(IInputStream* in, std::pair<K, V>& value)
    {
        KeyCoder_.Decode(in, value.first);
        ValueCoder_.Decode(in, value.second);
    }

private:
    TCoder<K> KeyCoder_;
    TCoder<V> ValueCoder_;
};

template <typename... Args>
class TCoder<std::variant<Args...>>
{
public:
    void Encode(IOutputStream* out, const std::variant<Args...>& value)
    {
        auto index = value.index();
        if (index == std::variant_npos) {
            ythrow yexception() << "variant is invalid";
        }
        ::Save<i64>(out, index);
        ApplyForSingle(
            std::index_sequence_for<Args...>{},
            [&] (auto&& coder, auto&& typeIndexIntegralConstant) {
                constexpr size_t typeIndex = std::decay_t<decltype(typeIndexIntegralConstant)>::value;
                coder.Encode(out, std::get<typeIndex>(value));
            },
            value.index()
        );
    }

    void Decode(IInputStream* in, std::variant<Args...>& value)
    {
        i64 valueIndex;
        ::Load(in, valueIndex);

        ApplyForSingle(
            std::index_sequence_for<Args...>{},
            [&] (auto&& coder, auto&& typeIndexIntegralConstant) {
                constexpr size_t typeIndex = std::decay_t<decltype(typeIndexIntegralConstant)>::value;
                value.template emplace<typeIndex>();
                coder.Decode(in, std::get<typeIndex>(value));
            },
            valueIndex
        );
    }

private:
    template <size_t... Idx, typename F>
    inline void ApplyForSingle(std::index_sequence<Idx...>, F func, size_t idx)
    {
        // TODO: check generated code
        [[maybe_unused]] int dummy[] = {
            (idx == Idx ? (func(std::get<Idx>(Coders_), std::integral_constant<size_t, Idx>{}), 0) : 0)...
        };
    }

private:
    std::tuple<TCoder<Args>...> Coders_;
};

template <>
class TCoder<std::monostate>
{
public:
    void Encode(IOutputStream*, const std::monostate&)
    { }

    void Decode(IInputStream*, std::monostate&)
    { }
};

template <>
class TCoder<NYT::TNode>
{
public:
    void Encode(IOutputStream* out, const NYT::TNode& node);
    void Decode(IInputStream* in, NYT::TNode& node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
