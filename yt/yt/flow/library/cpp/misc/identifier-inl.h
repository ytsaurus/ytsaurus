#pragma once

#ifndef IDENTIFIER_INL_H_
    #error "Direct inclusion of this file is not allowed, include identifier.h"
    // For the sake of sane code completion.
    #include "identifier.h"
#endif

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/string/format_arg.h>
#include <library/cpp/yt/yson/public.h>

#include <cstdint>
#include <cstring>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline char* TIdentifierStringData::ExtraPtr() noexcept
{
    return reinterpret_cast<char*>(this + 1);
}

inline const char* TIdentifierStringData::ExtraPtr() const noexcept
{
    return reinterpret_cast<const char*>(this + 1);
}

inline std::string_view TIdentifierStringData::Data() const noexcept
{
    const char* buf = ExtraPtr();
    NDetail::TShortSize shortLen;
    std::memcpy(&shortLen, buf, sizeof(shortLen));
    if (shortLen != 0) [[likely]] {
        return std::string_view(buf + NDetail::ShortHeaderSize, shortLen);
    }
    NDetail::TLongSize longLen;
    std::memcpy(&longLen, buf + NDetail::ShortHeaderSize, sizeof(longLen));
    return std::string_view(buf + NDetail::LongHeaderSize, static_cast<size_t>(longLen));
}

inline size_t TIdentifierStringData::ByteSize() const noexcept
{
    const char* buf = ExtraPtr();
    NDetail::TShortSize shortLen;
    std::memcpy(&shortLen, buf, sizeof(shortLen));
    if (shortLen != 0) [[likely]] {
        return sizeof(TIdentifierStringData) + NDetail::ShortHeaderSize + shortLen;
    }
    NDetail::TLongSize longLen;
    std::memcpy(&longLen, buf + NDetail::ShortHeaderSize, sizeof(longLen));
    return sizeof(TIdentifierStringData) + NDetail::LongHeaderSize + static_cast<size_t>(longLen);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

namespace NYT::NFlow::NDetail {

////////////////////////////////////////////////////////////////////////////////

void IdentifierSave(IOutputStream* out, std::string_view data);
void IdentifierLoad(IInputStream* in, std::string& data);

void IdentifierSerialize(std::string_view data, NYson::IYsonConsumer* consumer);
void IdentifierDeserializeFromNode(std::string& data, const NYTree::INodePtr& node);
void IdentifierDeserializeFromCursor(std::string& data, NYson::TYsonPullParserCursor* cursor);

void IdentifierFormat(TStringBuilderBase* builder, std::string_view data, TStringBuf spec);

void IdentifierToUnversionedValue(
    NTableClient::TUnversionedValue* unversionedValue,
    std::string_view data,
    const NTableClient::TRowBufferPtr& rowBuffer,
    int id,
    NTableClient::EValueFlags flags);
void IdentifierFromUnversionedValue(std::string& data, const NTableClient::TUnversionedValue& unversionedValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDetail

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
TStrongIdentifierTypedef<TTag>::TStrongIdentifierTypedef()
    : Value_(nullptr)
{ }

template <class TTag>
TStrongIdentifierTypedef<TTag>::TStrongIdentifierTypedef(TStrongIdentifierTypedef&& str) noexcept
    : Value_(std::move(str.Value_))
{ }

template <class TTag>
TStrongIdentifierTypedef<TTag>& TStrongIdentifierTypedef<TTag>::operator=(TStrongIdentifierTypedef&& str) noexcept
{
    Value_ = std::move(str.Value_);
    return *this;
}

template <class TTag>
template <size_t N>
TStrongIdentifierTypedef<TTag>::TStrongIdentifierTypedef(const char (&literal)[N])
    : Value_(N <= 1 ? nullptr : TIdentifierStringData::Make(std::string_view(literal, N - 1)))
{ }

template <class TTag>
TStrongIdentifierTypedef<TTag>::TStrongIdentifierTypedef(std::string_view str)
    : Value_(str.empty() ? nullptr : TIdentifierStringData::Make(str))
{ }

template <class TTag>
constexpr TStrongIdentifierTypedef<TTag>::operator std::string_view() const noexcept
{
    return Value_ ? Value_->Data() : std::string_view();
}

template <class TTag>
constexpr std::string_view TStrongIdentifierTypedef<TTag>::Underlying() const& noexcept
{
    return Value_ ? Value_->Data() : std::string_view();
}

template <class TTag>
size_t TStrongIdentifierTypedef<TTag>::Capacity() const noexcept
{
    return Value_ ? Value_->ByteSize() : 0;
}

template <class TTag>
template <typename TArgument>
bool TStrongIdentifierTypedef<TTag>::operator==(TArgument&& argument) const
    requires requires(const std::string& v, TArgument&& argument) { v == argument; }
{
    return Underlying() == argument;
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator<(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() < rhs.Underlying();
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator>(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() > rhs.Underlying();
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator<=(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() <= rhs.Underlying();
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator>=(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() >= rhs.Underlying();
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator==(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() == rhs.Underlying();
}

template <class TTag>
bool TStrongIdentifierTypedef<TTag>::operator!=(const TStrongIdentifierTypedef& rhs) const
{
    return Underlying() != rhs.Underlying();
}

template <class TTag>
void TStrongIdentifierTypedef<TTag>::Save(IOutputStream* out) const
{
    NDetail::IdentifierSave(out, Underlying());
}

template <class TTag>
void TStrongIdentifierTypedef<TTag>::Load(IInputStream* in)
{
    std::string tmp;
    NDetail::IdentifierLoad(in, tmp);
    Value_ = tmp.empty() ? nullptr : TIdentifierStringData::Make(tmp);
}

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
void Serialize(const TStrongIdentifierTypedef<TTag>& value, NYson::IYsonConsumer* consumer)
{
    NDetail::IdentifierSerialize(value.Underlying(), consumer);
}

template <class TTag>
void Deserialize(TStrongIdentifierTypedef<TTag>& value, const NYTree::INodePtr& node)
{
    std::string tmp;
    NDetail::IdentifierDeserializeFromNode(tmp, node);
    value = TStrongIdentifierTypedef<TTag>(std::string_view(tmp));
}

template <class TTag>
void Deserialize(TStrongIdentifierTypedef<TTag>& value, NYson::TYsonPullParserCursor* cursor)
{
    std::string tmp;
    NDetail::IdentifierDeserializeFromCursor(tmp, cursor);
    value = TStrongIdentifierTypedef<TTag>(std::string_view(tmp));
}

template <class TTag>
void ToProto(TProtobufString* serialized, const TStrongIdentifierTypedef<TTag>& original)
{
    *serialized = original.Underlying();
}

template <class TTag>
void FromProto(TStrongIdentifierTypedef<TTag>* original, const TProtobufString& serialized)
{
    *original = TStrongIdentifierTypedef<TTag>(std::string_view(serialized));
}

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NYTree {

// Forward declaration for specialization below.
template <class T>
struct TAssociativeContainerKeyHelper;

template <class TTag>
struct TAssociativeContainerKeyHelper<NYT::NFlow::TStrongIdentifierTypedef<TTag>>
{
    static std::string_view Serialize(const NYT::NFlow::TStrongIdentifierTypedef<TTag>& value)
    {
        return value.Underlying();
    }

    static NYT::NFlow::TStrongIdentifierTypedef<TTag> Deserialize(std::string_view key)
    {
        return NYT::NFlow::TStrongIdentifierTypedef<TTag>(key);
    }
};

} // namespace NYT::NYTree

////////////////////////////////////////////////////////////////////////////////

namespace std {

template <class TTag>
struct hash<NYT::NFlow::TStrongIdentifierTypedef<TTag>>
{
    size_t operator()(const NYT::NFlow::TStrongIdentifierTypedef<TTag>& value) const
    {
        return std::hash<std::string_view>{}(value.Underlying());
    }
};

} // namespace std

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTableClient {

template <class TTag>
void ToUnversionedValue(
    TUnversionedValue* unversionedValue,
    const NFlow::TStrongIdentifierTypedef<TTag>& value,
    const TRowBufferPtr& rowBuffer,
    int id = 0,
    EValueFlags flags = EValueFlags::None)
{
    NFlow::NDetail::IdentifierToUnversionedValue(unversionedValue, value.Underlying(), rowBuffer, id, flags);
}

template <class TTag>
void FromUnversionedValue(NFlow::TStrongIdentifierTypedef<TTag>* value, const TUnversionedValue& unversionedValue)
{
    std::string tmp;
    NFlow::NDetail::IdentifierFromUnversionedValue(tmp, unversionedValue);
    *value = NFlow::TStrongIdentifierTypedef<TTag>(std::string_view(tmp));
}

} // namespace NYT::NTableClient

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

template <class TTag>
void FormatValue(TStringBuilderBase* builder, const NFlow::TStrongIdentifierTypedef<TTag>& value, TStringBuf spec)
{
    NFlow::NDetail::IdentifierFormat(builder, value.Underlying(), spec);
}

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
IOutputStream& operator<<(IOutputStream& out, const NYT::NFlow::TStrongIdentifierTypedef<TTag>& value)
{
    return out << value.Underlying();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
struct THash<NYT::NFlow::TStrongIdentifierTypedef<TTag>>
{
    size_t operator()(const NYT::NFlow::TStrongIdentifierTypedef<TTag>& value) const
    {
        return THash<std::string_view>{}(value.Underlying());
    }
};
