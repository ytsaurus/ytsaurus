#pragma once

#include "public.h"

#include <yt/yt/core/yson/protobuf_interop_options.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

// Non-owning continuous view of serialized protobuf message part.
class TWireStringPart
{
public:
    TWireStringPart();
    TWireStringPart(const ui8* data, size_t size);

    std::span<const ui8> AsSpan() const;
    std::string_view AsStringView() const;
    static TWireStringPart FromStringView(std::string_view view);

private:
    std::span<const ui8> Span_;
};

////////////////////////////////////////////////////////////////////////////////

// Non-owning view of concatenated serialized protobuf message parts.
//
// NB! The `N` template parameter of TCompactVector
// is intentionally equal to 1 for two reasons:
// 1) In most cases TWireString represents single protobuf message.
// 2) The size of TWireStringPart is exactly equal to the size of vector internals.
//
// Parsers of TWireString should follow the "last one wins" rule
// described in protobuf encoding. However, additional constraint is introduced
// and could be utilized for effective parsing and navigation: for repeated fields
// we expect size() to be equal to the size of the repeated field
// and each element should correspond to different TWireStringPart.
class TWireString
    : public TCompactVector<TWireStringPart, 1>
{
public:
    using TCompactVector::TCompactVector;

    static const TWireString Empty;

    static TWireString FromSerialized(std::string_view serializedProto);
    static TWireString FromSerialized(const std::vector<std::string_view>& serializedProtos);
    static TWireString FromSerialized(const std::vector<std::string>& serializedProtos);
    static TWireString FromSerialized(const std::vector<TString>& serializedProtos);

    bool operator==(const TWireString& other) const;

    TWireStringPart LastOrEmptyPart() const;
};

////////////////////////////////////////////////////////////////////////////////

// Returns whether parsing was successful.
// NB! Caller should guarantee that provided element type refers to appropriate
// cpp type (e.g. FieldDescriptor::TYPE_UINT32 is valid argument for ParseUint64, but not others).
bool ParseUint64(ui64* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart);
bool ParseInt64(i64* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart);
bool ParseDouble(double* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart);
bool ParseBoolean(bool* value, NYson::TProtobufElementType type, TWireStringPart wireStringPart);

std::string SerializeUint64(ui64 value, NYson::TProtobufElementType type);
std::string SerializeInt64(i64 value, NYson::TProtobufElementType type);
std::string SerializeDouble(double value, NYson::TProtobufElementType type);
std::string SerializeBoolean(bool value, NYson::TProtobufElementType type);

std::string SerializeAttributeDictionary(const NYTree::IAttributeDictionary& attributeDictionary);
std::string SerializeMessage(
    const NYTree::INodePtr& message,
    const NYson::TProtobufMessageType* messageType,
    NYson::TProtobufWriterOptions options = {});

////////////////////////////////////////////////////////////////////////////////

void MergeMessageFrom(NProtoBuf::MessageLite* message, const TWireString& wireString);

////////////////////////////////////////////////////////////////////////////////

// Walks deeply in wire string, ignoring missing fields.
// Returns requested subpart or empty wire string.
// Closest analogue of `GetNodeByPathOrEntity` for protobuf.
TWireString GetWireStringByPath(
    const NProtoBuf::Descriptor* descriptor,
    const TWireString& wireString,
    NYPath::TYPathBuf path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
