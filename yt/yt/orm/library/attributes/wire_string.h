#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

// Non-owning continuous view of serialized protobuf message part.
class TWireStringPart
    : public std::span<const ui8>
{
public:
    TWireStringPart();
    TWireStringPart(const ui8* data, size_t size);

    TWireStringPart Skip(int count) const;

    std::string_view AsStringView() const;
    static TWireStringPart FromStringView(std::string_view view);
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
    static TWireString FromSerialized(const std::vector<TString>& serializedProtos);

    bool operator==(const TWireString& other) const;
};

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
