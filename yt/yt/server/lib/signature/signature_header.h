#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/yson/format.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/misc/guid.h>

#include <util/datetime/base.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TSignatureVersion
{
    int Major;
    int Minor;

    [[nodiscard]] constexpr bool operator==(const TSignatureVersion& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

template <TSignatureVersion version>
struct TSignatureHeaderImpl;

// TODO(pavook) Maybe TSignatureHeaderImpl<{0, 1}> when CTAD for NTTP arrives (C++20, clang 18).

template <>
struct TSignatureHeaderImpl<TSignatureVersion{0, 1}>
{
    std::string Issuer;
    TGuid KeypairId;
    TGuid SignatureId;
    TInstant IssuedAt;
    TInstant ValidAfter;
    TInstant ExpiresAt;

    constexpr static bool IsDeprecated = false;

    [[nodiscard]] bool operator==(const TSignatureHeaderImpl& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

using TSignatureHeader = std::variant<
    TSignatureHeaderImpl<TSignatureVersion{0, 1}>
>;

void Serialize(const TSignatureHeader& header, NYson::IYsonConsumer* consumer);

void Deserialize(TSignatureHeader& header, NYTree::INodePtr node);

void Deserialize(TSignatureHeader& header, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TYsonFormatTraits<NSignature::TSignatureHeader>
    : public TYsonTextFormatTraits
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
