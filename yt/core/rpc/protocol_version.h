#pragma once

#include "private.h"

#include <util/generic/string.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TProtocolVersion
{
    int Major;
    int Minor;

    static TProtocolVersion FromString(TStringBuf protocolVersionString);
};

bool operator == (const TProtocolVersion& lhs, const TProtocolVersion& rhs);
bool operator != (const TProtocolVersion& lhs, const TProtocolVersion& rhs);

void FormatValue(TStringBuilder* builder, TProtocolVersion version, TStringBuf spec);
TString ToString(TProtocolVersion protocolVersion);

////////////////////////////////////////////////////////////////////////////////

constexpr TProtocolVersion GenericProtocolVersion{-1, -1};
constexpr TProtocolVersion DefaultProtocolVersion{1, 0};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
