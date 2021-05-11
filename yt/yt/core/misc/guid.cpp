#include "guid.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/string.h>

#include <yt/yt/core/ytree/serialize.h>

#include <util/random/random.h>

namespace NYT {

using NYTree::Serialize;

////////////////////////////////////////////////////////////////////////////////

TGuid TGuid::Create()
{
    return TGuid(RandomNumber<ui64>(), RandomNumber<ui64>());
}

TGuid TGuid::FromString(TStringBuf str)
{
    TGuid guid;
    if (!FromString(str, &guid)) {
        THROW_ERROR_EXCEPTION("Error parsing GUID %Qv",
            str);
    }
    return guid;
}

bool TGuid::FromString(TStringBuf str, TGuid* result)
{
    size_t partId = 3;
    ui64 partValue = 0;
    bool isEmptyPart = true;

    for (size_t i = 0; i != str.size(); ++i) {
        const char c = str[i];

        if (c == '-') {
            if (isEmptyPart || partId == 0) { // x-y--z, -x-y-z or x-y-z-m-...
                return false;
            }
            result->Parts32[partId] = static_cast<ui32>(partValue);
            --partId;
            partValue = 0;
            isEmptyPart = true;
            continue;
        }

        ui32 digit = 0;
        if ('0' <= c && c <= '9') {
            digit = c - '0';
        } else if ('a' <= c && c <= 'f') {
            digit = c - 'a' + 10;
        } else if ('A' <= c && c <= 'F') {
            digit = c - 'A' + 10;
        } else {
            return false; // non-hex character
        }

        partValue = partValue * 16 + digit;
        isEmptyPart = false;

        // overflow check
        if (partValue > Max<ui32>()) {
            return false;
        }
    }

    if (partId != 0 || isEmptyPart) { // x-y or x-y-z-
        return false;
    }
    result->Parts32[partId] = static_cast<ui32>(partValue);
    return true;
}

TGuid TGuid::FromStringHex32(TStringBuf str)
{
    TGuid guid;
    if (!FromStringHex32(str, &guid)) {
        THROW_ERROR_EXCEPTION("Error parsing Hex32 GUID %Qv",
            str);
    }
    return guid;
}

bool TGuid::FromStringHex32(TStringBuf str, TGuid* result)
{
    if (str.size() != 32) {
        return false;
    }

    bool ok = true;
    int i = 0;
    auto parseChar = [&] {
        const char c = str[i++];
        ui32 digit = 0;
        if ('0' <= c && c <= '9') {
            digit = c - '0';
        } else if ('a' <= c && c <= 'f') {
            digit = c - 'a' + 10;
        } else if ('A' <= c && c <= 'F') {
            digit = c - 'A' + 10;
        } else {
            ok = false;
        }
        return digit;
    };

    for (size_t j = 0; j < 16; ++j) {
        result->ReversedParts8[15 - j] = parseChar() * 16 + parseChar();
    }

    return ok;
}

void FormatValue(TStringBuilderBase* builder, TGuid value, TStringBuf /*format*/)
{
    char* begin = builder->Preallocate(8 * 4 + 3);
    char* end = WriteGuidToBuffer(begin, value);
    builder->Advance(end - begin);
}

TString ToString(TGuid guid)
{
    return ToStringViaBuilder(guid);
}

REGISTER_INTERMEDIATE_PROTO_INTEROP_REPRESENTATION(NProto::TGuid, TGuid)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
