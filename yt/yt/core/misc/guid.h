#pragma once

#include "public.h"

#include <yt/yt_proto/yt/core/misc/proto/guid.pb.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <util/generic/typetraits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! TGuid is 16-byte value that might be interpreted as 4 little-endian 32-bit integers or 2 64-bit little-endian integers.
/*!
 *    *-------------------------*-------------------------*
 *    |       Parts64[0]        |       Parts64[1]        |
 *    *------------*------------*------------*------------*
 *    | Parts32[0] | Parts32[1] | Parts32[2] | Parts32[3] |
 *    *------------*------------*------------*------------*
 *    | 0..............................................15 |
 *    *---------------------------------------------------*
 *
 *  Canonical text representation of guid consists of four base-16 numbers.
 *  In text form, Parts32[3] comes first, and Parts32[0] comes last.
 *
 *  For example:
 *
 *    xxyyzzaa-0-1234-ff
 *
 *      xx is byte [15]
 *      yy is byte [14]
 *      zz is byte [13]
 *      12 is byte [9]
 *      34 is byte [8]
 *      ff is byte [0]
 */
struct TGuid
{
    union
    {
        ui32 Parts32[4];
        ui64 Parts64[2];
        ui8 Parts8[16];
    };

    //! Constructs a null (zero) guid.
    constexpr TGuid();

    //! Constructs guid from parts.
    constexpr TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3);

    //! Constructs guid from parts.
    constexpr TGuid(ui64 part0, ui64 part1);

    //! Copies an existing guid.
    TGuid(const TGuid& other) = default;

    //! Checks if TGuid is zero.
    bool IsEmpty() const;

    //! Converts TGuid to bool, returns |false| iff TGuid is zero.
    explicit operator bool() const;

    //! Creates a new instance.
    static TGuid Create();

    //! Parses guid from TStringBuf, throws an exception if something went wrong.
    static TGuid FromString(TStringBuf str);

    //! Parses guid from TStringBuf, returns |true| if everything was ok.
    static bool FromString(TStringBuf str, TGuid* guid);

    //! Same as FromString, but expects exactly 32 hex digits without dashes.
    static TGuid FromStringHex32(TStringBuf str);

    //! Same as TryFromString, but expects exactly 32 hex digits without dashes.
    static bool FromStringHex32(TStringBuf str, TGuid* guid);
};

void ToProto(NProto::TGuid* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const NProto::TGuid& protoGuid);

void ToProto(TProtoStringType* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const TProtoStringType& protoGuid);

void FormatValue(TStringBuilderBase* builder, TGuid value, TStringBuf format);
TString ToString(TGuid guid);

bool operator == (TGuid lhs, TGuid rhs);
bool operator != (TGuid lhs, TGuid rhs);
bool operator <  (TGuid lhs, TGuid rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

Y_DECLARE_PODTYPE(NYT::TGuid);

//! A hasher for TGuid.
template <>
struct THash<NYT::TGuid>
{
    size_t operator()(const NYT::TGuid& guid) const;
};

////////////////////////////////////////////////////////////////////////////////

#define GUID_INL_H_
#include "guid-inl.h"
#undef GUID_INL_H_
