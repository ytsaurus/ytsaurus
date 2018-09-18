#pragma once

#include "common.h"

#include <yt/core/misc/proto/guid.pb.h>

#include <util/generic/typetraits.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TGuid
{
    union
    {
        ui32 Parts32[4];
        ui64 Parts64[2];
    };

    //! Constructs a null (zero) guid.
    TGuid();

    //! Constructs guid from parts.
    TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3);

    //! Constructs guid from parts.
    TGuid(ui64 part0, ui64 part1);

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
};

void ToProto(NProto::TGuid* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const NProto::TGuid& protoGuid);

void ToProto(TProtoStringType* protoGuid, TGuid guid);
void FromProto(TGuid* guid, const TProtoStringType& protoGuid);

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
