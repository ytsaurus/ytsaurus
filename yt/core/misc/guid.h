#pragma once

#include "common.h"

#include <util/generic/typetraits.h>

#include <core/misc/guid.pb.h>

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

    //! Creates a new instance.
    static TGuid Create();

    //! Parses guid from TStringBuf, throws an exception if something went wrong.
    static TGuid FromString(const TStringBuf& str);

    //! Parses guid from TStringBuf, returns |true| if everything was ok.
    static bool FromString(const TStringBuf& str, TGuid* guid);
};

void ToProto(NProto::TGuid* protoGuid, const TGuid& guid);
void FromProto(TGuid* guid, const NProto::TGuid& protoGuid);

Stroka ToString(const TGuid& guid);

bool operator == (const TGuid& lhs, const TGuid& rhs);
bool operator != (const TGuid& lhs, const TGuid& rhs);
bool operator <  (const TGuid& lhs, const TGuid& rhs);


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

DECLARE_PODTYPE(NYT::TGuid)

//! A hasher for TGuid.
template <>
struct hash<NYT::TGuid>
{
    inline size_t operator()(const NYT::TGuid& guid) const
    {
        const size_t p = 1000000009; // prime number
        return guid.Parts32[0] +
               guid.Parts32[1] * p +
               guid.Parts32[2] * p * p +
               guid.Parts32[3] * p * p * p;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define GUID_INL_H_
#include "guid-inl.h"
#undef GUID_INL_H_
