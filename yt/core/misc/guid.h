#pragma once

#include "common.h"

#include <util/generic/typetraits.h>

#include <core/misc/guid.pb.h>

#include <core/ytree/public.h>

#include <array>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TGuid
{
    std::array<ui32, 4> Parts;

    //! Constructs a null (zero) guid.
    TGuid();

    //! Constructs guid from parts.
    TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3);

    //! Constructs guid from parts.
    TGuid(ui64 part0, ui64 part1);

    //! Checks if TGuid is zero.
    bool IsEmpty() const;

    //! Creates a new instance.
    static TGuid Create();

    //! Conversion from TStringBuf, throws an exception if something went wrong.
    static TGuid FromString(const TStringBuf& str);

    //! Conversion from TStringBuf, returns |true| if everything was ok.
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
        ui32 p = 1000000009; // prime number
        return guid.Parts[0] +
               guid.Parts[1] * p +
               guid.Parts[2] * p * p +
               guid.Parts[3] * p * p * p;
    }
};

////////////////////////////////////////////////////////////////////////////////
