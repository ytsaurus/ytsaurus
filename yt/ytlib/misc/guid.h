#pragma once

#include "common.h"

#include <util/generic/typetraits.h>

#include <ytlib/misc/guid.pb.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TGuid
{
    ui32 Parts[4];

    //! Empty constructor.
    TGuid();

    //! Constructor from parts.
    TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3);

    //! Constructor from parts.
    TGuid(ui64 part0, ui64 part1);

    //! Copy constructor.
    TGuid(const TGuid& guid);

    //! Checks if TGuid is zero.
    bool IsEmpty() const;

    //! Creates a new instance.
    static TGuid Create();

    //! Conversion to Stroka.
    Stroka ToString() const;

    //! Conversion from TStringBuf, throws an exception if something went wrong.
    static TGuid FromString(const TStringBuf& str);

    //! Conversion from TStringBuf, returns true if everything was ok.
    static bool FromString(const TStringBuf& str, TGuid* guid);

    //! Conversion to protobuf type, which we mapped to Stroka
    NProto::TGuid ToProto() const;

    //! Conversion from protobuf type.
    static TGuid FromProto(const NProto::TGuid& protoGuid);
};

bool operator == (const TGuid &a, const TGuid &b);
bool operator != (const TGuid &a, const TGuid &b);
bool operator <  (const TGuid &a, const TGuid &b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

DECLARE_PODTYPE(NYT::TGuid)

// TODO: consider removing TGuid::ToString
inline Stroka ToString(const NYT::TGuid& guid)
{
    return guid.ToString();
}

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

namespace std {

template <>
struct hash<NYT::TGuid>
{
    inline size_t operator()(const NYT::TGuid& guid) const
    {
        return ::hash<NYT::TGuid>()(guid);
    }
};

} // namespace std

////////////////////////////////////////////////////////////////////////////////


