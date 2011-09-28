#pragma once

#include "common.h"

#include <util/generic/typetraits.h>
#include <quality/Misc/Guid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TGuid
{
    ui32 Parts[4];

    //! Empty constructor.
    TGuid();

    //! Constructor from parts.
    TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3);

    //! Copy constructor.
    TGuid(const TGuid& guid); // copy ctor

    //! Conversion from quality/Misc/TGUID.
    TGuid(const TGUID& guid);

    //! Conversion to quality/Misc/TGUID.
    operator TGUID() const;

    //! Checks if TGuid hasn't been created yet.
    bool IsEmpty() const;

    //! Creates a new instance.
    static TGuid Create();

    //! Conversion to Stroka.
    Stroka ToString() const;

    //! Conversion from Stroka, throws an exception if something went wrong.
    static TGuid FromString(const Stroka& str);

    //! Conversion from Stroka, returns true if everything was ok.
    static bool FromString(const Stroka &str, TGuid* guid);

    //! Conversion to protobuf type, which we mapped to Stroka
    Stroka ToProto() const;

    //! Conversion from protobuf type.
    static TGuid FromProto(const Stroka& protoGuid);
};

bool operator==(const TGuid &a, const TGuid &b);
bool operator!=(const TGuid &a, const TGuid &b);
bool operator<(const TGuid &a, const TGuid &b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

//! A hasher for TGuid.
template<>
struct hash<NYT::TGuid>
{
    inline size_t operator()(const NYT::TGuid &a) const
    {
        return a.Parts[0] + a.Parts[1] + a.Parts[2] + a.Parts[3];
    }
};

// TODO: consider removing TGuid::ToString
inline Stroka ToString(const NYT::TGuid& guid)
{
    return guid.ToString();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_PODTYPE(NYT::TGuid)

