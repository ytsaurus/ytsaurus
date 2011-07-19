#pragma once

#include "common.h"

#include <quality/Misc/Guid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TGuid
{
    ui32 parts[4];

    TGuid() { Zero(*this); }

    TGuid(const TGUID& guid); // TGuid <- TGUID
    TGuid(const TGuid& guid); // copy ctor
    operator TGUID() const;   // TGUID <- TGuid

    bool IsEmpty() const { return (parts[0] | parts[1] | parts[2] | parts[3]) == 0; }

    static TGuid Create();
    Stroka ToString() const;
    static TGuid FromString(const Stroka& str);
    static bool FromString(const Stroka &str, TGuid* guid);
};
inline bool operator==(const TGuid &a, const TGuid &b) { return memcmp(&a, &b, sizeof(a)) == 0; }
inline bool operator!=(const TGuid &a, const TGuid &b) { return !(a == b); }

struct TGuidHash
{
    int operator()(const TGuid &a) const { return a.parts[0] + a.parts[1] + a.parts[2] + a.parts[3]; }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
