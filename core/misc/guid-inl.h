#pragma once
#ifndef GUID_INL_H_
#error "Direct inclusion of this file is not allowed, include guid.h"
// For the sake of sane code completion.
#include "guid.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TGuid::TGuid()
{
    Parts64[0] = 0;
    Parts64[1] = 0;
}

Y_FORCE_INLINE TGuid::TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3)
{
    Parts32[0] = part0;
    Parts32[1] = part1;
    Parts32[2] = part2;
    Parts32[3] = part3;
}

Y_FORCE_INLINE TGuid::TGuid(ui64 part0, ui64 part1)
{
    Parts64[0] = part0;
    Parts64[1] = part1;
}

Y_FORCE_INLINE bool TGuid::IsEmpty() const
{
    return Parts64[0] == 0 && Parts64[1] == 0;
}

Y_FORCE_INLINE TGuid::operator bool() const
{
    return !IsEmpty();
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void ToProto(NProto::TGuid* protoGuid, TGuid guid)
{
    protoGuid->set_first(guid.Parts64[0]);
    protoGuid->set_second(guid.Parts64[1]);
}

Y_FORCE_INLINE void FromProto(TGuid* guid, const NYT::NProto::TGuid& protoGuid)
{
    guid->Parts64[0] = protoGuid.first();
    guid->Parts64[1] = protoGuid.second();
}

Y_FORCE_INLINE void ToProto(TProtoStringType* protoGuid, TGuid guid)
{
    *protoGuid = guid ? ToString(guid) : TString();
}

Y_FORCE_INLINE void FromProto(TGuid* guid, const TProtoStringType& protoGuid)
{
    *guid = protoGuid ? TGuid::FromString(protoGuid) : TGuid();
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool operator == (TGuid lhs, TGuid rhs)
{
    return lhs.Parts64[0] == rhs.Parts64[0] &&
           lhs.Parts64[1] == rhs.Parts64[1];
}

Y_FORCE_INLINE bool operator != (TGuid lhs, TGuid rhs)
{
    return !(lhs == rhs);
}

Y_FORCE_INLINE bool operator < (TGuid lhs, TGuid rhs)
{
#ifdef __GNUC__
    ui64 lhs0 = __builtin_bswap64(lhs.Parts64[0]);
    ui64 rhs0 = __builtin_bswap64(rhs.Parts64[0]);
    if (lhs0 < rhs0) {
        return true;
    }
    if (lhs0 > rhs0) {
        return false;
    }
    ui64 lhs1 = __builtin_bswap64(lhs.Parts64[1]);
    ui64 rhs1 = __builtin_bswap64(rhs.Parts64[1]);
    return lhs1 < rhs1;
#else
    return memcmp(&lhs, &rhs, sizeof(TGuid)) < 0;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

Y_FORCE_INLINE size_t THash<NYT::TGuid>::operator()(const NYT::TGuid& guid) const
{
    const size_t p = 1000000009; // prime number
    return guid.Parts32[0] +
           guid.Parts32[1] * p +
           guid.Parts32[2] * p * p +
           guid.Parts32[3] * p * p * p;
}
