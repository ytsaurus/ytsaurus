#ifndef GUID_INL_H_
#error "Direct inclusion of this file is not allowed, include guid.h"
#endif
#undef GUID_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

FORCED_INLINE TGuid::TGuid()
{
    Parts64[0] = 0;
    Parts64[1] = 0;
}

FORCED_INLINE TGuid::TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3)
{
    Parts32[0] = part0;
    Parts32[1] = part1;
    Parts32[2] = part2;
    Parts32[3] = part3;
}

FORCED_INLINE TGuid::TGuid(ui64 part0, ui64 part1)
{
    Parts64[0] = part0;
    Parts64[1] = part1;
}

FORCED_INLINE bool TGuid::IsEmpty() const
{
    return Parts64[0] == 0 && Parts64[1] == 0;
}

////////////////////////////////////////////////////////////////////////////////

FORCED_INLINE void ToProto(NProto::TGuid* protoGuid, const TGuid& guid)
{
    protoGuid->set_first(guid.Parts64[0]);
    protoGuid->set_second(guid.Parts64[1]);
}

FORCED_INLINE void FromProto(TGuid* guid, const NYT::NProto::TGuid& protoGuid)
{
    guid->Parts64[0] = protoGuid.first();
    guid->Parts64[1] = protoGuid.second();
}

////////////////////////////////////////////////////////////////////////////////

FORCED_INLINE bool operator == (const TGuid& lhs, const TGuid& rhs)
{
    return lhs.Parts64[0] == rhs.Parts64[0] &&
           lhs.Parts64[1] == rhs.Parts64[1];
}

FORCED_INLINE bool operator != (const TGuid& lhs, const TGuid& rhs)
{
    return !(lhs == rhs);
}

FORCED_INLINE bool operator < (const TGuid& lhs, const TGuid& rhs)
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
