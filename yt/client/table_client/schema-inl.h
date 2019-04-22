#pragma once
#ifndef SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include schema.h"
// For the sake of sane code completion.
#include "schema.h"
#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline TLockMask::TLockMask(TLockBitmap value)
    : Data_(value)
{ }

inline ELockType TLockMask::Get(int index) const
{
    return ELockType((Data_ >> (BitsPerType * index)) & TypeMask);
}

inline void TLockMask::Set(int index, ELockType lock)
{
    Data_ &= ~(TypeMask << (BitsPerType * index));
    Data_ |= static_cast<TLockBitmap>(lock) << (BitsPerType * index);
}

inline void TLockMask::Enrich(int columnCount)
{
    auto primaryLockType = Get(PrimaryLockIndex);
    auto maxLockType = primaryLockType;
    for (int index = 1; index < columnCount; ++index) {
        auto lockType = Get(index);
        Set(index, std::max(primaryLockType, lockType));
        maxLockType = std::max(maxLockType, lockType);
    }
    Set(PrimaryLockIndex, maxLockType);
}

inline TLockMask::operator TLockBitmap() const
{
    return Data_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
