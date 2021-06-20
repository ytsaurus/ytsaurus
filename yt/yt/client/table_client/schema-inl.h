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

constexpr bool operator < (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return static_cast<int>(lhs) < static_cast<int>(rhs);
}

constexpr bool operator > (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return rhs < lhs;
}

constexpr bool operator <= (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return !(rhs < lhs);
}

constexpr bool operator >= (ESchemaCompatibility lhs, ESchemaCompatibility rhs)
{
    return !(lhs < rhs);
}

////////////////////////////////////////////////////////////////////////////////

inline size_t TTableSchemaHash::operator() (const TTableSchema& schema) const
{
    return THash<TTableSchema>()(schema);
}

inline size_t TTableSchemaHash::operator() (const TTableSchemaPtr& schema) const
{
    return THash<TTableSchema>()(*schema);
}

////////////////////////////////////////////////////////////////////////////////

inline bool TTableSchemaEquals::operator() (const TTableSchema& lhs, const TTableSchema& rhs) const
{
    return lhs == rhs;
}

inline bool TTableSchemaEquals::operator() (const TTableSchemaPtr& lhs, const TTableSchemaPtr& rhs) const
{
    return *lhs == *rhs;
}

inline bool TTableSchemaEquals::operator() (const TTableSchemaPtr& lhs, const TTableSchema& rhs) const
{
    return *lhs == rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
