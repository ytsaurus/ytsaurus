#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline void GetValue(double* result, const TUnversionedValue& value)
{
    *result = value.Data.Double;
}

inline void GetValue(ui64* result, const TUnversionedValue& value)
{
    *result = value.Data.Uint64;
}

inline void GetValue(i64* result, const TUnversionedValue& value)
{
    *result = value.Data.Int64;
}

inline void GetValue(bool* result, const TUnversionedValue& value)
{
    *result = value.Data.Boolean;
}

inline void GetValue(TStringBuf* result, const TUnversionedValue& value)
{
    *result = TStringBuf(value.Data.String, value.Length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NTableClient
