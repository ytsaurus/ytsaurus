#pragma once
#ifndef UDF_C_ABI_INL_H_
#error "Direct inclusion of this file is not allowed, include udf_c_abi.h"
// For the sake of sane code completion.
#include "udf_c_abi.h"
#endif

////////////////////////////////////////////////////////////////////////////////

inline void ClearValue(TUnversionedValue* value)
{
    value->Id = 0;
    value->Type = VT_Min; // effectively 0
    value->Aggregate = 0;
    value->Length = 0;
    value->Data.String = 0;
}

////////////////////////////////////////////////////////////////////////////////
