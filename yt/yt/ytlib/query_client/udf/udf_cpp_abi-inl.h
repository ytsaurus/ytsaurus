#pragma once
#ifndef UDF_CPP_ABI_INL_H_
#error "Direct inclusion of this file is not allowed, include udf_cpp_abi.h"
// For the sake of sane code completion.
#include "udf_cpp_abi.h"
#endif

namespace NYT::NQueryClient::NUdf {

////////////////////////////////////////////////////////////////////////////////

inline void ClearValue(TUnversionedValue* value)
{
    value->Id = 0;
    value->Type = EValueType::Min; // effectively 0
    value->Aggregate = 0;
    value->Length = 0;
    value->Data.String = nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NUdf
