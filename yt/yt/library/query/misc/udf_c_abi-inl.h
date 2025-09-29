#ifndef UDF_C_ABI_INL_H_
#error "Direct inclusion of this file is not allowed, include udf_c_abi.h"
// For the sake of sane code completion.
#include "udf_c_abi.h"
#endif

#include "platform_specific.h"

#include <string.h>

////////////////////////////////////////////////////////////////////////////////

inline void WASM_WEAK ClearValue(TUnversionedValue* value)
{
    memset(value, 0, sizeof(*value));
}

////////////////////////////////////////////////////////////////////////////////
