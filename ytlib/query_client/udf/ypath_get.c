#include "yt_udf.h"

#define DEFINE_YPATH_GET_IMPL(PREFIX, prefix, TYPE, type) \
    void PREFIX ## Get ## TYPE( \
        TExpressionContext* context, \
        TUnversionedValue* result, \
        TUnversionedValue* anyValue, \
        TUnversionedValue* ypath); \
    \
    void prefix ## get_ ## type( \
        TExpressionContext* context, \
        TUnversionedValue* result, \
        TUnversionedValue* anyValue, \
        TUnversionedValue* ypath) \
    { \
        if (anyValue->Type == Null || ypath->Type == Null) { \
            result->Type = Null; \
        } else { \
            PREFIX ## Get ## TYPE(context, result, anyValue, ypath); \
        } \
    }

#define DEFINE_YPATH_GET(TYPE, type) \
    DEFINE_YPATH_GET_IMPL(Try, try_, TYPE, type) \
    DEFINE_YPATH_GET_IMPL(, , TYPE, type)

DEFINE_YPATH_GET(Int64, int64)
DEFINE_YPATH_GET(Uint64, uint64)
DEFINE_YPATH_GET(Double, double)
DEFINE_YPATH_GET(Boolean, boolean)
DEFINE_YPATH_GET(String, string)
DEFINE_YPATH_GET(Any, any)
