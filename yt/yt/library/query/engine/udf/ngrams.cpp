#include <yt/yt/library/query/misc/udf_c_abi.h>

extern "C" void MakeNgrams(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* text,
    TUnversionedValue* n);

extern "C" void make_ngrams(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* text,
    TUnversionedValue* n)
{
    MakeNgrams(context, result, text, n);
}
