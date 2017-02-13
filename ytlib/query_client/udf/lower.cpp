#include <yt_udf_cpp.h>

#include <util/charset/utf8.h>

#include <ctype.h>

extern "C" void lower(
    TExpressionContext* context,
    char** result,
    int* result_len,
    char* s,
    int s_len)
{
    auto lowered = ToLowerUTF8(TStringBuf(s, s_len));

    *result = AllocateBytes(context, lowered.size());
    for (int i = 0; i < lowered.size(); i++) {
        (*result)[i] = lowered[i];
    }
    *result_len = lowered.size();
}
