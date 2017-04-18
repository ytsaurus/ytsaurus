#include "yt_udf.h"

int8_t is_substr(
    TExpressionContext* context,
    char* s2,
    int s2_len,
    char* s1,
    int s1_len)
{
    (void)context;
    if (s2_len == 0) {
        return 1;
    }

    char* s1_end = s1 + s1_len;
    char* s2_end = s2 + s2_len;

    while (s1 != s1_end) {
        char* it1 = s1;
        char* it2 = s2;
        while (*it1 == *it2) {
            ++it1;
            ++it2;
            if (it2 == s2_end) {
                return 1;
            }
            if (it1 == s1_end) {
                return 0;
            }
        }
        ++s1;
    }
    return 0;
}
