#include "yt_udf.h"

char is_prefix(
    TExpressionContext* context,
    char* pattern_begin,
    int pattern_length,
    char* data_begin,
    int data_length)
{
    (void)context;
    char* pattern_end = pattern_begin + pattern_length;

    if (pattern_length > data_length) {
        return 0;
    }

    while (pattern_begin != pattern_end && *pattern_begin == *data_begin) {
        ++pattern_begin;
        ++data_begin;
    }

    return pattern_begin == pattern_end;
}
