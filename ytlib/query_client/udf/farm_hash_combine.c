#include "yt_udf.h"

uint64_t farm_hash_combine(
    TExecutionContext* context,
    uint64_t lhs,
    uint64_t rhs)
{
    const uint64_t mul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (lhs ^ rhs) * mul;
    a ^= (a >> 47);
    uint64_t b = (rhs ^ a) * mul;
    b ^= (b >> 47);
    b *= mul;
    return b;
}
