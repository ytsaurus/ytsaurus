#include "yt_udf_cpp.h"
#include <util/string/cast.h>
#include <util/digest/multi.h>

extern "C" uint64_t bigb_hash(
    TExpressionContext* context,
    char* s,
    int len)
{
    TStringBuf uid{s, static_cast<size_t>(len)};
    if (uid.length() == 0) {
        return 0;
    }
    ui64 ans;
    if (uid[0] == 'y' && TryFromString(uid.SubStr(1), ans)) {
        return ans;
    }
    return MultiHash(TStringBuf{"shard"}, uid);
}
