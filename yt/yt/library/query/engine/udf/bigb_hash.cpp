#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <util/string/cast.h>

#include <util/digest/multi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" uint64_t bigb_hash(
    TExpressionContext* /*context*/,
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
