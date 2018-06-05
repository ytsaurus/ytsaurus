#include "yt_udf_cpp.h"
#include <util/string/cast.h>
#include <util/digest/multi.h>

extern "C" uint64_t bigb_hash(
    TExpressionContext* context,
    char* s,
    int len)
{
    TStringBuf str{s, static_cast<size_t>(len)};
    if (str.StartsWith('y')) {
        str.Skip(1);
        ui64 ans;
        if (TryFromString(str, ans)) {
            return ans;
        } else {
            return 0;
        }
    } else if (str.StartsWith('p')) {
        str.Skip(1);
        ui64 ans;
        if (TryFromString(str, ans)) {
            return MultiHash(TStringBuf{"puid"}, ans);
        } else {
            return 0;
        }
    }
    auto index = str.find('/');
    if (index != str.npos) {
        TStringBuf type{str.data(), str.data() + index};
        TStringBuf id{str.data() + index + 1, str.data() + str.length()};
        return MultiHash(type, id);
    }
    return 0;

}
