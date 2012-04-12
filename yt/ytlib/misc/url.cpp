
#include "url.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// copied from util/string/url.cpp
namespace {

#define X(c) (c >= 'A' ? ((c & 0xdf) - 'A') + 10 : (c - '0'))

static inline int x2c(unsigned char* x) {
    if (!isxdigit(x[0]) || !isxdigit(x[1]))
        return -1;
    return X(x[0]) * 16 + X(x[1]);
}

#undef X

static inline int Unescape(char *str) {
    char *to, *from;
    int dlen = 0;
    if ((str = strchr(str, '%')) == 0)
        return dlen;
    for (to = str, from = str; *from; from++, to++) {
        if ((*to = *from) == '%') {
            int c = x2c((unsigned char *)from+1);
            *to = char((c > 0) ? c : '0');
            from += 2;
            dlen += 2;
        }
    }
    *to = 0;  /* terminate it at the new length */
    return dlen;
}

}

Stroka UnescapeUrl(const Stroka& url)
{
    Stroka result = url;
    int dlen = Unescape(result.begin());
    result.resize(url.size() - dlen);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
