#include "camel2hyphen.h"

#include <util/string/ascii.h>

namespace NGetoptPb {
    TString CamelToHyphen(const TString& str) {
        TString ret;
        bool upper = true;
        bool skip = false;
        const char* c = str.data();
        for (; c < str.data() + (str.size()); ++c) {
            if (IsAsciiAlnum(*c)) {
                break;
            }
        }
        for (; c < str.data() + (str.size()); ++c) {
            if (!IsAsciiAlnum(*c)) {
                skip = true;
                continue;
            } else if (skip) {
                ret.append('-');
                skip = false;
                upper = true;
            }
            if (IsAsciiUpper(*c) && !upper) {
                ret.append('-');
            }
            ret.append(AsciiToLower(*c));
            upper = IsAsciiUpper(*c);
        }
        return ret;
    }

}
