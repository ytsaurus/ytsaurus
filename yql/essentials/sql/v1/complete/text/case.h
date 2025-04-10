#pragma once

#include <util/generic/string.h>

namespace NSQLComplete {

    inline bool NoCaseCompare(const TString& lhs, const TString& rhs) {
        return std::lexicographical_compare(
            std::begin(lhs), std::end(lhs),
            std::begin(rhs), std::end(rhs),
            [](const char lhs, const char rhs) {
                return ToLower(lhs) < ToLower(rhs);
            });
    }

    inline auto NoCaseCompareLimit(size_t size) {
        return [size](const TString& lhs, const TString& rhs) -> bool {
            return strncasecmp(lhs.data(), rhs.data(), size) < 0;
        };
    }

} // namespace NSQLComplete
