#include "case.h"

namespace NSQLComplete {

    bool NoCaseCompare(const TString& lhs, const TString& rhs) {
        return std::lexicographical_compare(
            std::begin(lhs), std::end(lhs),
            std::begin(rhs), std::end(rhs),
            [](const char lhs, const char rhs) {
                return ToLower(lhs) < ToLower(rhs);
            });
    }

} // namespace NSQLComplete
