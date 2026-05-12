#include "ldap_helpers.h"

#include <util/string/subst.h>

namespace NYT::NAuth::NDetail {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf LoginPlaceholder = "{login}";

std::string LdapEscapeFilterValue(TStringBuf value)
{
    std::string result;
    result.reserve(value.size());
    for (char c : value) {
        switch (c) {
            case '\\': result += "\\5c"; break;
            case '*':  result += "\\2a"; break;
            case '(':  result += "\\28"; break;
            case ')':  result += "\\29"; break;
            case '\0': result += "\\00"; break;
            default:   result += c;      break;
        }
    }
    return result;
}

std::string BuildSearchFilter(TStringBuf filterTemplate, TStringBuf login)
{
    auto result = std::string(filterTemplate);
    SubstGlobal(result, LoginPlaceholder, LdapEscapeFilterValue(login));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth::NDetail
