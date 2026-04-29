#include "ldap_helpers.h"

#include <util/string/subst.h>

namespace NYT::NAuth::NDetail {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf LoginPlaceholder = "{login}";

TString LdapEscapeFilterValue(TStringBuf value)
{
    TString result;
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

TString BuildSearchFilter(TStringBuf filterTemplate, TStringBuf login)
{
    auto result = TString(filterTemplate);
    SubstGlobal(result, LoginPlaceholder, LdapEscapeFilterValue(login));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth::NDetail
