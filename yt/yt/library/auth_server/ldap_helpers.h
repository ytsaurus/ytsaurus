#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NYT::NAuth::NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Escapes a string for safe use in LDAP filter values (RFC 4515).
TString LdapEscapeFilterValue(TStringBuf value);

//! Substitutes {login} placeholder in filterTemplate with the escaped login.
TString BuildSearchFilter(TStringBuf filterTemplate, TStringBuf login);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth::NDetail
