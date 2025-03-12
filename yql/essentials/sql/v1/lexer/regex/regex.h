#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

namespace NSQLTranslationV1 {

    THashMap<TString, TString> GetRegexByComplexTokenMap(const NSQLReflect::TLexerGrammar& grammar, bool ansi);

} // namespace NSQLTranslationV1
