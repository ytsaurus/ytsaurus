#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/hash.h>

namespace NSQLTranslationV1 {

    THashMap<TString, TString> MakeRegexByTokenNameMap(const NSQLReflect::TLexerGrammar& grammar, bool ansi);

} // namespace NSQLTranslationV1
