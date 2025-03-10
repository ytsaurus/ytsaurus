#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

namespace NSQLTranslationV1 {

    THashMap<TString, TString> GetRegexByComplexTokenMap(const NSQLReflect::TGrammarMeta& meta, bool ansi);

}
