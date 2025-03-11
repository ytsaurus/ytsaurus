#pragma once

#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>

namespace NSQLReflect {

    struct TLexerGrammar {
        THashSet<TString> Keywords;
        THashSet<TString> Punctuation;
        THashSet<TString> Other;
        THashMap<TString, TString> ContentByName;
        THashMap<TString, THashMap<TString, TString>> Substitutions;
    };

    TLexerGrammar GetLexerGrammar();

} // namespace NSQLReflect
