#pragma once

#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>

namespace NSQLReflect {

    class ILexerGrammar {
    public:
        using TPtr = THolder<ILexerGrammar>;

        virtual const THashSet<TString>& GetKeywordTokenNames() const = 0;
        virtual const THashSet<TString>& GetPunctuationTokenNames() const = 0;
        virtual const THashSet<TString>& GetOtherTokenNames() const = 0;

        virtual const TString& GetRuleBlockByTokenName(const TString& name) const = 0;

        virtual ~ILexerGrammar() = default;
    };

    ILexerGrammar::TPtr LoadLexerGrammar();

} // namespace NSQLReflect
