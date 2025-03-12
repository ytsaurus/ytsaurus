#pragma once

#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>

namespace NSQLReflect {

    class ILexerGrammar {
    public:
        using TPtr = THolder<ILexerGrammar>;

        virtual const THashSet<TString>& GetKeywords() const = 0;
        virtual const THashSet<TString>& GetPunctuation() const = 0;
        virtual const THashSet<TString>& GetOther() const = 0;

        virtual const TString& GetContentByName(const TString& name) const = 0;

        virtual ~ILexerGrammar() = default;
    };

    ILexerGrammar::TPtr LoadLexerGrammar();

} // namespace NSQLReflect
