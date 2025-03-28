#pragma once

#include <yql/essentials/sql/v1/complete/sql_complete.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TCompletionContext {
        TVector<TString> Keywords;
        bool IsTypeName;
    };

    class ISqlContextInference {
    public:
        using TPtr = THolder<ISqlContextInference>;

        virtual TCompletionContext Analyze(TCompletionInput input) = 0;
        virtual ~ISqlContextInference() = default;
    };

    ISqlContextInference::TPtr MakeSqlContextInference(TLexerSupplier lexer);

} // namespace NSQLComplete
