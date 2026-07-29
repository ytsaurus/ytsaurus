#pragma once

#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>

#include <library/cpp/json/json_value.h>

class TCompletionFactory {
public:
    explicit TCompletionFactory(NSQLComplete::TFrequencyData frequency);

    NSQLComplete::ISqlCompletionEngine::TPtr MakeEngine(
        const NJson::TJsonValue* schema = nullptr) const;

private:
    NSQLComplete::IRanking::TPtr Ranking_;
    NSQLComplete::INameService::TPtr StaticNames_;
    NSQLComplete::TLexerSupplier Lexer_;
};
