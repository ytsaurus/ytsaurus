#include "yql.h"

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;

class TSQLv1Parser {
public:
    TSQLv1Parser() {
        Settings_.Arena = &Arena_;
        Settings_.ClusterMapping = {
            {"socrates", TString(NYql::YtProviderName)},
            {"plato", TString(NYql::YtProviderName)},
        };
        Settings_.SyntaxVersion = 1;

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();

        Translator_ = NSQLTranslationV1::MakeTranslator(lexers, parsers);
    }

    NYql::TAstParseResult Parse(const TString& query) {
        Arena_.Reset();

        NSQLTranslation::TTranslators translators(
            /* V0 = */ nullptr,
            /* V1 = */ Translator_,
            /* PG = */ nullptr);

        auto result = NSQLTranslation::SqlToYql(translators, query, Settings_);
        Y_ENSURE(result.IsOk());
        return result;
    }

private:
    google::protobuf::Arena Arena_;
    NSQLTranslation::TTranslationSettings Settings_;
    NSQLTranslation::TTranslatorPtr Translator_;
};

TYqlContext Analyze(const TString& query) {
    auto ast = TSQLv1Parser().Parse(query);

    NYql::TIssues issues;
    return *MakeYqlAnalysis()->Analyze(*ast.Root, issues);
}

Y_UNIT_TEST_SUITE(YqlAnalysisTests) {

    Y_UNIT_TEST(NamesAreCollected) {
        TString input = R"(
            USE yt:socrates;

            SELECT * FROM Input;

            CREATE TABLE Newbie (x Unit);

            INSERT INTO plato.Input (id) VALUES (1);
        )";

        THashMap<TString, THashSet<TString>> expected = {
            {"socrates", {"Input", "Newbie"}},
            {"plato", {"Input"}},
        };

        UNIT_ASSERT_VALUES_EQUAL(Analyze(input).TablesByCluster, expected);
    }

} // Y_UNIT_TEST_SUITE(YqlAnalysisTests)
