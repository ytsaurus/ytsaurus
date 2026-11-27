#include "proto_parser.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ProtoParserTests) {

google::protobuf::Message* SqlAST(
    const TString& query,
    NSQLTranslation::TTranslationSettings settings,
    NYql::TIssues& issues,
    bool checkAmbiguity = false)
{
    NSQLTranslationV1::TLexers lexers;
    NSQLTranslationV1::TParsers parsers;

    if (!ParseTranslationSettings(query, settings, issues)) {
        return nullptr;
    }

    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer);
    auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
        Y_UNUSED(token);
    };

    if (!lexer->Tokenize(query, "", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
        return nullptr;
    }

    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
        /*isAmbiguityError=*/checkAmbiguity,
        /*isAmbiguityDebugging=*/checkAmbiguity,
        settings.MaxParseTreeDepth);

    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
        /*isAmbiguityError=*/checkAmbiguity,
        /*isAmbiguityDebugging=*/checkAmbiguity,
        settings.MaxParseTreeDepth);

    return NSQLTranslationV1::SqlAST(
        parsers,
        query,
        /* queryName = */ "",
        issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        settings.AnsiLexer,
        settings.Arena);
}

TString GenerateQuery(size_t depth = 8 * 1024) {
    const TString prefix = "SELECT 1 FROM (";
    const TString core = "SELECT 1";
    const TString suffix = ")";

    TString query;
    query.reserve(prefix.size() * depth + core.size() + suffix.size() * depth);
    for (size_t i = 0; i < depth; ++i) {
        query += prefix;
    }
    query += core;
    for (size_t i = 0; i < depth; ++i) {
        query += suffix;
    }

    return query;
}

Y_UNIT_TEST(StackOverflowSubquery) {
    const auto query = GenerateQuery();

    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.MaxParseTreeDepth = 1 * 1024;

    NYql::TIssues issues;
    const auto* m = SqlAST(query, settings, issues);

    UNIT_ASSERT(!m);
    UNIT_ASSERT_STRING_CONTAINS(issues.ToOneLineString(), "Maximum parse tree depth exceeded");
}

Y_UNIT_TEST(MatchExpression) {
    const TString queries[] = {
        R"(
            SELECT MATCH $value
                WHEN Just(0) THEN 'zero'
                WHEN Just($x) AND $x > 0 THEN 'positive'
                WHEN Just($_) THEN 'negative'
                WHEN Nothing THEN 'missing'
            END AS category;
        )",
        "SELECT MATCH $value WHEN Just(Nothing) THEN 1 ELSE 0 END;",
        "SELECT 1 IN MATCH $value WHEN $_ THEN [1] END;",
        "SELECT MATCH 1 WHEN $_ AND (TRUE OR FALSE) THEN 2 END;",
        "SELECT CASE WHEN TRUE THEN 1 ELSE 0 END AS match;",
        "SELECT 'a' MATCH 'b' AS just, Nothing(Int32?) AS nothing;",
    };
    for (bool ansiLexer : {false, true}) {
        for (const auto& query : queries) {
            google::protobuf::Arena arena;
            NSQLTranslation::TTranslationSettings settings;
            settings.Arena = &arena;
            settings.AnsiLexer = ansiLexer;
            NYql::TIssues issues;
            UNIT_ASSERT_C(SqlAST(query, settings, issues, /*checkAmbiguity=*/true), issues.ToString());
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        }
    }
}

Y_UNIT_TEST(InvalidMatchExpression) {
    const TString queries[] = {
        "SELECT MATCH $value ELSE 0 END;",
        "SELECT MATCH $value WHEN Just() THEN 1 END;",
        "SELECT MATCH $value WHEN $_ THEN 1;",
        "SELECT MATCH $value WHEN $_ AND TRUE OR FALSE THEN 1 END;",
    };
    for (const auto& query : queries) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        NYql::TIssues issues;
        UNIT_ASSERT_C(!SqlAST(query, settings, issues, /*checkAmbiguity=*/true), query);
        UNIT_ASSERT_C(!issues.Empty(), query);
    }
}

} // Y_UNIT_TEST_SUITE(ProtoParserTests)
