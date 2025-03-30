#include "lexer.h"
#include "lexer_ut.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <yql/essentials/sql/v1/lexer/antlr3/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/regex/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

#define UNIT_ASSERT_TOKENIZED(LEXER, QUERY, TOKENS) \
    do {                                            \
        auto tokens = Tokenized((LEXER), (QUERY));  \
        UNIT_ASSERT_VALUES_EQUAL(tokens, (TOKENS)); \
    } while (false)

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;

TLexers Lexers = {
    .Antlr3 = MakeAntlr3LexerFactory(),
    .Antlr3Ansi = MakeAntlr4AnsiLexerFactory(),
    .Antlr4 = MakeAntlr4LexerFactory(),
    .Antlr4Ansi = MakeAntlr4AnsiLexerFactory(),
    .Antlr4Pure = MakeAntlr4PureLexerFactory(),
    .Antlr4PureAnsi = MakeAntlr4PureAnsiLexerFactory(),
    .Regex = MakeRegexLexerFactory(/* ansi = */ false),
    .RegexAnsi = MakeRegexLexerFactory(/* ansi = */ true),
};

std::pair<TParsedTokenList, NYql::TIssues> Tokenize(ILexer::TPtr& lexer, const TString& query) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    Tokenize(*lexer, query, "", tokens, issues, SQL_MAX_PARSER_ERRORS);
    return {tokens, issues};
}

TVector<TString> GetIssueMessages(ILexer::TPtr& lexer, const TString& query) {
    TVector<TString> messages;
    for (const auto& issue : Tokenize(lexer, query).second) {
        messages.emplace_back(issue.ToString(/* oneLine = */ true));
    }
    return messages;
}

TVector<TString> GetTokenViews(ILexer::TPtr& lexer, const TString& query) {
    TVector<TString> names;
    for (auto& token : Tokenize(lexer, query).first) {
        TString view = std::move(token.Name);
        if (view == "ID_PLAIN" || view == "STRING_VALUE") {
            view.append(" (");
            view.append(token.Content);
            view.append(")");
        }
        names.emplace_back(std::move(view));
    }
    return names;
}

TString ToString(TParsedToken token) {
    TString& string = token.Name;
    if (token.Name != token.Content && token.Name != "EOF") {
        string += "(";
        string += token.Content;
        string += ")";
    }
    return string;
}

TString Tokenized(ILexer::TPtr& lexer, const TString& query) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    bool ok = Tokenize(*lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS);

    TString out;
    if (!ok) {
        out = "[INVALID] ";
    }

    for (auto& token : tokens) {
        out += ToString(std::move(token));
        out += " ";
    }
    if (!out.empty()) {
        out.pop_back();
    }
    return out;
}

void AssertEq(const TParsedToken& lhs, const TParsedToken& rhs) {
    if (lhs.Name == "EOF" && rhs.Name == "EOF") {
        return;
    }

    UNIT_ASSERT_VALUES_EQUAL(lhs.Name, rhs.Name);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Content, rhs.Content);
    UNIT_ASSERT_VALUES_EQUAL(lhs.Line, rhs.Line);
}

void AssertEq(const TParsedTokenList& lhs, const TParsedTokenList& rhs) {
    UNIT_ASSERT_VALUES_EQUAL(lhs.size(), rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        AssertEq(lhs.at(i), rhs.at(i));
    }
}

Y_UNIT_TEST_SUITE(SQLv1Lexer) {
    Y_UNIT_TEST(UnsupportedIssues) {
        NSQLTranslationV1::TLexers factories;

        TVector<ILexer::TPtr> lexers;
        for (auto ansi : {false, true}) {
            for (auto antlr4 : {false, true}) {
                for (auto flavor : {ELexerFlavor::Default, ELexerFlavor::Pure, ELexerFlavor::Regex}) {
                    lexers.emplace_back(MakeLexer(factories, ansi, antlr4, flavor));
                }
            }
        }

        TVector<TString> actual;
        for (auto& lexer : lexers) {
            auto issues = GetIssueMessages(lexer, "");
            actual.emplace_back(std::move(issues.at(0)));
        }

        TVector<TString> expected = {
            "<main>: Error: Lexer antlr3 is not supported",
            "<main>: Error: Lexer antlr3_pure is not supported",
            "<main>: Error: Lexer regex is not supported",
            "<main>: Error: Lexer antlr4 is not supported",
            "<main>: Error: Lexer antlr4_pure is not supported",
            "<main>: Error: Lexer antlr4_regex is not supported",
            "<main>: Error: Lexer antlr3_ansi is not supported",
            "<main>: Error: Lexer antlr3_pure_ansi is not supported",
            "<main>: Error: Lexer regex_ansi is not supported",
            "<main>: Error: Lexer antlr4_ansi is not supported",
            "<main>: Error: Lexer antlr4_pure_ansi is not supported",
            "<main>: Error: Lexer antlr4_regex_ansi is not supported",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(AntlrVersionIndependent) {
        const TVector<TString> queriesUtf8 = {
            "",
            "   ",
            "SELECT",
            "SEL", // identifier
            "SELECT FROM test",
            "SELECT * FROM",
            "   SELECT * FROM ",
            "SELECT \"\xF0\x9F\x98\x8A\" FROM ydb",
            (
                "SELECT \"\xF0\x9F\x98\x8A Hello, друзья\", count, name\n"
                "FROM table -- главная таблица 数据库 \n"
                "WHERE count < 6\n"
                "  AND name = \"可靠性\"\n"
                "  AND count > 12"),
            "\"select\"select",
        };

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr3 = NSQLTranslationV1::MakeAntlr3LexerFactory();
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();

        auto lexer3 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);
        auto lexer4p = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true, ELexerFlavor::Pure);

        for (const auto& query : queriesUtf8) {
            auto [tokens3, issues3] = Tokenize(lexer3, query);
            auto [tokens4, issues4] = Tokenize(lexer4, query);
            auto [tokens4p, issues4p] = Tokenize(lexer4p, query);
            AssertEq(tokens3, tokens4);
            AssertEq(tokens3, tokens4p);
            UNIT_ASSERT(issues3.Empty());
            UNIT_ASSERT(issues4.Empty());
            UNIT_ASSERT(issues4p.Empty());
        }
    }

    TVector<TString> InvalidQueries();

    void TestInvalidTokensSkipped(bool antlr4, const TVector<TVector<TString>>& expected) {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr3 = NSQLTranslationV1::MakeAntlr3LexerFactory();
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

        auto lexer = MakeLexer(lexers, /* ansi = */ false, antlr4);

        auto input = InvalidQueries();
        UNIT_ASSERT_VALUES_EQUAL(input.size(), expected.size());

        for (size_t i = 0; i < input.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(GetTokenViews(lexer, input[i]), expected[i]);
        }
    }

    TVector<TString> InvalidQueries() {
        return {
            /* 0: */ "\xF0\x9F\x98\x8A",
            /* 1: */ "select \"aaaa",
            /* 2: */ "\"\\\"",
            /* 3: */ "\xF0\x9F\x98\x8A SELECT * FR",
            /* 4: */ "! SELECT *  from",
            /* 5: */ "\xF0\x9F\x98\x8Aselect ! from",
            /* 6: */ "\"",
            /* 7: */ "!select",
            /* 8: */ "SELECT \\\"\xF0\x9F\x98\x8A\\\" FROM test",
        };
    }

    Y_UNIT_TEST(ErrorRecoveryAntlr3) {
        TVector<TVector<TString>> actual = {
            /* 0: */ {"EOF"},
            /* 1: */ {"SELECT", "WS", "EOF"},
            /* 2: */ {"EOF"},
            /* 3: */ {"WS", "SELECT", "WS", "ASTERISK", "WS", "ID_PLAIN (FR)", "EOF"},
            /* 4: */ {"ID_PLAIN (ELECT)", "WS", "ASTERISK", "WS", "WS", "FROM", "EOF"},
            /* 5: */ {"SELECT", "WS", "ID_PLAIN (rom)", "EOF"},
            /* 6: */ {"EOF"},
            /* 7: */ {"ID_PLAIN (lect)", "EOF"},
            /* 8: */ {"SELECT", "WS", "EOF"},
        };
        TestInvalidTokensSkipped(/* antlr4 = */ false, actual);
    }

    Y_UNIT_TEST(ErrorRecoveryAntlr4) {
        TVector<TVector<TString>> actual = {
            /* 0: */ {"EOF"},
            /* 1: */ {"SELECT", "WS", "EOF"},
            /* 2: */ {"EOF"},
            /* 3: */ {"WS", "SELECT", "WS", "ASTERISK", "WS", "ID_PLAIN (FR)", "EOF"},
            /* 4: */ {"SELECT", "WS", "ASTERISK", "WS", "WS", "FROM", "EOF"},
            /* 5: */ {"SELECT", "WS", "FROM", "EOF"},
            /* 6: */ {"EOF"},
            /* 7: */ {"ID_PLAIN (elect)", "EOF"},
            /* 8: */ {"SELECT", "WS", "EOF"},
        };
        TestInvalidTokensSkipped(/* antlr4 = */ true, actual);
    }

    Y_UNIT_TEST(IssuesCollected) {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr3 = NSQLTranslationV1::MakeAntlr3LexerFactory();
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Regex = NSQLTranslationV1::MakeRegexLexerFactory(/* ansi = */ false);

        auto lexer3 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);
        auto lexer4p = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true, ELexerFlavor::Pure);
        auto lexerR = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ false, ELexerFlavor::Regex);

        for (const auto& query : InvalidQueries()) {
            auto issues3 = GetIssueMessages(lexer3, query);
            auto issues4 = GetIssueMessages(lexer4, query);
            auto issues4p = GetIssueMessages(lexer4p, query);
            auto issuesR = GetIssueMessages(lexerR, query);

            UNIT_ASSERT(!issues3.empty());
            UNIT_ASSERT(!issues4.empty());
            UNIT_ASSERT(!issues4p.empty());
            UNIT_ASSERT(!issuesR.empty());
        }
    }

    Y_UNIT_TEST(IssueMessagesAntlr3) {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr3 = NSQLTranslationV1::MakeAntlr3LexerFactory();
        auto lexer3 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ false);

        auto actual = GetIssueMessages(lexer3, "\xF0\x9F\x98\x8A SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: Unexpected character '\xF0\x9F\x98\x8A' (Unicode character <128522>) : cannot match to any predicted input...",
            "<main>:1:1: Error: Unexpected character : cannot match to any predicted input...",
            "<main>:1:2: Error: Unexpected character : cannot match to any predicted input...",
            "<main>:1:3: Error: Unexpected character : cannot match to any predicted input...",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(IssueMessagesAntlr4) {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();

        auto lexer4 = MakeLexer(lexers, /* ansi = */ false, /* antlr4 = */ true);

        auto actual = GetIssueMessages(lexer4, "\xF0\x9F\x98\x8A SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: token recognition error at: '\xF0\x9F\x98\x8A'",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Whitespace) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "", "EOF");
        UNIT_ASSERT_TOKENIZED(lexer, " ", "WS( ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "  ", "WS( ) WS( ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\n", "WS(\n) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Keyword) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "SELECT", "SELECT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "INSERT", "INSERT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "FROM", "FROM EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Punctuation) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(
            lexer,
            "* / + - <|",
            "ASTERISK(*) WS( ) SLASH(/) WS( ) "
            "PLUS(+) WS( ) MINUS(-) WS( ) STRUCT_OPEN(<|) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "SELECT*FROM", "SELECT ASTERISK(*) FROM EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(IdPlain) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "variable my_table", "ID_PLAIN(variable) WS( ) ID_PLAIN(my_table) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(IdQuoted) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "``", "ID_QUOTED(``) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "` `", "ID_QUOTED(` `) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "`local/table`", "ID_QUOTED(`local/table`) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(SingleLineString) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "\"\"", "STRING_VALUE(\"\") EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\' \'", "STRING_VALUE(\' \') EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\" \"", "STRING_VALUE(\" \") EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\"test\"", "STRING_VALUE(\"test\") EOF");

        if (!ANSI) {
            UNIT_ASSERT_TOKENIZED(lexer, "\"\\\"\"", "STRING_VALUE(\"\\\"\") EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "\"\"\"\"", "STRING_VALUE(\"\") STRING_VALUE(\"\") EOF");
        } else {
            UNIT_ASSERT_TOKENIZED(lexer, "\"\\\"\"", "[INVALID] STRING_VALUE(\"\\\") EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "\"\"\"\"", "STRING_VALUE(\"\"\"\") EOF");
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(MultiLineString) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "@@@@", "STRING_VALUE(@@@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@ @@@", "STRING_VALUE(@@ @@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@test@@", "STRING_VALUE(@@test@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@line1\nline2@@", "STRING_VALUE(@@line1\nline2@@) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(SingleLineComment) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "--yql", "COMMENT(--yql) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "--  yql ", "COMMENT(--  yql ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "-- yql\nSELECT", "COMMENT(-- yql\n) SELECT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "-- yql --", "COMMENT(-- yql --) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(MultiLineComment) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql */", "COMMENT(/* yql */) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql\n * yql\n */", "COMMENT(/* yql\n * yql\n */) EOF");
    }

} // Y_UNIT_TEST_SUITE(SQLv1Lexer)
