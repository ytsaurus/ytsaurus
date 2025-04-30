#include "sql_highlighter.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLHighlight;

struct TTest {
    struct TCase {
        TString Input;
        TString Expected;
    };

    TString Name;
    TVector<TCase> Cases;
};

TVector<TTest> LoadTestSuite() {
    TString text;
    Y_ENSURE(NResource::FindExact("suite.json", &text));
    auto json = NJson::ReadJsonFastTree(text).GetMapSafe();

    TVector<TTest> tests;
    for (auto& [k, v] : json) {
        TVector<TTest::TCase> cases;
        for (auto& c : v.GetArraySafe()) {
            cases.emplace_back(
                std::move(c[0].GetStringSafe()),
                std::move(c[1].GetStringSafe()));
        }
        tests.emplace_back(std::move(k), std::move(cases));
    }
    return tests;
}

char ToChar(EUnitKind kind) {
    switch (kind) {
        case EUnitKind::Keyword:
            return 'K';
        case EUnitKind::Punctuation:
            return 'P';
        case EUnitKind::Identifier:
            return 'I';
        case EUnitKind::QuotedIdentifier:
            return 'Q';
        case EUnitKind::BindParamterIdentifier:
            return 'B';
        case EUnitKind::TypeIdentifier:
            return 'T';
        case EUnitKind::FunctionIdentifier:
            return 'F';
        case EUnitKind::Literal:
            return 'L';
        case EUnitKind::StringLiteral:
            return 'S';
        case EUnitKind::Comment:
            return 'C';
        case EUnitKind::Whitespace:
            return '_';
        case EUnitKind::Error:
            return 'E';
    }
}

TString ToMask(const TVector<TToken>& tokens) {
    TString s;
    for (const auto& t : tokens) {
        s += TString(t.Length, ToChar(t.Kind));
    }
    return s;
}

TString Mask(IHighlighter::TPtr& h, TStringBuf text) {
    return ToMask(Tokenize(*h, text));
}

Y_UNIT_TEST_SUITE(SqlHighlighterTests) {

    Y_UNIT_TEST(Suite) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        size_t count = 0;
        for (const auto& test : LoadTestSuite()) {
            Cerr << "Testing '" << test.Name << "'..." << Endl;
            for (size_t i = 0; i < test.Cases.size(); ++i) {
                const auto& check = test.Cases[i];
                UNIT_ASSERT_VALUES_EQUAL_C(
                    Mask(h, check.Input),
                    check.Expected,
                    test.Name << " #" << i
                              << ": Input = '" << check.Input << "'");
                count += 1;
            }
        }
        Cerr << "Test Cases Executed: " << count << Endl;
    }

} // Y_UNIT_TEST_SUITE(SqlHighlighterTests)
