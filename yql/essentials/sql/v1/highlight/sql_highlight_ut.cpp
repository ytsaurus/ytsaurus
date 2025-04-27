#include "sql_highlight.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

using namespace NSQLHighlight;

Y_UNIT_TEST_SUITE(SqlHighlightTests) {

    Y_UNIT_TEST(Example) {
        const auto human = MakeHighlighting(NSQLReflect::LoadLexerGrammar());

        for (const auto& unit : human.Units) {
            Cout << "Unit " << unit.Kind << Endl;
            for (const auto& pattern : unit.Patterns) {
                Cout << "- " << pattern.BodyRe;
                if (!pattern.AfterRe.empty()) {
                    Cout << "(?=" << pattern.AfterRe + ")";
                }
                Cout << Endl;
            }
        }
    }

} // Y_UNIT_TEST_SUITE(SqlHighlightTests)
