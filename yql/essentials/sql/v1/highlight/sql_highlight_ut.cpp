#include "sql_highlight.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

using namespace NSQLHighlight;

Y_UNIT_TEST_SUITE(SqlHighlightTests) {

    Y_UNIT_TEST(Example) {
        auto highlighting = MakeHighlighting(NSQLReflect::LoadLexerGrammar());

        for (const auto& unit : highlighting.Units) {
            Cout << "Unit " << unit.Kind << Endl;
            for (const auto& pattern : unit.Patterns) {
                Cout << "- " << pattern.Body;
                if (!pattern.After.empty()) {
                    Cout << "(?=" << pattern.After + ")";
                }
                Cout << Endl;
            }
        }
    }

} // Y_UNIT_TEST_SUITE(SqlHighlightTests)
