#include "sql_highlight.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

using namespace NSQLHighlight;

Y_UNIT_TEST_SUITE(SqlHighlightTests) {

    Y_UNIT_TEST(Example) {
        const auto human = MakeHighlighting(NSQLReflect::LoadLexerGrammar());

        for (const auto& unit : human.Units) {
            Cout << "Unit " << unit.Kind << Endl;
            for (const auto& token : unit.Tokens) {
                Cout << "- " << JoinSeq(" ", token) << Endl;
            }
        }

        for (const auto& [name, content] : human.References) {
            Cout << "Ref " << name << ": " << content << Endl;
        }
    }

} // Y_UNIT_TEST_SUITE(SqlHighlightTests)
