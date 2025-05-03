#include "token.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

using namespace NSQLComplete;

Y_UNIT_TEST_SUITE(CursorTokenContextTests) {

    NSQLTranslation::ILexer::TPtr MakeLexer() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        return NSQLTranslationV1::MakeLexer(
            lexers, /* ansi = */ false, /* antlr4 = */ true,
            NSQLTranslationV1::ELexerFlavor::Pure);
    }

    TCursorTokenContext Context(TString input) {
        auto lexer = MakeLexer();
        TCursorTokenContext context;
        UNIT_ASSERT(GetCursorTokenContext(lexer, SharpedInput(input), context));
        return context;
    }

    Y_UNIT_TEST(Empty) {
        auto context = Context("");
        auto enclosing = context.Enclosing();
        UNIT_ASSERT_VALUES_EQUAL(context.Cursor.PrevTokenIndex, 0);
        UNIT_ASSERT_VALUES_EQUAL(context.Cursor.NextTokenIndex, 0);
        UNIT_ASSERT_VALUES_EQUAL(context.Cursor.Position, 0);
        UNIT_ASSERT(enclosing.Empty());
    }

} // Y_UNIT_TEST_SUITE(CursorTokenContextTests)
