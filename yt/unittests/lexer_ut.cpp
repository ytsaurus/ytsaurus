#include "stdafx.h"

#include <ytlib/ytree/lexer.h>

#include <ytlib/misc/nullable.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexerTest: public ::testing::Test
{
public:
    typedef TLexer::EState EState;

    TLexer Lexer;
    Stroka Input;

    void TestConsume(char ch, bool expectedConsumed = true)
    {
        EXPECT_EQ(expectedConsumed, Lexer.Consume(ch));
    }

    void CheckState(EState state)
    {
        EXPECT_EQ(state, Lexer.GetState());
    }

//    void CheckToken(const Stroka& input, )
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TLexerTest, States)
{
    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    TestConsume('1');
    CheckState(EState::InProgress);
    TestConsume('2');
    CheckState(EState::InProgress);
    TestConsume(' ', false);
    CheckState(EState::Terminal);
    
    Lexer.Reset();

    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    TestConsume('1');
    CheckState(EState::InProgress);
    TestConsume(';', false);
    CheckState(EState::Terminal);

    Lexer.Reset();

    CheckState(EState::None);
    TestConsume(';');
    CheckState(EState::Terminal);
    
    Lexer.Reset();

    CheckState(EState::None);
    TestConsume('1');
    CheckState(EState::InProgress);
    TestConsume('2');
    CheckState(EState::InProgress);
    Lexer.Finish();
    CheckState(EState::Terminal);
    
    Lexer.Reset();
    
    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    Lexer.Finish();
    CheckState(EState::None);

    Lexer.Reset();

    CheckState(EState::None);
    Lexer.Finish();
    CheckState(EState::None);
    Lexer.Finish();
    CheckState(EState::None);
}

////////////////////////////////////////////////////////////////////////////////
    
} // namespace NYTree
} // namespace NYT
