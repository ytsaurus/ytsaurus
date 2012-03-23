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

    THolder<TLexer> Lexer;
    
    virtual void SetUp()
    {
        Reset();
    }

    void Reset()
    {
        Lexer.Reset(new TLexer());
    }

    void TestConsume(char ch, bool expectedConsumed = true)
    {
        EXPECT_EQ(expectedConsumed, Lexer->Consume(ch));
    }

    void CheckState(EState state)
    {
        EXPECT_EQ(state, Lexer->GetState());
    }

    const TToken& GetToken(const Stroka& input)
    {
        FOREACH (char ch, input) {
            TestConsume(ch);
        }
        Lexer->Finish();
        CheckState(EState::Terminal);
        return Lexer->GetToken();
    }

    void TestToken(const Stroka& input, ETokenType expectedType, const Stroka& expectedValue)
    {
        auto& token = GetToken(input);
        EXPECT_EQ(expectedType, token.GetType());
        EXPECT_EQ(expectedValue, token.ToString());
        Reset();
    }

    void TestDouble(const Stroka& input, double expectedValue)
    {
        auto& token = GetToken(input);
        EXPECT_EQ(ETokenType::Double, token.GetType());
        EXPECT_DOUBLE_EQ(expectedValue, token.GetDoubleValue());
        Reset();
    }

    void TestIncorrectFinish(const Stroka& input)
    {
        FOREACH (char ch, input) {
            TestConsume(ch);
        }
        EXPECT_THROW(Lexer->Finish(), yexception);
        Reset();
    }

    void TestIncorrectInput(const Stroka& input)
    {
        int length = input.length();
        for (int i = 0; i < length - 1; ++i) {
            TestConsume(input[i]);
        }
        EXPECT_THROW(Lexer->Consume(input.back()), yexception);
        Reset();
    }
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
    
    Lexer->Reset();

    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    TestConsume('1');
    CheckState(EState::InProgress);
    TestConsume(';', false);
    CheckState(EState::Terminal);

    Lexer->Reset();

    CheckState(EState::None);
    TestConsume(';');
    CheckState(EState::Terminal);
    
    Lexer->Reset();

    CheckState(EState::None);
    TestConsume('1');
    CheckState(EState::InProgress);
    TestConsume('2');
    CheckState(EState::InProgress);
    Lexer->Finish();
    CheckState(EState::Terminal);
    
    Lexer->Reset();
    
    CheckState(EState::None);
    TestConsume(' ');
    CheckState(EState::None);
    Lexer->Finish();
    CheckState(EState::None);

    Lexer->Reset();

    CheckState(EState::None);
    Lexer->Finish();
    CheckState(EState::None);
    Lexer->Finish();
    CheckState(EState::None);
}

TEST_F(TLexerTest, IncorrectChars)
{
    TestIncorrectInput("\x01\x03"); // Binary string with negative length
    TestIncorrectInput("|");
    TestIncorrectInput("&");
    TestIncorrectInput("*");
}

TEST_F(TLexerTest, IncorrectFinish)
{
    TestIncorrectFinish("\"abc"); // no matching quote
    TestIncorrectFinish("\"abc\\\""); // no matching quote (\" is escaped quote)
    TestIncorrectFinish("\x01"); // binary string without length
    TestIncorrectFinish("\x01\x06YT"); // binary string shorter than the specified length
    TestIncorrectFinish("\x02\x80\x80"); // unfinished varint
    TestIncorrectFinish("\x03\x01\x01\x01\x01\x01\x01\x01"); // binary double too short
}

////////////////////////////////////////////////////////////////////////////////
    
} // namespace NYTree
} // namespace NYT
