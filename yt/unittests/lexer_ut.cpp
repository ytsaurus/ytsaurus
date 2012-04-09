#include "stdafx.h"

#include <ytlib/ytree/lexer.h>

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/foreach.h>

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

    void TestSpecialValue(const Stroka& input, ETokenType expectedType)
    {
        auto& token = GetToken(input);
        EXPECT_EQ(expectedType, token.GetType());
        EXPECT_EQ(input, token.ToString());
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
    TestConsume('\t');
    CheckState(EState::None);
    TestConsume('\r');
    CheckState(EState::None);
    TestConsume('\n');
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

TEST_F(TLexerTest, Strings)
{
    TestToken("abc_123-%", ETokenType::String, "abc_123-%");
    TestToken("%0-0-0-0", ETokenType::String, "%0-0-0-0"); // guids
    TestToken("_", ETokenType::String, "_");
    TestToken("%", ETokenType::String, "%");

    TestToken("\"abc_123\"", ETokenType::String, "abc_123");
    TestToken("\" abc_123\\t\\\\\\\"\"", ETokenType::String, " abc_123\t\\\"");
    TestToken("\"\\x01\\x02\\x03\\x04\"", ETokenType::String, "\x01\x02\x03\x04");

    TestToken(Stroka("\x01\x00", 2), ETokenType::String, "");
    TestToken("\x01\x08\x01\x02\x03\x04", ETokenType::String, "\x01\x02\x03\x04");
}

TEST_F(TLexerTest, Integers)
{
    TestToken("123", ETokenType::Integer, "123");
    TestToken("0", ETokenType::Integer, "0");
    TestToken("+1", ETokenType::Integer, "1");
    TestToken("-1", ETokenType::Integer, "-1");

    TestToken(Stroka("\x02\x00", 2), ETokenType::Integer, "0");
    TestToken("\x02\x01", ETokenType::Integer, "-1");
    TestToken("\x02\x02", ETokenType::Integer, "1");
    TestToken("\x02\x03", ETokenType::Integer, "-2");
    TestToken("\x02\x04", ETokenType::Integer, "2");
    TestToken("\x02\x80\x80\x80\x02", ETokenType::Integer, ToString(1ull << 21));
}

TEST_F(TLexerTest, Doubles)
{
    const double x = 3.1415926;
    TestDouble("3.1415926", x);
    TestDouble("0.31415926e+1", x);
    TestDouble("31415926e-7", x);
    TestDouble(Stroka('\x03') + Stroka((const char*) &x, sizeof(x)), x);
}

TEST_F(TLexerTest, SpecialValues)
{
    TestSpecialValue(";", ETokenType::Semicolon);
    TestSpecialValue("=", ETokenType::Equals);
    TestSpecialValue("[", ETokenType::LeftBracket);
    TestSpecialValue("]", ETokenType::RightBracket);
    TestSpecialValue("{", ETokenType::LeftBrace);
    TestSpecialValue("}", ETokenType::RightBrace);
    TestSpecialValue("<", ETokenType::LeftAngle);
    TestSpecialValue(">", ETokenType::RightAngle);
    TestSpecialValue("(", ETokenType::LeftParenthesis);
    TestSpecialValue(")", ETokenType::RightParenthesis);
    TestSpecialValue("/", ETokenType::Slash);
    TestSpecialValue("@", ETokenType::At);
    TestSpecialValue("#", ETokenType::Hash);
    TestSpecialValue("!", ETokenType::Bang);
    TestSpecialValue("+", ETokenType::Plus);
    TestSpecialValue("^", ETokenType::Caret);
    TestSpecialValue(":", ETokenType::Colon);
    TestSpecialValue(",", ETokenType::Comma);
}

TEST_F(TLexerTest, IncorrectChars)
{
    TestIncorrectInput("\x01\x03"); // Binary string with negative length

    TestIncorrectInput("1a"); // Alpha after numeric
    TestIncorrectInput("1.1e-1a"); // Alpha after numeric

    // Unknown symbols
    TestIncorrectInput(".");
    TestIncorrectInput("|");
    TestIncorrectInput("\\");
    TestIncorrectInput("?");
    TestIncorrectInput("'");
    TestIncorrectInput("`");
    TestIncorrectInput("$");
    TestIncorrectInput("&");
    TestIncorrectInput("*");
    TestIncorrectInput("~");
}

TEST_F(TLexerTest, IncorrectFinish)
{
    TestIncorrectFinish("\"abc"); // no matching quote
    TestIncorrectFinish("\"abc\\\""); // no matching quote (\" is escaped quote)
    TestIncorrectFinish("\x01"); // binary string without length
    TestIncorrectFinish("\x01\x06YT"); // binary string shorter than the specified length
    TestIncorrectFinish("\x02\x80\x80"); // unfinished varint
    TestIncorrectFinish("\x03\x01\x01\x01\x01\x01\x01\x01"); // binary double too short
    TestIncorrectFinish("-"); // numeric not finished
}

////////////////////////////////////////////////////////////////////////////////
    
} // namespace NYTree
} // namespace NYT
