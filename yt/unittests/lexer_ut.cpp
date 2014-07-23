#include "stdafx.h"
#include "framework.h"

#include <core/yson/lexer.h>

#include <core/misc/nullable.h>

namespace NYT {
namespace NYson {
namespace {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TStatelessLexerTest
    : public ::testing::Test
{
public:
    std::unique_ptr<TStatelessLexer> Lexer;

    virtual void SetUp()
    {
        Reset();
    }

    void Reset()
    {
        Lexer.reset(new TStatelessLexer());
    }

    void TestConsume(const TStringBuf& input)
    {
        TToken token;
        Lexer->GetToken(input, &token);
    }

    TToken GetToken(const TStringBuf& input)
    {
        TToken token;
        Lexer->GetToken(input, &token);
        return token;
    }

    void TestToken(const TStringBuf& input, ETokenType expectedType, const Stroka& expectedValue)
    {
        auto token = GetToken(input);
        EXPECT_EQ(expectedType, token.GetType());
        EXPECT_EQ(expectedValue, ToString(token));
        Reset();
    }

    void TestDouble(const TStringBuf& input, double expectedValue)
    {
        auto token = GetToken(input);
        EXPECT_EQ(ETokenType::Double, token.GetType());
        EXPECT_DOUBLE_EQ(expectedValue, token.GetDoubleValue());
        Reset();
    }

    void TestSpecialValue(const TStringBuf& input, ETokenType expectedType)
    {
        auto token = GetToken(input);
        EXPECT_EQ(expectedType, token.GetType());
        EXPECT_EQ(input, ToString(token));
        Reset();
    }

    void TestIncorrectFinish(const TStringBuf& input)
    {
        EXPECT_THROW(TestConsume(input), std::exception);
        Reset();
    }

    void TestIncorrectInput(const TStringBuf& input)
    {
        EXPECT_THROW(TestConsume(input), std::exception);
        Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStatelessLexerTest, StringValues)
{
    TestToken("abc_123.-%", ETokenType::String, "abc_123.-%");
    TestToken("%0-0-0-0", ETokenType::String, "%0-0-0-0"); // guids
    TestToken("_", ETokenType::String, "_");
    TestToken("%", ETokenType::String, "%");

    TestToken("\"abc_123\"", ETokenType::String, "abc_123");
    TestToken("\" abc_123\\t\\\\\\\"\"", ETokenType::String, " abc_123\t\\\"");
    TestToken("\"\\x01\\x02\\x03\\x04\"", ETokenType::String, "\x01\x02\x03\x04");

    TestToken(Stroka("\x01\x00", 2), ETokenType::String, "");
    TestToken("\x01\x08\x01\x02\x03\x04", ETokenType::String, "\x01\x02\x03\x04");
}

TEST_F(TStatelessLexerTest, Int64Values)
{
    TestToken("123", ETokenType::Int64, "123");
    TestToken("0", ETokenType::Int64, "0");
    TestToken("+1", ETokenType::Int64, "1");
    TestToken("-1", ETokenType::Int64, "-1");

    TestToken(Stroka("\x02\x00", 2), ETokenType::Int64, "0");
    TestToken("\x02\x01", ETokenType::Int64, "-1");
    TestToken("\x02\x02", ETokenType::Int64, "1");
    TestToken("\x02\x03", ETokenType::Int64, "-2");
    TestToken("\x02\x04", ETokenType::Int64, "2");
    TestToken("\x02\x80\x80\x80\x02", ETokenType::Int64, ToString(1ull << 21));
}

TEST_F(TStatelessLexerTest, DoubleValues)
{
    const double x = 3.1415926;
    TestDouble("3.1415926", x);
    TestDouble("0.31415926e+1", x);
    TestDouble("31415926e-7", x);
    TestDouble(Stroka('\x03') + Stroka((const char*) &x, sizeof(x)), x);
}

TEST_F(TStatelessLexerTest, SpecialValues)
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
    TestSpecialValue("#", ETokenType::Hash);
    TestSpecialValue("+", ETokenType::Plus);
    TestSpecialValue(":", ETokenType::Colon);
    TestSpecialValue(",", ETokenType::Comma);
}

TEST_F(TStatelessLexerTest, IncorrectChars)
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
}

TEST_F(TStatelessLexerTest, IncorrectFinish)
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

} // namespace
} // namespace NYTree
} // namespace NYT
