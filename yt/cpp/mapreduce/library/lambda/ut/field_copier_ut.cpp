#include <yt/cpp/mapreduce/library/lambda/field_copier.h>

#include <yt/cpp/mapreduce/library/lambda/ut/proto/test_message.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

const TNode TestNode = TNode()
    ("Str",    "Foo")
    ("Int64",  -142)
    ("UInt64", 142u)
    ("Int32",  -42)
    ("UInt32", 42u)
    ("Float",  42.)
    ("Double", 142.)
    ("Enum",   "CONST2");

static void CheckResultMsg(const TTestMessage& result)
{
    EXPECT_EQ(result.GetStr(), "Foo");
    EXPECT_EQ(result.GetInt64(), -142);
    EXPECT_EQ(result.GetUInt64(), 142u);
    EXPECT_EQ(result.GetInt32(), -42);
    EXPECT_EQ(result.GetUInt32(), 42u);
    EXPECT_EQ(result.GetFloat(), 42.);
    EXPECT_EQ(result.GetDouble(), 142.);
    EXPECT_EQ(result.GetEnum(), CONST2);
}

TEST(TFieldCopierTest, Node2Node)
{
    TFieldCopier<TNode, TNode> copier(
        {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
    TNode result = TNode()("Str", "Bar");

    copier(TestNode, result);
    EXPECT_EQ(TestNode, result);
}

TEST(TFieldCopierTest, Proto2Node)
{
    TFieldCopier<TTestMessageDefv, TNode> copier(
        {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
    TNode result = TNode()("Str", "Bar");
    TTestMessageDefv filledMessage;

    copier(filledMessage, result);
    EXPECT_EQ(TestNode, result);
}

TEST(TFieldCopierTest, Node2Proto)
{
    TFieldCopier<TNode, TTestMessage> copier(
        {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
    TTestMessage result;

    copier(TestNode, result);
    CheckResultMsg(result);
}

TEST(TFieldCopierTest, Proto2Proto)
{
    TFieldCopier<TTestMessageDefv, TTestMessage> copier(
        {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
    TTestMessageDefv filledMessage;
    TTestMessage result;

    copier(filledMessage, result);
    CheckResultMsg(result);
}

TEST(TFieldCopierTest, WrongKey)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (TFieldCopier<TTestMessage, TTestMessage2>("key")),
        yexception,
        "does not have field");
}

TEST(TFieldCopierTest, WrongType)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (TFieldCopier<TTestMessage2, TTestMessage3>("val")),
        yexception,
        "have different types");
}

TEST(TFieldCopierTest, WrongEnum)
{
    TFieldCopier<TNode, TTestMessage> copier("Enum");
    TNode testNode = TestNode;
    testNode["Enum"] = "Bar";
    TTestMessage result;

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        copier(testNode, result),
        yexception,
        "Failed to parse");

    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (TFieldCopier<TTestMessage, TTestMessage2>("Enum")),
        yexception,
        "different enum types");
}

TEST(TFieldCopierTest, Unsupported)
{
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (TFieldCopier<TTestMessage2, TBadTTestMessage1>("key")),
        yexception,
        "is repeated");
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        (TFieldCopier<TBadTTestMessage2, TBadTTestMessage2>("key")),
        yexception,
        "is of Message type");
}
