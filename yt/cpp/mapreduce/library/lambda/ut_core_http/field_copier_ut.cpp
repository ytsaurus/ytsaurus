#include <yt/cpp/mapreduce/library/lambda/field_copier.h>

#include <yt/cpp/mapreduce/library/lambda/ut_core_http/test-message.pb.h>
#include <library/cpp/testing/unittest/registar.h>

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

static void CheckResultMsg(const TTestMessage& result) {
    UNIT_ASSERT_EQUAL(result.GetStr(), "Foo");
    UNIT_ASSERT_EQUAL(result.GetInt64(), -142);
    UNIT_ASSERT_EQUAL(result.GetUInt64(), 142u);
    UNIT_ASSERT_EQUAL(result.GetInt32(), -42);
    UNIT_ASSERT_EQUAL(result.GetUInt32(), 42u);
    UNIT_ASSERT_EQUAL(result.GetFloat(), 42.);
    UNIT_ASSERT_EQUAL(result.GetDouble(), 142.);
    UNIT_ASSERT_EQUAL(result.GetEnum(), CONST2);
}

Y_UNIT_TEST_SUITE(FieldCopier) {
    Y_UNIT_TEST(Node2Node) {
        TFieldCopier<TNode, TNode> copier(
            {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
        TNode result = TNode()("Str", "Bar");

        copier(TestNode, result);
        UNIT_ASSERT_EQUAL(TestNode, result);
    }

    Y_UNIT_TEST(Proto2Node) {
        TFieldCopier<TTestMessageDefv, TNode> copier(
            {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
        TNode result = TNode()("Str", "Bar");
        TTestMessageDefv filledMessage;

        copier(filledMessage, result);
        UNIT_ASSERT_EQUAL(TestNode, result);
    }

    Y_UNIT_TEST(Node2Proto) {
        TFieldCopier<TNode, TTestMessage> copier(
            {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
        TTestMessage result;

        copier(TestNode, result);
        CheckResultMsg(result);
    }

    Y_UNIT_TEST(Proto2Proto) {
        TFieldCopier<TTestMessageDefv, TTestMessage> copier(
            {"Str", "Int64", "UInt64", "Int32", "UInt32", "Float", "Double", "Enum"});
        TTestMessageDefv filledMessage;
        TTestMessage result;

        copier(filledMessage, result);
        CheckResultMsg(result);
    }

    Y_UNIT_TEST(WrongKey) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            (TFieldCopier<TTestMessage, TTestMessage2>("key")),
            yexception,
            "does not have field");
    }

    Y_UNIT_TEST(WrongType) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            (TFieldCopier<TTestMessage2, TTestMessage3>("val")),
            yexception,
            "have different types");
    }

    Y_UNIT_TEST(WrongEnum) {
        TFieldCopier<TNode, TTestMessage> copier("Enum");
        TNode testNode = TestNode;
        testNode["Enum"] = "Bar";
        TTestMessage result;

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            copier(testNode, result),
            yexception,
            "Failed to parse");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            (TFieldCopier<TTestMessage, TTestMessage2>("Enum")),
            yexception,
            "different enum types");
    }

    Y_UNIT_TEST(Unsupported) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            (TFieldCopier<TTestMessage2, TBadTTestMessage1>("key")),
            yexception,
            "is repeated");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            (TFieldCopier<TBadTTestMessage2, TBadTTestMessage2>("key")),
            yexception,
            "is of Message type");
    }
}
