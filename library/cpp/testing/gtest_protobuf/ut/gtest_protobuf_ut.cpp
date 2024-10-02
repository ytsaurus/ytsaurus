#include <library/cpp/testing/gtest_protobuf/matcher.h>
#include <library/cpp/testing/gtest_protobuf/ut/data.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace {
    template<typename T, typename M>
    std::pair<bool, std::string> Match(const T& t, M&& m) {
        testing::StringMatchResultListener listener;
        auto matches = testing::SafeMatcherCast<const T&>(std::forward<M>(m)).MatchAndExplain(t, &listener);
        return {matches, listener.str()};
    }
}

TEST(EqualsProtoTest, ScopeOptions) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;

    expected.SetField1(1.0);
    actual.SetField1(1.0);
    actual.SetField2("abc");
    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(actual)));

    NGTest::TProtoCompareOptions options;
    options.Scope = NGTest::TProtoCompareOptions::EScope::PARTIAL;
    EXPECT_THAT(expected, NGTest::EqualsProto(std::ref(actual), options));
}

TEST(EqualsProtoTest, RepeatedFieldComparisonOptions) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;

    expected.AddField3(1);
    expected.AddField3(2);
    actual.AddField3(2);
    actual.AddField3(1);

    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(actual)));

    NGTest::TProtoCompareOptions options;
    options.RepeatedFieldComparison = NGTest::TProtoCompareOptions::ERepeatedFieldComparison::AS_SET;
    EXPECT_THAT(expected, NGTest::EqualsProto(std::ref(actual), options));
}

TEST(EqualsProtoTest, FieldComparisonOptions) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;

    expected.SetField1(0.0);
    expected.SetField2("");
    EXPECT_THAT(expected, NGTest::EqualsProto(actual));

    NGTest::TProtoCompareOptions options;
    options.MessageFieldComparison = NGTest::TProtoCompareOptions::EMessageFieldComparison::EQUAL;
    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(std::ref(actual), options)));
}

TEST(EqualsProtoTest, FloatComparisonOptions) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;

    expected.SetField1(0.00000000011);
    actual.SetField1(0.00000000012);

    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(std::ref(actual))));

    NGTest::TProtoCompareOptions options;
    options.FloatComparison = NGTest::TProtoCompareOptions::EFloatComparison::APPROXIMATE;
    EXPECT_THAT(expected, NGTest::EqualsProto(std::ref(actual), options));

    expected.SetField1(0.011);
    actual.SetField1(0.012);

    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(std::ref(actual))));
}

TEST(EqualsProtoTest, TreatNanAsEqual) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;

    expected.SetField1(NAN);
    actual.SetField1(NAN);

    EXPECT_THAT(expected, NGTest::EqualsProto(std::ref(actual)));

    NGTest::TProtoCompareOptions options;
    options.TreatNanAsEqual = false;
    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(std::ref(actual), options)));

    expected.SetField1(0.01);
    EXPECT_THAT(expected, ::testing::Not(NGTest::EqualsProto(std::ref(actual))));
}

TEST(EqualsProtoTest, MatchAndExplain) {
    NGTest::TTestMessage expected;
    NGTest::TTestMessage actual;
    {
        auto [matched, explanation] = Match(expected, NGTest::EqualsProto(std::ref(actual)));
        EXPECT_TRUE(matched);
        EXPECT_EQ(explanation, "is equal.");
    }

    expected.SetField2("test");
    {
        auto [matched, explanation] = Match(expected, NGTest::EqualsProto(std::ref(actual)));
        EXPECT_FALSE(matched);
        EXPECT_THAT(explanation, ::testing::HasSubstr("Field2: \"test\" -> \"\""));
    }
}

TEST(EqualsProtoTest, DescribeTo) {
    NGTest::TTestMessage message;
    message.SetField2("text");


    auto matcher = ::testing::SafeMatcherCast<const NGTest::TTestMessage&>(NGTest::EqualsProto(std::ref(message)));
    {
        ::std::stringstream ss;
        matcher.DescribeTo(&ss);
        EXPECT_THAT(ss.str(), ::testing::HasSubstr("message is equal to <Field2: \"text\">"));
    }

    {
        ::std::stringstream ss;
        matcher.DescribeNegationTo(&ss);
        EXPECT_THAT(ss.str(), ::testing::HasSubstr("message isn't equal to <Field2: \"text\">"));
    }
}

