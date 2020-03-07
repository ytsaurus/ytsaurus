#include <yt/core/test_framework/framework.h>
#include "ql_helpers.h"

#include <yt/ytlib/query_client/query_helpers.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/coordination_helpers.h>

#include <util/random/fast.h>

// Tests:
// TKeyRangeTest
// TRefineKeyRangeTest

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyRangeTest, Unite)
{
    auto k1 = YsonToKey("1"); auto k2 = YsonToKey("2");
    auto k3 = YsonToKey("3"); auto k4 = YsonToKey("4");
    auto mp = [] (const TOwningKey& a, const TOwningKey& b) {
        return std::make_pair(a, b);
    };

    EXPECT_EQ(mp(k1, k4), Unite(mp(k1, k2), mp(k3, k4)));
    EXPECT_EQ(mp(k1, k4), Unite(mp(k1, k3), mp(k2, k4)));
    EXPECT_EQ(mp(k1, k4), Unite(mp(k1, k4), mp(k2, k3)));
    EXPECT_EQ(mp(k1, k4), Unite(mp(k2, k3), mp(k1, k4)));
    EXPECT_EQ(mp(k1, k4), Unite(mp(k2, k4), mp(k1, k3)));
    EXPECT_EQ(mp(k1, k4), Unite(mp(k3, k4), mp(k1, k2)));
}

TEST(TKeyRangeTest, Intersect)
{
    auto k1 = YsonToKey("1"); auto k2 = YsonToKey("2");
    auto k3 = YsonToKey("3"); auto k4 = YsonToKey("4");
    auto mp = [] (const TOwningKey& a, const TOwningKey& b) {
        return std::make_pair(a, b);
    };

    EXPECT_TRUE(IsEmpty(Intersect(mp(k1, k2), mp(k3, k4))));
    EXPECT_EQ(mp(k2, k3), Intersect(mp(k1, k3), mp(k2, k4)));
    EXPECT_EQ(mp(k2, k3), Intersect(mp(k1, k4), mp(k2, k3)));
    EXPECT_EQ(mp(k2, k3), Intersect(mp(k2, k3), mp(k1, k4)));
    EXPECT_EQ(mp(k2, k3), Intersect(mp(k2, k4), mp(k1, k3)));
    EXPECT_TRUE(IsEmpty(Intersect(mp(k3, k4), mp(k1, k2))));

    EXPECT_EQ(mp(k1, k2), Intersect(mp(k1, k2), mp(k1, k3)));
    EXPECT_EQ(mp(k1, k2), Intersect(mp(k1, k3), mp(k1, k2)));

    EXPECT_EQ(mp(k3, k4), Intersect(mp(k3, k4), mp(k2, k4)));
    EXPECT_EQ(mp(k3, k4), Intersect(mp(k2, k4), mp(k3, k4)));

    EXPECT_EQ(mp(k1, k4), Intersect(mp(k1, k4), mp(k1, k4)));
}

TEST(TKeyRangeTest, IsEmpty)
{
    auto k1 = YsonToKey("1"); auto k2 = YsonToKey("2");
    auto mp = [] (const TOwningKey& a, const TOwningKey& b) {
        return std::make_pair(a, b);
    };

    EXPECT_TRUE(IsEmpty(mp(k1, k1)));
    EXPECT_TRUE(IsEmpty(mp(k2, k2)));

    EXPECT_TRUE(IsEmpty(mp(k2, k1)));
    EXPECT_FALSE(IsEmpty(mp(k1, k2)));

    EXPECT_TRUE(IsEmpty(mp(YsonToKey("0;0;1"), YsonToKey("0;0;0"))));
    EXPECT_FALSE(IsEmpty(mp(YsonToKey("0;0;0"), YsonToKey("0;0;1"))));
}

////////////////////////////////////////////////////////////////////////////////
// Refinement tests.

TKeyRange RefineKeyRange(
    const TKeyColumns& keyColumns,
    const TKeyRange& keyRange,
    TConstExpressionPtr predicate)
{
    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        TRowRange(keyRange.first, keyRange.second),
        keyTrie,
        rowBuffer);

    if (result.empty()) {
        return std::make_pair(EmptyKey(), EmptyKey());
    } else if (result.size() == 1) {
        return TKeyRange(TOwningKey(result[0].first), TOwningKey(result[0].second));
    } else {
        return keyRange;
    }
}

struct TRefineKeyRangeTestCase
{
    const char* InitialLeftBoundAsYson;
    const char* InitialRightBoundAsYson;

    const char* ConstraintColumnName;
    EBinaryOp ConstraintOpcode;
    i64 ConstraintValue;

    bool ResultIsEmpty;
    const char* ResultingLeftBoundAsYson;
    const char* ResultingRightBoundAsYson;

    TOwningKey GetInitialLeftBound() const
    {
        return YsonToKey(InitialLeftBoundAsYson);
    }

    TOwningKey GetInitialRightBound() const
    {
        return YsonToKey(InitialRightBoundAsYson);
    }

    TOwningKey GetResultingLeftBound() const
    {
        return YsonToKey(ResultingLeftBoundAsYson);
    }

    TOwningKey GetResultingRightBound() const
    {
        return YsonToKey(ResultingRightBoundAsYson);
    }
};

class TRefineKeyRangeTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TRefineKeyRangeTestCase>
{
protected:
    virtual void SetUp() override
    { }

    void ExpectIsEmpty(const TKeyRange& keyRange)
    {
        EXPECT_TRUE(IsEmpty(keyRange))
            << "Left bound: " << ::testing::PrintToString(keyRange.first) << "; "
            << "Right bound: " << ::testing::PrintToString(keyRange.second);
    }
};

void PrintTo(const TRefineKeyRangeTestCase& testCase, ::std::ostream* os)
{
    *os
        << "{ "
        << "P: "
        << testCase.ConstraintColumnName << " "
        << GetBinaryOpcodeLexeme(testCase.ConstraintOpcode) << " "
        << testCase.ConstraintValue << ", "
        << "E: "
        << (testCase.ResultIsEmpty ? "True" : "False") << ", "
        << "L: "
        << ::testing::PrintToString(testCase.GetResultingLeftBound()) << ", "
        << "R: "
        << ::testing::PrintToString(testCase.GetResultingRightBound())
        << " }";
}

TEST_P(TRefineKeyRangeTest, Basic)
{
    auto testCase = GetParam();

    auto expr = Make<TBinaryOpExpression>(testCase.ConstraintOpcode,
        Make<TReferenceExpression>(testCase.ConstraintColumnName),
        Make<TLiteralExpression>(MakeInt64(testCase.ConstraintValue)));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(
            testCase.GetInitialLeftBound(),
            testCase.GetInitialRightBound()),
        expr);

    if (testCase.ResultIsEmpty) {
        ExpectIsEmpty(result);
    } else {
        EXPECT_EQ(testCase.GetResultingLeftBound(), result.first);
        EXPECT_EQ(testCase.GetResultingRightBound(), result.second);
    }
}

TEST_P(TRefineKeyRangeTest, BasicReversed)
{
    auto testCase = GetParam();

    auto expr = Make<TBinaryOpExpression>(GetReversedBinaryOpcode(testCase.ConstraintOpcode),
        Make<TLiteralExpression>(MakeInt64(testCase.ConstraintValue)),
        Make<TReferenceExpression>(testCase.ConstraintColumnName));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(
            testCase.GetInitialLeftBound(),
            testCase.GetInitialRightBound()),
        expr);

    if (testCase.ResultIsEmpty) {
        ExpectIsEmpty(result);
    } else {
        EXPECT_EQ(testCase.GetResultingLeftBound(), result.first);
        EXPECT_EQ(testCase.GetResultingRightBound(), result.second);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Here is a guideline on how to read this cases table.
//
// Basically, initial key range is specified in the first line
// (e. g. from `[0;0;0]` to `[100;100;100]`) and the constraint is on the second
// line (e. g. `k = 50`). Then there is a flag whether result is empty or not
// and also resulting boundaries.
//
// Keep in mind that there are three columns in schema (`k`, `l` and `m`).
//
// TODO(sandello): Plug in an expression parser here.
////////////////////////////////////////////////////////////////////////////////

// Equal, First component.
TRefineKeyRangeTestCase refineCasesForEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 50,
        false, ("50"), ("50;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 1,
        false, ("1;1;1"), ("1;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 99,
        false, ("99"), ("99;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 100,
        false, ("100"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    EqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForEqualOpcodeInFirstComponent));

// NotEqual, First component.
TRefineKeyRangeTestCase refineCasesForNotEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 50,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 1,
        false, ("1;" _MAX_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 100,
        false, ("1;1;1"), ("100;")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 200,
        false, ("1;1;1"), ("100;100;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    NotEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForNotEqualOpcodeInFirstComponent));

// Less, First component.
TRefineKeyRangeTestCase refineCasesForLessOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 50,
        false, ("1;1;1"), ("50")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 1,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 100,
        false, ("1;1;1"), ("100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 200,
        false, ("1;1;1"), ("100;100;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    LessInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOpcodeInFirstComponent));

// LessOrEqual, First component.
TRefineKeyRangeTestCase refineCasesForLessOrEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 50,
        false, ("1;1;1"), ("50;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 1,
        false, ("1;1;1"), ("1;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 99,
        false, ("1;1;1"), ("99;" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 100,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 200,
        false, ("1;1;1"), ("100;100;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    LessOrEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOrEqualOpcodeInFirstComponent));

// Greater, First component.
TRefineKeyRangeTestCase refineCasesForGreaterOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 50,
        false, ("50;" _MAX_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 0,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 1,
        false, ("1;" _MAX_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 100,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    GreaterInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOpcodeInFirstComponent));

// GreaterOrEqual, First component.
TRefineKeyRangeTestCase refineCasesForGreaterOrEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 50,
        false, ("50"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 1,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 100,
        false, ("100"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    GreaterOrEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOrEqualOpcodeInFirstComponent));

////////////////////////////////////////////////////////////////////////////////

// Equal, Last component.
TRefineKeyRangeTestCase refineCasesForEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 50,
        false, ("1;1;50"), ("1;1;50;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 1,
        false, ("1;1;1"), ("1;1;1;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 99,
        false, ("1;1;99"), ("1;1;99;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 100,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    EqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForEqualOpcodeInLastComponent));

// NotEqual, Last component.
TRefineKeyRangeTestCase refineCasesForNotEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::NotEqual, 50,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::NotEqual, 1,
        false, ("1;1;1;" _MAX_), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::NotEqual, 100,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::NotEqual, 200,
        false, ("1;1;1"), ("1;1;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    NotEqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForNotEqualOpcodeInLastComponent));

// Less, Last component.
TRefineKeyRangeTestCase refineCasesForLessOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Less, 50,
        false, ("1;1;1"), ("1;1;50")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Less, 1,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Less, 100,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Less, 200,
        false, ("1;1;1"), ("1;1;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    LessInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOpcodeInLastComponent));

// LessOrEqual, Last component.
TRefineKeyRangeTestCase refineCasesForLessOrEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 50,
        false, ("1;1;1"), ("1;1;50;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 1,
        false, ("1;1;1"), ("1;1;1;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 99,
        false, ("1;1;1"), ("1;1;99;" _MAX_)
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 100,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 200,
        false, ("1;1;1"), ("1;1;100")
    },
};
INSTANTIATE_TEST_SUITE_P(
    LessOrEqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOrEqualOpcodeInLastComponent));

// Greater, Last component.
TRefineKeyRangeTestCase refineCasesForGreaterOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 50,
        false, ("1;1;50;" _MAX_), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 0,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 1,
        false, ("1;1;1;" _MAX_), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 100,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    GreaterInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOpcodeInLastComponent));

// GreaterOrEqual, Last component.
TRefineKeyRangeTestCase refineCasesForGreaterOrEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::GreaterOrEqual, 50,
        false, ("1;1;50"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::GreaterOrEqual, 1,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::GreaterOrEqual, 100,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::GreaterOrEqual, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_SUITE_P(
    GreaterOrEqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOrEqualOpcodeInLastComponent));

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRefineKeyRangeTest, Empty)
{
    auto expr = PrepareExpression("false", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1"), YsonToKey("100")),
        expr);

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, ContradictiveConjuncts)
{
    auto expr = PrepareExpression("k >= 90 and k < 10", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, Lookup1)
{
    auto expr = PrepareExpression("k = 50 and l = 50", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;50"), result.first);
    EXPECT_EQ(YsonToKey("50;50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, Lookup2)
{
    auto expr = PrepareExpression("k = 50 and l = 50 and m = 50", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;50;50"), result.first);
    EXPECT_EQ(YsonToKey("50;50;50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, Range1)
{
    auto expr = PrepareExpression("k > 0 and k < 100", GetSampleTableSchema());

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    auto result = RefineKeyRange(
        keyColumns,
        std::make_pair(YsonToKey(""), YsonToKey("1000000000")),
        expr);

    EXPECT_EQ(YsonToKey("0;" _MAX_), result.first);
    EXPECT_EQ(YsonToKey("100"), result.second);
}

TEST_F(TRefineKeyRangeTest, NegativeRange1)
{
    auto expr = PrepareExpression("k > -100 and (k) <= -(-1)", GetSampleTableSchema());

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    auto result = RefineKeyRange(
        keyColumns,
        std::make_pair(YsonToKey(""), YsonToKey("1000000000")),
        expr);

    EXPECT_EQ(YsonToKey("-100;" _MAX_), result.first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts1)
{
    auto expr = PrepareExpression("k >= 10 and k < 90", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("10"), result.first);
    EXPECT_EQ(YsonToKey("90"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts2)
{
    auto expr = PrepareExpression(
        "k = 50 and l >= 10 and l < 90 and m = 50",
        GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;10"), result.first);
    EXPECT_EQ(YsonToKey("50;90"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts3)
{
    auto expr = PrepareExpression("k = 50 and m = 50", GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50"), result.first);
    EXPECT_EQ(YsonToKey("50;" _MAX_), result.second);
}


TMutableRowRanges GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    TKeyTriePtr trie,
    TRowBufferPtr rowBuffer)
{
    return GetRangesFromTrieWithinRange(
        TRowRange(keyRange.first, keyRange.second),
        trie,
        rowBuffer);
}

TEST_F(TRefineKeyRangeTest, EmptyKeyTrie)
{
    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey(_MIN_), YsonToKey(_MAX_)),
        TKeyTrie::Empty(),
        rowBuffer);

    EXPECT_EQ(0, result.size());
}

TEST_F(TRefineKeyRangeTest, MultipleDisjuncts)
{
    auto expr = PrepareExpression(
        "k = 50 and m = 50 or k = 75 and m = 50",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        GetSampleKeyColumns(),
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("50"), result[0].first);
    EXPECT_EQ(YsonToKey("50;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("75"), result[1].first);
    EXPECT_EQ(YsonToKey("75;" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, NotEqualToMultipleRanges)
{
    auto expr = PrepareExpression(
        "(k = 50 and l != 50) and (l > 40 and l < 60)",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        GetSampleKeyColumns(),
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("50;40;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("50;50"), result[0].second);

    EXPECT_EQ(YsonToKey("50;50;" _MAX_), result[1].first);
    EXPECT_EQ(YsonToKey("50;60"), result[1].second);
}

TEST_F(TRefineKeyRangeTest, RangesProduct)
{
    auto expr = PrepareExpression(
        "(k = 40 or k = 50 or k = 60) and (l = 40 or l = 50 or l = 60)",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        GetSampleKeyColumns(),
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(9, result.size());

    EXPECT_EQ(YsonToKey("40;40"), result[0].first);
    EXPECT_EQ(YsonToKey("40;40;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("40;50"), result[1].first);
    EXPECT_EQ(YsonToKey("40;50;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("40;60"), result[2].first);
    EXPECT_EQ(YsonToKey("40;60;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("50;40"), result[3].first);
    EXPECT_EQ(YsonToKey("50;40;" _MAX_), result[3].second);

    EXPECT_EQ(YsonToKey("50;50"), result[4].first);
    EXPECT_EQ(YsonToKey("50;50;" _MAX_), result[4].second);

    EXPECT_EQ(YsonToKey("50;60"), result[5].first);
    EXPECT_EQ(YsonToKey("50;60;" _MAX_), result[5].second);

    EXPECT_EQ(YsonToKey("60;40"), result[6].first);
    EXPECT_EQ(YsonToKey("60;40;" _MAX_), result[6].second);

    EXPECT_EQ(YsonToKey("60;50"), result[7].first);
    EXPECT_EQ(YsonToKey("60;50;" _MAX_), result[7].second);

    EXPECT_EQ(YsonToKey("60;60"), result[8].first);
    EXPECT_EQ(YsonToKey("60;60;" _MAX_), result[8].second);
}

TEST_F(TRefineKeyRangeTest, RangesProductWithOverlappingKeyPositions)
{
    auto expr = PrepareExpression(
        "(k, m) in ((2, 3), (4, 6)) and l in (2, 3)",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        GetSampleKeyColumns(),
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(YsonToKey("2;2;3"), result[0].first);
    EXPECT_EQ(YsonToKey("2;2;3;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("2;3;3"), result[1].first);
    EXPECT_EQ(YsonToKey("2;3;3;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("4;2;6"), result[2].first);
    EXPECT_EQ(YsonToKey("4;2;6;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("4;3;6"), result[3].first);
    EXPECT_EQ(YsonToKey("4;3;6;" _MAX_), result[3].second);
}

TEST_F(TRefineKeyRangeTest, BetweenRanges)
{
    auto expr = PrepareExpression(
        R"(
            (k, l) between (
                (1) and (1, 20),
                (2, 30) and (2, 40),
                (3, 50) and (3),
                4 and 5
            )
        )",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        GetSampleKeyColumns(),
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("0"), YsonToKey("100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(YsonToKey("1;" _MIN_), result[0].first);
    EXPECT_EQ(YsonToKey("1;20;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("2;30"), result[1].first);
    EXPECT_EQ(YsonToKey("2;40;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("3;50"), result[2].first);
    EXPECT_EQ(YsonToKey("3;" _MAX_ ";" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("4"), result[3].first);
    EXPECT_EQ(YsonToKey("5;" _MAX_), result[3].second);
}

TEST_F(TRefineKeyRangeTest, NormalizeShortKeys)
{
    auto expr = PrepareExpression(
        "k = 1 and l = 2 and m = 3",
        GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("1"), YsonToKey("2")),
        expr);

    EXPECT_EQ(YsonToKey("1;2;3"), result.first);
    EXPECT_EQ(YsonToKey("1;2;3;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, PrefixQuery)
{
    TTableSchema tableSchema({
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("s", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("b", EValueType::Int64),
    });

    auto expr = PrepareExpression(
        "k = 50 and l = 50 and m = 50 and is_prefix(\"abc\", s)",
        tableSchema);

    auto result = RefineKeyRange(
        GetSampleKeyColumns2(),
        std::make_pair(YsonToKey("1;1;1;aaa"), YsonToKey("100;100;100;bbb")),
        expr);

    EXPECT_EQ(YsonToKey("50;50;50;abc"), result.first);
    EXPECT_EQ(YsonToKey("50;50;50;abd"), result.second);
}

TEST_F(TRefineKeyRangeTest, EmptyRange)
{
    auto expr = PrepareExpression(
        "k between 1 and 1",
        GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("0;0;0"), YsonToKey("2;2;2")),
        expr);

    EXPECT_EQ(YsonToKey("1"), result.first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, RangeToPointCollapsing)
{
    auto expr = PrepareExpression(
        "k >= 1 and k <= 1 and l = 1",
        GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(YsonToKey("0;0;0"), YsonToKey("2;2;2")),
        expr);

    EXPECT_EQ(YsonToKey("1;1"), result.first);
    EXPECT_EQ(YsonToKey("1;1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleRangeDisjuncts)
{
    auto expr = PrepareExpression(
        "(k between 21 and 32) OR (k between 43 and 54)",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyColumns = GetSampleKeyColumns();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        keyColumns,
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(YsonToKey("21"), result[0].first);
    EXPECT_EQ(YsonToKey("32;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("43"), result[1].first);
    EXPECT_EQ(YsonToKey("54;" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, SecondDimensionRange)
{
    auto expr = PrepareExpression(
        "(k, l) >= (1, 2) and (k, l) < (1, 4)",
        GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto keyColumns = GetSampleKeyColumns();
    auto keyTrie = ExtractMultipleConstraints(
        expr,
        keyColumns,
        rowBuffer);

    auto result = GetRangesFromTrieWithinRange(
        std::make_pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        keyTrie,
        rowBuffer);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(YsonToKey("1;2"), result[0].first);
    EXPECT_EQ(YsonToKey("1;4;"), result[0].second);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Test MergeOverlappingRanges
// TODO(lukyan): Test for ranges, partial keys (ranges) and full keys (points)

struct TMyPartition
{
    TRow PivotKey;
    TRow NextPivotKey;
    std::vector<TRow> SampleKeys;
};

struct TMyTablet
{
    TRow PivotKey;
    TRow NextPivotKey;
    std::vector<TMyPartition> Partitions;
};

template <class TIter>
void FillRandomUniqueSequence(TFastRng64& rng, TIter begin, TIter end, size_t min, size_t max)
{
    // TODO: Use linear congruential generator without materialization of sequence.

    YT_VERIFY(end - begin <= max - min);

    if (begin == end) {
        return;
    }

    YT_VERIFY(min < max);

    TIter current = begin;

    do {
        while (current < end) {
            *current++ = rng.Uniform(min, max);
        }

        std::sort(begin, end);
        current = std::unique(begin, end);
    } while (current < end);
}

TSharedRange<size_t> GetRandomUniqueSequence(TFastRng64& rng, size_t length, size_t min, size_t max)
{
    std::vector<ui64> sequence(length);
    FillRandomUniqueSequence(rng, sequence.begin(), sequence.end(), min, max);
    return MakeSharedRange(std::move(sequence));
}

template <class T>
bool CheckRangesAreEquivalent(TRange<std::pair<T, T>> a, TRange<std::pair<T, T>> b)
{
    auto it1 = a.begin();
    auto it2 = b.begin();

    while (true) {
        bool end1 = it1 == a.end();
        bool end2 = it2 == b.end();

        if (end1 || end2) {
            return end1 && end2;
        }

        if (it1->first != it2->first) {
            return false;
        }

        while (true) {
            if (it1->second < it2->second) {
                auto lastBound = it1->second;
                if (++it1 == a.end() || it1->first != lastBound) {
                    return false;
                }
            } else if (it2->second < it1->second) {
                auto lastBound = it2->second;
                if (++it2 == b.end() || it2->first != lastBound) {
                    return false;
                }
            } else {
                ++it1;
                ++it2;
                break;
            }
        }
    }

    Y_UNREACHABLE();
}

TEST(TestHelpers, Step)
{
    auto step = [&] (size_t c, size_t s, size_t t) {
        auto s1 = ((c * t / s + 1) * s + t - 1) / t - c;
        auto s2 = (s + t - 1 - c * t % s) / t;
        auto s3 = Step(c, s, t);

        YT_VERIFY(s1 == s2);
        YT_VERIFY(s2 == s3);
        return s1;
    };

    EXPECT_EQ(step(0, 4, 3), 2);
    EXPECT_EQ(step(1, 4, 3), 1);
    EXPECT_EQ(step(2, 4, 3), 1);
    EXPECT_EQ(step(3, 4, 3), 1);

    // [0 .. 3] -> [0 .. 3]

    for (size_t s = 1; s < 100; ++s) {
        for (size_t t = 1; t <= s; ++t) {
            for (size_t c = 0; c < s; ++c) {
                auto next = step(c, s, t) + c;

                EXPECT_EQ(next * t / s, c * t / s + 1);
            }
        }
    }
}

void TestSplitTablet(
    TRange<size_t> sampleValues,
    TRange<std::pair<size_t, size_t>> rangeValues,
    size_t maxSubsplitsPerTablet)
{
    auto rowBuffer = New<TRowBuffer>();
    auto makeRow = [&] (int value) {
        auto row = rowBuffer->AllocateUnversioned(1);
        row[0] = MakeInt64(value);
        return row;
    };
    auto makeRange = [&] (int a, int b) {
        return TRowRange(makeRow(a), makeRow(b));
    };

    std::vector<TMyPartition> partitons;
    auto& partiton = partitons.emplace_back();
    partiton.PivotKey = makeRow(0);
    partiton.NextPivotKey = makeRow(110);

    for (auto value : sampleValues) {
        partiton.SampleKeys.push_back(makeRow(value)); // Sample key can be equal to PivotKey
    }

    std::vector<TRowRange> ranges;
    for (auto [l, r] : rangeValues) {
        ranges.push_back(makeRange(l, r));
    }

    NLogging::TLogger logger;

    auto result = SplitTablet(
        MakeRange(partitons),
        MakeSharedRange(ranges),
        rowBuffer,
        maxSubsplitsPerTablet,
        true,
        logger);

    std::vector<TRowRange> mergedRanges;
    for (const auto& group : result) {
        mergedRanges.insert(mergedRanges.end(), group.begin(), group.end());
    }

    if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
        Cerr << Format("Source: %v, Samples: %v, MaxSubsplits: %v",
            MakeFormattableView(ranges, TRangeFormatter()),
            partiton.SampleKeys,
            maxSubsplitsPerTablet) << Endl;
        Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TRangeFormatter())) << Endl;

        for (const auto& group : result) {
            Cerr << Format("Group: %v", MakeFormattableView(group, TRangeFormatter())) << Endl;
        }

        GTEST_FAIL() << "Expected ranges are eqivalent";
    }
}

TEST(TestHelpers, TestSplitTablet)
{
    {
        std::pair<size_t, size_t> ranges[] = {{17, 42}, {47, 60}, {64, 75}};
        size_t samples[] = {27, 36, 52, 57, 60, 70, 74, 85, 90};

        TestSplitTablet(samples, ranges, 6);
    }

    {
        std::pair<size_t, size_t> ranges[] = {{6, 17}, {21, 24}, {35, 58}, {68, 79}, {85, 88}};
        size_t samples[] = {5, 22, 23, 46, 49, 99, 100};

        TestSplitTablet(samples, ranges, 64);
    }

    TFastRng64 rng(42);

    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        std::vector<std::pair<size_t, size_t>> ranges;
        std::vector<size_t> samples;

        for (auto index : GetRandomUniqueSequence(rng, rng.Uniform(0, 10), 0, 100)) {
            samples.push_back(index + 10); // Sample key can be equal to PivotKey
        }

        auto bounds = GetRandomUniqueSequence(rng, rng.Uniform(2, 10), 0, 100);
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.emplace_back(bounds[index - 1], bounds[index]);
        }

        size_t maxSubsplitsPerTablet = rng.Uniform(1, 64);

        TestSplitTablet(samples, ranges, maxSubsplitsPerTablet);
    }

    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        auto rowBuffer = New<TRowBuffer>();
        auto makeRow = [&] (size_t value) {
            auto row = rowBuffer->AllocateUnversioned(1);
            row[0] = MakeInt64(value);
            return row;
        };
        auto makeRange = [&] (size_t a, size_t b) {
            return TRowRange(makeRow(a), makeRow(b));
        };

        // Generate tablet structure.
        std::vector<TMyTablet> tablets;
        std::vector<size_t> keys(rng.Uniform(30, 70));
        FillRandomUniqueSequence(rng, keys.begin(), keys.end(), 0, 1000);

        std::vector<size_t> tabletPivotIds(rng.Uniform(1, 5) - 1);
        FillRandomUniqueSequence(rng, tabletPivotIds.begin(), tabletPivotIds.end(), 1, keys.size() - 1);
        tabletPivotIds.push_back(keys.size() - 1);

        size_t lastPivotId = 0;
        for (auto pivotId : tabletPivotIds) {
            auto& tablet = tablets.emplace_back();

            tablet.PivotKey = makeRow(keys[lastPivotId]);
            tablet.NextPivotKey = makeRow(keys[pivotId]);

            size_t maxPartCount = std::min(pivotId - lastPivotId, keys[pivotId] - keys[lastPivotId]);
            YT_VERIFY(maxPartCount > 0);

            std::vector<size_t> partPivotIds(rng.Uniform(0, maxPartCount));
            FillRandomUniqueSequence(rng, partPivotIds.begin(), partPivotIds.end(), lastPivotId + 1, pivotId);
            partPivotIds.push_back(pivotId);

            auto lastPartPivotId = lastPivotId;
            for (auto partPivotId : partPivotIds) {
                auto& partition = tablet.Partitions.emplace_back();
                partition.PivotKey = makeRow(keys[lastPartPivotId]);
                partition.NextPivotKey = makeRow(keys[partPivotId]);

                bool firstSampleIsPivot = false;

                for (size_t sampleId = lastPartPivotId + !firstSampleIsPivot; sampleId < partPivotId; ++sampleId) {
                    partition.SampleKeys.push_back(makeRow(keys[sampleId]));
                }

                lastPartPivotId = partPivotId;
            }

            YT_VERIFY(lastPartPivotId == pivotId);
            lastPivotId = pivotId;
        }

        YT_VERIFY(lastPivotId + 1 == keys.size());

        // Generate ranges.
        std::vector<size_t> bounds(std::min(rng.Uniform(1, 30), keys.back() - keys.front()));
        FillRandomUniqueSequence(rng, bounds.begin(), bounds.end(), keys.front(), keys.back());

        std::vector<TRowRange> ranges;
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.push_back(makeRange(bounds[index - 1], bounds[index]));
        }

        size_t maxSubsplitsPerTablet = rng.Uniform(1, 100);

        NLogging::TLogger logger;
        std::vector<TRowRange> mergedRanges;

        SplitRangesByTablets(
            MakeRange(ranges),
            MakeRange(tablets),
            makeRow(0),
            makeRow(1000),
            [&] (auto shardIt, auto rangesIt, auto rangesItEnd) {

                const auto& tablet = *(shardIt - 1);

                std::vector<TRowRange> rangesSlice(rangesIt, rangesItEnd);

                auto groups = SplitTablet(
                    MakeRange(tablet.Partitions),
                    MakeSharedRange(rangesSlice),
                    rowBuffer,
                    maxSubsplitsPerTablet,
                    true,
                    logger);

                for (const auto& group : groups) {
                    mergedRanges.insert(mergedRanges.end(), group.begin(), group.end());
                }
            });

        if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
            Cerr << Format("Ranges: %v", MakeFormattableView(ranges, TRangeFormatter())) << Endl;
            Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TRangeFormatter())) << Endl;

            Cerr << Format("Tablets: %v", MakeFormattableView(tablets, [] (auto* builder, const auto& tablet) {
                builder->AppendFormat("[%v .. %v]",
                    tablet.PivotKey,
                    tablet.NextPivotKey);
            })) << Endl;

            for (const auto& tablet : tablets) {
                Cerr << Format("Tablet [%v..%v] \t %v",
                    tablet.PivotKey,
                    tablet.NextPivotKey,
                    MakeFormattableView(tablet.Partitions, [] (auto* builder, const auto& partition) {
                        builder->AppendFormat("[%v .. %v] : %v",
                            partition.PivotKey,
                            partition.NextPivotKey,
                            partition.SampleKeys);
                    })) << Endl;
            }

            GTEST_FAIL() << "Expected ranges are eqivalent";
        }
    }
}

TEST(TestHelpers, SplitByPivots)
{
    using TItemIt = const std::pair<int, int>*;
    using TShardIt = const int*;

    struct TPredicate
    {
        // itemIt PRECEDES shardIt
        bool operator() (TItemIt itemIt, TShardIt shardIt) const
        {
            return itemIt->second <= *shardIt;
        }

        // itemIt FOLLOWS shardIt
        bool operator() (TShardIt shardIt, TItemIt itemIt) const
        {
            return *shardIt <= itemIt->first;
        }

    };

    TFastRng64 rng(42);
    for (size_t iteration = 0; iteration < 1000; ++iteration) {
        // Generate ranges.
        std::vector<int> bounds(rng.Uniform(1, 30));
        FillRandomUniqueSequence(rng, bounds.begin(), bounds.end(), 0, 100);

        std::vector<std::pair<int, int>> ranges;
        for (size_t index = 1; index < bounds.size(); index += rng.GenRand64() % 3 ? 2 : 1) {
            ranges.emplace_back(bounds[index - 1], bounds[index]);
        }

        // Generate pivots.
        std::vector<int> pivots(rng.Uniform(1, 30));
        FillRandomUniqueSequence(rng, pivots.begin(), pivots.end(), 0, 100);

        std::vector<std::pair<int, int>> mergedRanges;

        SplitByPivots(
            MakeRange(ranges),
            MakeRange(pivots),
            TPredicate{},
            [&] (auto itemsIt, auto itemsItEnd, auto shardIt) {
                mergedRanges.insert(mergedRanges.end(), itemsIt, itemsItEnd);
            },
            [&] (auto shardIt, auto shardItEnd, auto itemsIt) {
                auto lastBound = itemsIt->first;

                for (auto it = shardIt; it != shardItEnd; ++it) {
                    mergedRanges.emplace_back(lastBound, *it);
                    lastBound = *it;
                }

                mergedRanges.emplace_back(lastBound, itemsIt->second);
            });

        if (!CheckRangesAreEquivalent(MakeRange(ranges), MakeRange(mergedRanges))) {
            struct TPairFormatter
            {
                void operator()(TStringBuilderBase* builder, std::pair<int, int> source) const
                {
                    builder->AppendFormat("[%v .. %v]",
                        source.first,
                        source.second);
                }
            };

            Cerr << Format("Ranges: %v", MakeFormattableView(ranges, TPairFormatter())) << Endl;
            Cerr << Format("Merged: %v", MakeFormattableView(mergedRanges, TPairFormatter())) << Endl;

            GTEST_FAIL() << "Expected ranges are eqivalent";
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <>
TRow GetPivotKey(const TMyTablet& shard)
{
    return shard.PivotKey;
}

template <>
TRow GetPivotKey(const TMyPartition& shard)
{
    return shard.PivotKey;
}

template <>
TRow GetNextPivotKey(const TMyPartition& shard)
{
    return shard.NextPivotKey;
}

template <>
TRange<TRow> GetSampleKeys(const TMyPartition& shard)
{
    return shard.SampleKeys;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

