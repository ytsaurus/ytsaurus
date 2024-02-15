#include "ql_helpers.h"

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/range_inferrer.h>

#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/base/constraints.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/coordinator.h>

#include <yt/yt/library/query/engine/folding_profiler.h>

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
    auto mp = [] (const TLegacyOwningKey& a, const TLegacyOwningKey& b) {
        return std::pair(a, b);
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
    auto mp = [] (const TLegacyOwningKey& a, const TLegacyOwningKey& b) {
        return std::pair(a, b);
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
    auto mp = [] (const TLegacyOwningKey& a, const TLegacyOwningKey& b) {
        return std::pair(a, b);
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

TConstraintRef ConstraintFromLowerBound(
    TConstraintsHolder* constraints,
    TUnversionedRow keyBound,
    int keyColumnCount)
{
    // Lower bound is included.
    auto current = TConstraintRef::Universal();
    ui32 keyColumnIndex = keyColumnCount;


    // TODO(lukyan): Support sentinels.
    for (const auto& value : keyBound.Elements()) {
        YT_VERIFY(!IsSentinelType(value.Type));
    }

    while (keyColumnIndex > 0) {
        --keyColumnIndex;

        if (keyColumnIndex >= keyBound.GetCount()) {
            continue;
        }

        current = constraints->Append({
                TConstraint::Make(
                    TValueBound{keyBound[keyColumnIndex], false},
                    TValueBound{keyBound[keyColumnIndex], true},
                    current),
                TConstraint::Make(TValueBound{keyBound[keyColumnIndex], true}, MaxBound),
            },
            keyColumnIndex);
    }

    return current;
}

TConstraintRef ConstraintFromUpperBound(
    TConstraintsHolder* constraints,
    TUnversionedRow keyBound,
    int keyColumnCount)
{
    // Lower bound is excluded.
    auto current = TConstraintRef::Empty();
    ui32 keyColumnIndex = keyColumnCount;

    // TODO(lukyan): Support sentinels.
    for (const auto& value : keyBound.Elements()) {
        YT_VERIFY(!IsSentinelType(value.Type));
    }

    while (keyColumnIndex > 0) {
        --keyColumnIndex;

        if (keyColumnIndex >= keyBound.GetCount()) {
            continue;
        }

        current = constraints->Append({
                TConstraint::Make(MinBound, TValueBound{keyBound[keyColumnIndex], false}),
                TConstraint::Make(
                    TValueBound{keyBound[keyColumnIndex], false},
                    TValueBound{keyBound[keyColumnIndex], true},
                    current),
            },
            keyColumnIndex);
    }

    return current;
}

TConstraintRef GetConstraintFromKeyRange(TConstraintsHolder* constraints, TRowRange keyRange, int keyColumnCount)
{
    auto lower = ConstraintFromLowerBound(constraints, keyRange.first, keyColumnCount);
    auto upper = ConstraintFromUpperBound(constraints, keyRange.second, keyColumnCount);

    return constraints->Intersect(lower, upper);
}

std::vector<TRowRange> GetRangesFromConstraints(
    TRowBufferPtr buffer,
    int keyColumnCount,
    const TConstraintsHolder& constraints,
    TConstraintRef constraintRef,
    ui64 rangeCountLimit = std::numeric_limits<ui64>::max())
{
    std::vector<TRowRange> resultRanges;

    TReadRangesGenerator rangesGenerator(constraints);

    rangesGenerator.GenerateReadRanges(
        constraintRef,
        [&] (TRange<TColumnConstraint> constraintRow, ui64 /*rangeExpansionLimit*/) {
            auto boundRow = buffer->AllocateUnversioned(keyColumnCount );

            int columnId = 0;
            while (columnId < std::ssize(constraintRow) && constraintRow[columnId].IsExact()) {
                boundRow[columnId] = constraintRow[columnId].GetValue();
                ++columnId;
            }

            auto prefixSize = columnId;
            auto keyColumnCount = std::ssize(constraintRow);

            TRowRange rowRange;
            if (prefixSize < keyColumnCount) {
                auto lowerBound = MakeLowerBound(buffer.Get(), MakeRange(boundRow.Begin(), prefixSize), constraintRow[prefixSize].Lower);
                auto upperBound = MakeUpperBound(buffer.Get(), MakeRange(boundRow.Begin(), prefixSize), constraintRow[prefixSize].Upper);

                rowRange = std::pair(lowerBound, upperBound);
            } else {
                rowRange = RowRangeFromPrefix(buffer.Get(), MakeRange(boundRow.Begin(), prefixSize));
            }

            if (resultRanges.empty() || resultRanges.back().second < rowRange.first) {
                resultRanges.push_back(rowRange);
            } else if (resultRanges.back().second == rowRange.first) {
                YT_VERIFY(!resultRanges.empty());
                resultRanges.back().second = rowRange.second;
            } else {
                YT_VERIFY(resultRanges.back().second == rowRange.second && resultRanges.back().first <= rowRange.first);
            }
        },
        rangeCountLimit);

    return resultRanges;
}

std::vector<TRowRange> GetRangesFromExpression(
    TRowBufferPtr buffer,
    const TKeyColumns& keyColumns,
    TConstExpressionPtr predicate,
    const TKeyRange& keyRange,
    ui64 rangeCountLimit = std::numeric_limits<ui64>::max())
{
    TConstraintsHolder constraints(keyColumns.size());
    auto constraintRef = constraints.ExtractFromExpression(predicate, keyColumns, buffer);

    constraintRef = constraints.Intersect(
        constraintRef,
        GetConstraintFromKeyRange(&constraints, keyRange, keyColumns.size()));

    return GetRangesFromConstraints(buffer, std::ssize(keyColumns), constraints, constraintRef, rangeCountLimit);
}

TKeyRange RefineKeyRange(
    const TKeyColumns& keyColumns,
    const TKeyRange& keyRange,
    TConstExpressionPtr predicate)
{
    auto buffer = New<TRowBuffer>();

    auto result = GetRangesFromExpression(buffer, keyColumns, predicate, keyRange);

    if (result.empty()) {
        return std::pair(EmptyKey(), EmptyKey());
    }

    // Check that ranges are adjacent.
    for (int index = 0; index + 1 < std::ssize(result); ++index) {
        YT_VERIFY(result[index].second == result[index + 1].first);
    }

    return {TLegacyOwningKey(result.front().first), TLegacyOwningKey(result.back().second)};
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

    TLegacyOwningKey GetInitialLeftBound() const
    {
        return YsonToKey(InitialLeftBoundAsYson);
    }

    TLegacyOwningKey GetInitialRightBound() const
    {
        return YsonToKey(InitialRightBoundAsYson);
    }

    TLegacyOwningKey GetResultingLeftBound() const
    {
        return YsonToKey(ResultingLeftBoundAsYson);
    }

    TLegacyOwningKey GetResultingRightBound() const
    {
        return YsonToKey(ResultingRightBoundAsYson);
    }
};

class TRefineKeyRangeTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TRefineKeyRangeTestCase>
{
protected:
    void SetUp() override
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
        std::pair(
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
        std::pair(
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

TEST_F(TRefineKeyRangeTest, NotEqual)
{
    {
        auto expr = Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
            Make<TReferenceExpression>("m"),
            Make<TLiteralExpression>(MakeInt64(50)));

        auto buffer = New<TRowBuffer>();

        auto result = GetRangesFromExpression(
            buffer,
            GetSampleKeyColumns(),
            expr,
            {YsonToKey("1;1;1"), YsonToKey("1;1;100")});

        EXPECT_EQ(2u, result.size());

        EXPECT_EQ(YsonToKey("1;1;1"), result[0].first);
        EXPECT_EQ(YsonToKey("1;1;50"), result[0].second);

        EXPECT_EQ(YsonToKey("1;1;50;" _MAX_), result[1].first);
        EXPECT_EQ(YsonToKey("1;1;100"), result[1].second);
    }

    {
        auto expr = Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
            Make<TReferenceExpression>("k"),
            Make<TLiteralExpression>(MakeInt64(50)));

        auto buffer = New<TRowBuffer>();

        auto result = GetRangesFromExpression(
            buffer,
            GetSampleKeyColumns(),
            expr,
            {YsonToKey("1;1;1"), YsonToKey("100;100;100")});

        EXPECT_EQ(2u, result.size());

        EXPECT_EQ(YsonToKey("1;1;1"), result[0].first);
        EXPECT_EQ(YsonToKey("50"), result[0].second);

        EXPECT_EQ(YsonToKey("50;" _MAX_), result[1].first);
        EXPECT_EQ(YsonToKey("100;100;100"), result[1].second);
    }
}

TEST_F(TRefineKeyRangeTest, Empty)
{
    auto expr = PrepareExpression("false", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1"), YsonToKey("100")),
        expr);

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, ContradictiveConjuncts)
{
    auto expr = PrepareExpression("k >= 90 and k < 10", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, Lookup1)
{
    auto expr = PrepareExpression("k = 50 and l = 50", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;50"), result.first);
    EXPECT_EQ(YsonToKey("50;50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, Lookup2)
{
    auto expr = PrepareExpression("k = 50 and l = 50 and m = 50", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;50;50"), result.first);
    EXPECT_EQ(YsonToKey("50;50;50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, Range1)
{
    auto expr = PrepareExpression("k > 0 and k < 100", *GetSampleTableSchema());

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    auto result = RefineKeyRange(
        keyColumns,
        std::pair(YsonToKey(""), YsonToKey("1000000000")),
        expr);

    EXPECT_EQ(YsonToKey("0;" _MAX_), result.first);
    EXPECT_EQ(YsonToKey("100"), result.second);
}

TEST_F(TRefineKeyRangeTest, NegativeRange1)
{
    auto expr = PrepareExpression("k > -100 and (k) <= -(-1)", *GetSampleTableSchema());

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    auto result = RefineKeyRange(
        keyColumns,
        std::pair(YsonToKey(""), YsonToKey("1000000000")),
        expr);

    EXPECT_EQ(YsonToKey("-100;" _MAX_), result.first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts1)
{
    auto expr = PrepareExpression("k >= 10 and k < 90", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("10"), result.first);
    EXPECT_EQ(YsonToKey("90"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts2)
{
    auto expr = PrepareExpression(
        "k = 50 and l >= 10 and l < 90 and m = 50",
        *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50;10"), result.first);
    EXPECT_EQ(YsonToKey("50;90"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts3)
{
    auto expr = PrepareExpression("k = 50 and m = 50", *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        expr);

    EXPECT_EQ(YsonToKey("50"), result.first);
    EXPECT_EQ(YsonToKey("50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, EmptyKeyTrie)
{
    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromTrieWithinRange(
        std::pair(YsonToKey(_MIN_), YsonToKey(_MAX_)),
        TKeyTrie::Empty(),
        rowBuffer);

    EXPECT_EQ(0u, result.size());
}

TEST_F(TRefineKeyRangeTest, MultipleDisjuncts)
{
    auto expr = PrepareExpression(
        "k = 50 and m = 50 or k = 75 and m = 50",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("50"), result[0].first);
    EXPECT_EQ(YsonToKey("50;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("75"), result[1].first);
    EXPECT_EQ(YsonToKey("75;" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, NotEqualToMultipleRanges)
{
    auto expr = PrepareExpression(
        "(k = 50 and l != 50) and (l > 40 and l < 60)",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("50;40;" _MAX_), result[0].first);
    EXPECT_EQ(YsonToKey("50;50"), result[0].second);

    EXPECT_EQ(YsonToKey("50;50;" _MAX_), result[1].first);
    EXPECT_EQ(YsonToKey("50;60"), result[1].second);
}

TEST_F(TRefineKeyRangeTest, RangesProduct)
{
    auto expr = PrepareExpression(
        "(k = 40 or k = 50 or k = 60) and (l = 40 or l = 50 or l = 60)",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(9u, result.size());

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
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(4u, result.size());

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
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("0"), YsonToKey("100")));

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey("1"), result[0].first);
    EXPECT_EQ(YsonToKey("1;20;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("2;30"), result[1].first);
    EXPECT_EQ(YsonToKey("2;40;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("3;50"), result[2].first);
    EXPECT_EQ(YsonToKey("3;" _MAX_ ), result[2].second);

    EXPECT_EQ(YsonToKey("4"), result[3].first);
    EXPECT_EQ(YsonToKey("5;" _MAX_), result[3].second);
}

TEST_F(TRefineKeyRangeTest, NormalizeShortKeys)
{
    auto expr = PrepareExpression(
        "k = 1 and l = 2 and m = 3",
        *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("1"), YsonToKey("2")),
        expr);

    EXPECT_EQ(YsonToKey("1;2;3"), result.first);
    EXPECT_EQ(YsonToKey("1;2;3;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, PrefixQuery)
{
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("k", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("s", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("b", EValueType::Int64),
    });

    auto expr = PrepareExpression(
        "k = 50 and l = 50 and m = 50 and is_prefix(\"abc\", s)",
        *tableSchema);

    auto result = RefineKeyRange(
        GetSampleKeyColumns2(),
        std::pair(YsonToKey("1;1;1;aaa"), YsonToKey("100;100;100;bbb")),
        expr);

    EXPECT_EQ(YsonToKey("50;50;50;abc"), result.first);
    EXPECT_EQ(YsonToKey("50;50;50;abd"), result.second);
}

TEST_F(TRefineKeyRangeTest, EmptyRange)
{
    auto expr = PrepareExpression(
        "k between 1 and 1",
        *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("0;0;0"), YsonToKey("2;2;2")),
        expr);

    EXPECT_EQ(YsonToKey("1"), result.first);
    EXPECT_EQ(YsonToKey("1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, RangeToPointCollapsing)
{
    auto expr = PrepareExpression(
        "k >= 1 and k <= 1 and l = 1",
        *GetSampleTableSchema());

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::pair(YsonToKey("0;0;0"), YsonToKey("2;2;2")),
        expr);

    EXPECT_EQ(YsonToKey("1;1"), result.first);
    EXPECT_EQ(YsonToKey("1;1;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleRangeDisjuncts)
{
    auto expr = PrepareExpression(
        "(k between 21 and 32) OR (k between 43 and 54)",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("21"), result[0].first);
    EXPECT_EQ(YsonToKey("32;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("43"), result[1].first);
    EXPECT_EQ(YsonToKey("54;" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, SecondDimensionRange)
{
    auto expr = PrepareExpression(
        "(k, l) >= (1, 2) and (k, l) < (1, 4)",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(1u, result.size());

    EXPECT_EQ(YsonToKey("1;2"), result[0].first);
    EXPECT_EQ(YsonToKey("1;4;"), result[0].second);
}

TEST_F(TRefineKeyRangeTest, InTuples)
{
    auto expr = PrepareExpression(
        "(k, l) in ((1, 2), (1, 2), (1, 3), (2, 1), (2, 2))",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        7);

    EXPECT_EQ(4u, result.size());

    EXPECT_EQ(YsonToKey("1;2"), result[0].first);
    EXPECT_EQ(YsonToKey("1;2;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("1;3"), result[1].first);
    EXPECT_EQ(YsonToKey("1;3;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("2;1"), result[2].first);
    EXPECT_EQ(YsonToKey("2;1;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("2;2"), result[3].first);
    EXPECT_EQ(YsonToKey("2;2;" _MAX_), result[3].second);
}

TEST_F(TRefineKeyRangeTest, RangeExpansionLimit)
{
    auto expr = PrepareExpression(
        "k in (10, 20, 30, 40, 50) and l in (1, 3, 5, 7)",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")),
        7);

    EXPECT_EQ(5u, result.size());

    EXPECT_EQ(YsonToKey("10;1"), result[0].first);
    EXPECT_EQ(YsonToKey("10;7;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("20;1"), result[1].first);
    EXPECT_EQ(YsonToKey("20;7;" _MAX_), result[1].second);

    EXPECT_EQ(YsonToKey("30;1"), result[2].first);
    EXPECT_EQ(YsonToKey("30;7;" _MAX_), result[2].second);

    EXPECT_EQ(YsonToKey("40;1"), result[3].first);
    EXPECT_EQ(YsonToKey("40;7;" _MAX_), result[3].second);

    EXPECT_EQ(YsonToKey("50;1"), result[4].first);
    EXPECT_EQ(YsonToKey("50;7;" _MAX_), result[4].second);
}

TEST_F(TRefineKeyRangeTest, RedundantCondition)
{
    // Test case from ticket YT-19004.
    auto expr = PrepareExpression("k = 2 and (k = 2 and l = 3 or l = 4)", *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("2;3"), result[0].first);
    EXPECT_EQ(YsonToKey("2;3;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("2;4"), result[1].first);
    EXPECT_EQ(YsonToKey("2;4;" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, InColumnPermutation)
{
    auto expr = PrepareExpression(
        "(l, k) in ((0, 5), (1, 3))",
        *GetSampleTableSchema());

    auto rowBuffer = New<TRowBuffer>();
    auto result = GetRangesFromExpression(
        rowBuffer,
        GetSampleKeyColumns(),
        expr,
        std::pair(YsonToKey("1;1;1"), YsonToKey("100;100;100")));

    EXPECT_EQ(2u, result.size());

    EXPECT_EQ(YsonToKey("3;1"), result[0].first);
    EXPECT_EQ(YsonToKey("3;1;" _MAX_), result[0].second);

    EXPECT_EQ(YsonToKey("5;0"), result[1].first);
    EXPECT_EQ(YsonToKey("5;0;" _MAX_), result[1].second);
}

const TString Letters("ABCDEFGHIJKLMNOPQRSTUVWXYZ");

struct TRandomExpressionGenerator
{
    TTableSchemaPtr Schema;
    TRowBufferPtr RowBuffer;
    TColumnEvaluatorPtr ColumnEvaluator;

    TFastRng64 Rng{42};

    std::vector<ui64> RandomValues = GenerateRandomValues();

    int GetExponentialDistribution(int power)
    {
        YT_VERIFY(power > 0);
        int uniformValue = Rng.Uniform(1 << (power - 1));
        int valueBitCount = uniformValue != 0 ? GetValueBitCount(uniformValue) : 0;
        int result = (power - 1) - valueBitCount;
        YT_VERIFY(result >= 0 && result < power);
        return result;
    }

    std::vector<ui64> GenerateRandomValues()
    {
        std::vector<ui64> result;

        // Corner cases.
        result.push_back(std::numeric_limits<i64>::min());
        result.push_back(std::numeric_limits<i64>::max());
        result.push_back(std::numeric_limits<ui64>::min());
        result.push_back(std::numeric_limits<ui64>::max());

        // Some random values.
        for (int i = 0; i < 7; ++i) {
            result.push_back(Rng.GenRand());
        }

        for (int i = 0; i < 5; ++i) {
            ui64 bits = Rng.Uniform(64);
            ui64 value = (1ULL << bits) | Rng.Uniform(1ULL << bits);

            result.push_back(value);
        }

        std::sort(result.begin(), result.end());
        result.erase(std::unique(result.begin(), result.end()), result.end());

        return result;
    }

    std::vector<int> GenerateRandomFieldIds(int size)
    {
        std::vector<int> result;
        for (int i = 0; i < size; ++i) {
            result.push_back(GetExponentialDistribution(Schema->GetKeyColumnCount() - 1) + 1);
        }

        return result;
    }

    TString GenerateFieldTuple(TRange<int> ids)
    {
        YT_VERIFY(!ids.Empty());

        TString result = ids.Size() > 1 ? "(" : "";

        bool first = true;
        for (auto id : ids) {
            if (!first) {
                result += ", ";
            } else {
                first = false;
            }

            result += Schema->Columns()[id].Name();
        }

        result += ids.Size() > 1 ? ")" : "";
        return result;
    }

    TString GenerateLiteralTuple(TRange<int> ids)
    {
        YT_VERIFY(!ids.Empty());

        TString result = ids.Size() > 1 ? "(" : "";

        bool first = true;
        for (auto id : ids) {
            if (!first) {
                result += ", ";
            } else {
                first = false;
            }

            result += GenerateRandomLiteral(Schema->Columns()[id].GetWireType());
        }

        result += ids.Size() > 1 ? ")" : "";
        return result;
    }

    TString GenerateRandomLiteral(EValueType type)
    {
        bool nullValue = Rng.Uniform(10) == 0;
        if (nullValue) {
            return "null";
        }

        switch (type) {
            case EValueType::Int64:
                return Format("%v", static_cast<i64>(GenerateInt()));
            case EValueType::Uint64:
                return Format("%vu", GenerateInt());
            // FIXME: Pregenerate random strings.
            case EValueType::String: {
                static constexpr int MaxStringLength = 10;
                auto length = Rng.Uniform(MaxStringLength);

                TString result(length, '\0');
                for (size_t index = 0; index < length; ++index) {
                    result[index] = Letters[Rng.Uniform(Letters.size())];
                }

                return Format("%Qv", result);
            }
            case EValueType::Double:
                return Format("%v", Rng.GenRandReal1() * (1ULL << 63));
            case EValueType::Boolean:
                return Rng.Uniform(2) ? "true" : "false";
            default:
                YT_ABORT();
        }
    }

    TUnversionedValue GenerateRandomUnversionedLiteral(EValueType type)
    {
        bool nullValue = Rng.Uniform(10) == 0;
        if (nullValue) {
            return MakeUnversionedNullValue();
        }

        switch (type) {
            case EValueType::Int64:
                return MakeUnversionedInt64Value(GenerateInt());
            case EValueType::Uint64:
                return MakeUnversionedUint64Value(GenerateInt());
            case EValueType::String: {
                static constexpr int MaxStringLength = 10;
                auto length = Rng.Uniform(MaxStringLength);

                char* data = RowBuffer->GetPool()->AllocateUnaligned(length);
                for (size_t index = 0; index < length; ++index) {
                    data[index] = Letters[Rng.Uniform(Letters.size())];
                }

                return MakeUnversionedStringValue(TStringBuf(data, length));
            }
            case EValueType::Double:
                return MakeUnversionedDoubleValue(Rng.GenRandReal1() * (1ULL << 63));
            case EValueType::Boolean:
                return MakeUnversionedBooleanValue(Rng.Uniform(2));
            default:
                YT_ABORT();
        }
    }

    ui64 GenerateInt()
    {
        return RandomValues[Rng.Uniform(RandomValues.size())];
    }

    TString GenerateRelation(int tupleSize)
    {
        auto ids = GenerateRandomFieldIds(tupleSize);

        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

        return GenerateRelation(ids);
    }

    TString GenerateRelation(TRange<int> ids)
    {
        const char* reationOps[] = {">", ">=", "<", "<=", "=", "!=", "IN"};
        const char* reationOp = reationOps[Rng.Uniform(7)];

        return GenerateRelation(ids, reationOp);
    }

    TString GenerateRelation(TRange<int> ids, const char* reationOp)
    {
        TString result = GenerateFieldTuple(ids);
        result += Format(" %v ", reationOp);
        if (reationOp == TString("IN")) {
            result += "(";
            int tupleCount = GetExponentialDistribution(9) + 1;
            bool first = true;
            for (int i = 0; i < tupleCount; ++i) {
                if (!first) {
                    result += ", ";
                } else {
                    first = false;
                }
                result += GenerateLiteralTuple(ids);
            }

            result += ")";
        } else {
            result += GenerateLiteralTuple(ids);
        }

        return result;
    }

    TString RowToLiteralTuple(TUnversionedRow row)
    {
        TString result =  "(";
        bool first = true;
        for (int columnIndex = 0; columnIndex < static_cast<int>(row.GetCount()); ++columnIndex) {
            if (!first) {
                result += ", ";
            } else {
                first = false;
            }

            auto value = row[columnIndex];

            switch (value.Type) {
                case EValueType::Null:
                    result += "null";
                    break;
                case EValueType::Int64:
                    result += Format("%v", value.Data.Int64);
                    break;
                case EValueType::Uint64:
                    result += Format("%vu", value.Data.Uint64);
                    break;
                case EValueType::String:
                    result += Format("%Qv", value.AsStringBuf());
                    break;
                case EValueType::Double:
                    result += Format("%v", value.Data.Double);
                    break;
                case EValueType::Boolean:
                    result += value.Data.Boolean ? "true" : "false";
                    break;
                default:
                    YT_ABORT();
            }
        }

        result += ")";
        return result;
    }

    TString GenerateContinuationToken(int keyColumnCount)
    {
        YT_VERIFY(keyColumnCount > 1);
        std::vector<int> ids;
        for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
            ids.push_back(columnIndex);
        }

        TString result = GenerateFieldTuple(ids);

        const char* reationOps[] = {">", ">=", "<", "<="};
        const char* reationOp = reationOps[Rng.Uniform(4)];

        result += Format(" %v ", reationOp);
        result += RowToLiteralTuple(GenerateRandomRow(keyColumnCount));
        return result;
    }

    TRow GenerateRandomRow(int keyColumnCount)
    {
        auto row = RowBuffer->AllocateUnversioned(keyColumnCount);
        for (int columnIndex = 0; columnIndex < keyColumnCount; ++columnIndex) {
            if (!Schema->Columns()[columnIndex].Expression()) {
                row[columnIndex] = GenerateRandomUnversionedLiteral(Schema->Columns()[columnIndex].GetWireType());
            }
        }

        ColumnEvaluator->EvaluateKeys(row, RowBuffer);

        return row;
    }

    TString GenerateRelationOrContinuationToken()
    {
        return Rng.Uniform(4) == 0
            ? GenerateContinuationToken(GetExponentialDistribution(Schema->GetKeyColumnCount() - 2) + 2)
            : GenerateRelation(GetExponentialDistribution(3) + 1);
    }

    TString GenerateExpression2()
    {
        TString result = GenerateRelationOrContinuationToken();

        int count = GetExponentialDistribution(5);
        for (int i = 0; i < count; ++i) {
            result += Rng.Uniform(4) == 0 ? " OR " : " AND ";
            result += GenerateRelation(1);

        }
        return result;
    }
};

class TInferRangesTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<const char*, EValueType, EValueType>>
{
protected:
    void SetUp() override
    { }
};

TEST_P(TInferRangesTest, Stress)
{
    // 1. Generate expression.
    //    - alternate column, relation (< > = IN, BETWEEN, ), constants, logical ops (OR, AND)
    //    - if column is evaluated use suitable constant, depending on other columns.
    // 2. Infer ranges.
    // 3. Generate random range (random or depending on inferred ranges (inside or outside)).
    // 4. Check in ranges and evaluate expression.

    auto computedColumnExpression = std::get<0>(GetParam());
    auto hashColumnType = std::get<1>(GetParam());
    auto keyColumnType = std::get<2>(GetParam());

    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("h", hashColumnType)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(computedColumnExpression),
        TColumnSchema("k", keyColumnType)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("m", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("n", EValueType::Boolean)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("v", EValueType::Int64)
    });

    auto rowBuffer = New<TRowBuffer>();

    auto config = New<TColumnEvaluatorCacheConfig>();
    auto columnEvaluatorCache = CreateColumnEvaluatorCache(config);

    auto columnEvaluator = columnEvaluatorCache->Find(schema);

    TRandomExpressionGenerator gen{schema, rowBuffer, columnEvaluator};

    auto testExpression = [&] (TString expressionString) {
        Cout << Format("Expression: %v", expressionString) << Endl;

        auto expr = PrepareExpression(expressionString, *schema);

        TQueryOptions options;
        options.RangeExpansionLimit = 1000;
        options.VerboseLogging = true;

        auto inferredRanges = GetPrunedRanges(
            expr,
            schema,
            schema->GetKeyColumns(),
            {},
            MakeSingletonRowRange(NTableClient::MinKey(), NTableClient::MaxKey()),
            rowBuffer,
            columnEvaluatorCache,
            GetBuiltinRangeExtractors(),
            options,
            {});

        Y_UNUSED(inferredRanges);

        TCGVariables variables;
        auto image = Profile(expr, schema, /*id*/ nullptr, &variables)();
        auto instance = image.Instantiate();

        for (int j = 0; j < 1000; ++j) {
            // Generate random row.
            auto row = gen.GenerateRandomRow(schema->GetKeyColumnCount());

            // Evaluate predicate.
            TUnversionedValue resultValue{};
            instance.Run(
                variables.GetLiteralValues(),
                variables.GetOpaqueData(),
                variables.GetOpaqueDataSizes(),
                &resultValue,
                row.Elements(),
                rowBuffer);

            // Validate row in ranges.
            auto foundIt = BinarySearch(inferredRanges.begin(), inferredRanges.end(), [&] (TRowRange* rowRange) {
                return rowRange->second <= row;
            });

            bool rowInRanges = foundIt != inferredRanges.end() && foundIt->first <= row;

            EXPECT_FALSE(resultValue.Data.Boolean && !rowInRanges) <<
                Format("Expression: %v, InferedRanges: %v, RandomRow: %v",
                    expressionString,
                    inferredRanges,
                    row);
        }
    };

    for (int i = 0; i < 1000; ++i) {
        testExpression(gen.GenerateRelation(gen.GetExponentialDistribution(3) + 1));
        testExpression(gen.GenerateExpression2());
    }
}

INSTANTIATE_TEST_SUITE_P(
    Stress,
    TInferRangesTest,
    ::testing::Values(
        std::tuple("farm_hash(k)", EValueType::Uint64, EValueType::Int64),
        std::tuple("farm_hash(k)", EValueType::Uint64, EValueType::Uint64),
        std::tuple("farm_hash(k) % 10", EValueType::Uint64, EValueType::Int64),
        std::tuple("farm_hash(k) % 10", EValueType::Uint64, EValueType::Uint64),
        std::tuple("farm_hash(k / 3) % 10", EValueType::Uint64, EValueType::Int64),
        std::tuple("farm_hash(k / 3) % 10", EValueType::Uint64, EValueType::Uint64)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient

