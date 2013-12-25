#include "stdafx.h"
#include "framework.h"

#include <ytlib/object_client/helpers.h>

#include <ytlib/query_client/plan_fragment.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/query_client/coordinate_controller.h>
#include <ytlib/query_client/plan_node.h>
#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/plan_visitor.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>

#include "versioned_table_client_ut.h"

namespace NYT {

namespace NVersionedTableClient {

void PrintTo(const TOwningKey& key, ::std::ostream* os)
{
    *os << KeyToYson(key.Get());
}

void PrintTo(const TKey& key, ::std::ostream* os)
{
    *os << KeyToYson(key);
}

} // namespace NVersionedTableClient

namespace NQueryClient {

namespace {

using namespace NYPath;
using namespace NObjectClient;
using namespace NVersionedTableClient;

using ::testing::_;
using ::testing::StrictMock;
using ::testing::HasSubstr;
using ::testing::ContainsRegex;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::AllOf;

////////////////////////////////////////////////////////////////////////////////

class TPrepareCallbacksMock
    : public IPrepareCallbacks
{
public:
    MOCK_METHOD1(GetInitialSplit, TFuture<TErrorOr<TDataSplit>>(const TYPath&));
};

class TCoordinateCallbacksMock
    : public ICoordinateCallbacks
{
public:
    MOCK_METHOD1(GetReader, IReaderPtr(const TDataSplit&));
    MOCK_METHOD1(CanSplit, bool(const TDataSplit&));
    MOCK_METHOD1(SplitFurther, TFuture<TErrorOr<std::vector<TDataSplit>>>(const TDataSplit&));
    MOCK_METHOD2(Delegate, IReaderPtr(const TPlanFragment&, const TDataSplit&));
};

MATCHER_P(HasCounter, expectedCounter, "")
{
    auto objectId = GetObjectIdFromDataSplit(arg);
    auto cellId = CellIdFromId(objectId);
    auto counter = CounterFromId(objectId);

    if (cellId != 0x42) {
        *result_listener << "cell id is bad";
        return false;
    }

    if (counter != expectedCounter) {
        *result_listener
            << "actual counter id is " << counter << " while "
            << "expected counter id is " << expectedCounter;
        return false;
    }

    return true;
}

MATCHER_P(HasLowerBound, encodedLowerBound, "")
{
    auto expected = BuildKey(encodedLowerBound);
    auto actual = GetLowerBoundFromDataSplit(arg);

    auto result = TKeyComparer()(expected, actual);

    if (result != 0 && result_listener->IsInterested()) {
        *result_listener << "expected lower bound to be ";
        PrintTo(expected, result_listener->stream());
        *result_listener << " while actual is ";
        PrintTo(actual, result_listener->stream());
        *result_listener
            << " which is "
            << (result > 0 ? "greater" : "lesser")
            << " than expected";
    }

    return result == 0;
}

MATCHER_P(HasUpperBound, encodedUpperBound, "")
{
    auto expected = BuildKey(encodedUpperBound);
    auto actual = GetUpperBoundFromDataSplit(arg);

    auto result = TKeyComparer()(expected, actual);

    if (result != 0) {
        *result_listener << "expected upper bound to be ";
        PrintTo(expected, result_listener->stream());
        *result_listener << " while actual is ";
        PrintTo(actual, result_listener->stream());
        *result_listener
            << " which is "
            << (result > 0 ? "greater" : "lesser")
            << " than expected";
        return false;
    }

    return true;
}

TKeyColumns GetSampleKeyColumns()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    return keyColumns;
}

TTableSchema GetSampleTableSchema()
{
    TTableSchema tableSchema;
    tableSchema.Columns().push_back({ "k", EValueType::Integer });
    tableSchema.Columns().push_back({ "l", EValueType::Integer });
    tableSchema.Columns().push_back({ "m", EValueType::Integer });
    tableSchema.Columns().push_back({ "a", EValueType::Integer });
    tableSchema.Columns().push_back({ "b", EValueType::Integer });
    tableSchema.Columns().push_back({ "c", EValueType::Integer });
    return tableSchema;
}

template <class T>
TFuture<TErrorOr<T>> WrapInFuture(const T& value)
{
    return MakeFuture(TErrorOr<T>(value));
}

TDataSplit MakeSimpleSplit(const TYPath& path, ui64 counter = 0)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

    SetKeyColumns(&dataSplit, GetSampleKeyColumns());
    SetTableSchema(&dataSplit, GetSampleTableSchema());

    return dataSplit;
}

TFuture<TErrorOr<TDataSplit>> RaiseTableNotFound(const TYPath& path)
{
    return MakeFuture(TErrorOr<TDataSplit>(TError(Sprintf(
        "Could not find table %s",
        ~path))));
}

template <class TFunctor, class TMatcher>
void EXPECT_THROW_THAT(TFunctor functor, TMatcher matcher)
{
    bool exceptionThrown = false;
    try {
        functor();
    } catch (const std::exception& ex) {
        exceptionThrown = true;
        EXPECT_THAT(ex.what(), matcher);
    }
    EXPECT_TRUE(exceptionThrown);
}

class TQueryPrepareTest
    : public ::testing::Test
{
protected:
    template <class TMatcher>
    void ExpectPrepareThrowsWithDiagnostics(
        const Stroka& query,
        TMatcher matcher)
    {
        EXPECT_THROW_THAT(
            [&] { TPlanFragment::Prepare(query, &PrepareMock_); },
            matcher);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;

};

TEST_F(TQueryPrepareTest, Simple)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    TPlanFragment::Prepare("a, b FROM [//t] WHERE k > 3", &PrepareMock_);

    SUCCEED();
}

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectPrepareThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//bad/table"))
        .WillOnce(Invoke(&RaiseTableNotFound));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//bad/table]",
        HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnNameInProject)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Undefined reference \"foo\""));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Undefined reference \"bar\""));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where a > 3.1415926",
        ContainsRegex("Type mismatch .* in expression \"a > 3.1415926\""));
}

////////////////////////////////////////////////////////////////////////////////

class TQueryCoordinateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
            .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));
    }

    void Coordinate(const Stroka& source)
    {
        YCHECK(!Controller_);
        Controller_.Emplace(
            &CoordinateMock_,
            TPlanFragment::Prepare(source, &PrepareMock_));

        auto error = Controller_->Run();
        THROW_ERROR_EXCEPTION_IF_FAILED(error);

        CoordinatorFragment_ = Controller_->GetCoordinatorFragment();
        PeerFragments_ = Controller_->GetPeerFragments();
    }

    std::vector<const TDataSplit*> ExtractDataSplits(const TOperator* op)
    {
        std::vector<const TDataSplit*> result;
        Visit(op, [&result] (const TOperator* op) {
            if (auto* scanOp = op->As<TScanOperator>()) {
                result.emplace_back(&scanOp->DataSplit());
            }
        });
        return result;
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    StrictMock<TCoordinateCallbacksMock> CoordinateMock_;

    TNullable<TCoordinateController> Controller_;

    TNullable<TPlanFragment> CoordinatorFragment_;
    TNullable<std::vector<TPlanFragment>> PeerFragments_;

};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    std::vector<TDataSplit> emptySplit;

    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0)))
        .WillOnce(Return(WrapInFuture(emptySplit)));

    EXPECT_THROW_THAT(
        [&] { Coordinate("k from [//t]"); },
        ContainsRegex("Input [0-9a-f\\-]* is empty"));
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    std::vector<TDataSplit> singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0)))
        .WillOnce(Return(WrapInFuture(singleSplit)));
    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(1)))
        .WillOnce(Return(false));

    EXPECT_NO_THROW({ Coordinate("k from [//t]"); });
}

TEST(TKeyRangeTest, Unite)
{
    auto k1 = BuildKey("1"); auto k2 = BuildKey("2");
    auto k3 = BuildKey("3"); auto k4 = BuildKey("4");
    auto mp = [] (const TKey& a, const TKey& b) {
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
    auto k1 = BuildKey("1"); auto k2 = BuildKey("2");
    auto k3 = BuildKey("3"); auto k4 = BuildKey("4");
    auto mp = [] (const TKey& a, const TKey& b) {
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
    auto k1 = BuildKey("1"); auto k2 = BuildKey("2");
    auto mp = [] (const TKey& a, const TKey& b) {
        return std::make_pair(a, b);
    };

    EXPECT_TRUE(IsEmpty(mp(k1, k1)));
    EXPECT_TRUE(IsEmpty(mp(k2, k2)));

    EXPECT_TRUE(IsEmpty(mp(k2, k1)));
    EXPECT_FALSE(IsEmpty(mp(k1, k2)));

    EXPECT_TRUE(IsEmpty(mp(BuildKey("0;0;1"), BuildKey("0;0;0"))));
    EXPECT_FALSE(IsEmpty(mp(BuildKey("0;0;0"), BuildKey("0;0;1"))));
}

////////////////////////////////////////////////////////////////////////////////
// Refinement tests.

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

    TKey GetInitialLeftBound() const
    {
        return BuildKey(InitialLeftBoundAsYson);
    }

    TKey GetInitialRightBound() const
    {
        return BuildKey(InitialRightBoundAsYson);
    }

    TKey GetResultingLeftBound() const
    {
        return BuildKey(ResultingLeftBoundAsYson);
    }

    TKey GetResultingRightBound() const
    {
        return BuildKey(ResultingRightBoundAsYson);
    }
};

class TRefineKeyRangeTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TRefineKeyRangeTestCase>
{
protected:
    virtual void SetUp() override
    {
        Context_ = New<TPlanContext>();
    }

    void ExpectIsEmpty(const TKeyRange& keyRange)
    {
        EXPECT_TRUE(IsEmpty(keyRange))
            << "Left bound: " << ::testing::PrintToString(keyRange.first) << "; "
            << "Right bound: " << ::testing::PrintToString(keyRange.second);
    }

    template <class TTypedExpression, class... Args>
    const TTypedExpression* Make(Args&&... args)
    {
        return new (Context_.Get()) TTypedExpression(
            Context_.Get(),
            NullSourceLocation,
            std::forward<Args>(args)...);
    }

    TPlanContextPtr Context_;
    const TExpression* Predicate_;
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

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(
            testCase.GetInitialLeftBound(),
            testCase.GetInitialRightBound()),
        Make<TBinaryOpExpression>(testCase.ConstraintOpcode,
            Make<TReferenceExpression>(0, testCase.ConstraintColumnName),
            Make<TIntegerLiteralExpression>(testCase.ConstraintValue)));

    if (testCase.ResultIsEmpty) {
        ExpectIsEmpty(result);
    } else {
        EXPECT_EQ(testCase.GetResultingLeftBound(), result.first);
        EXPECT_EQ(testCase.GetResultingRightBound(), result.second);
    }
}

#define _MIN_ "<\"type\"=\"min\">#"
#define _MAX_ "<\"type\"=\"max\">#"

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
        false, ("50;" _MIN_ ";" _MIN_), ("50;" _MAX_ ";" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 1,
        false, ("1;1;1"), ("1;" _MAX_ ";" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 99,
        false, ("99;" _MIN_ ";" _MIN_), ("99;" _MAX_ ";" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 100,
        false, ("100;" _MIN_ ";" _MIN_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Equal, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_CASE_P(
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
        false, ("2;" _MIN_ ";" _MIN_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 100,
        false, ("1;1;1"), ("100;" _MIN_ ";" _MIN_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::NotEqual, 200,
        false, ("1;1;1"), ("100;100;100")
    },
};
INSTANTIATE_TEST_CASE_P(
    NotEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForNotEqualOpcodeInFirstComponent));

// Less, First component.
TRefineKeyRangeTestCase refineCasesForLessOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 50,
        false, ("1;1;1"), ("50;" _MIN_ ";" _MIN_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 1,
        true, (""), ("")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 100,
        false, ("1;1;1"), ("100;" _MIN_ ";" _MIN_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Less, 200,
        false, ("1;1;1"), ("100;100;100")
    },
};
INSTANTIATE_TEST_CASE_P(
    LessInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOpcodeInFirstComponent));

// LessOrEqual, First component.
TRefineKeyRangeTestCase refineCasesForLessOrEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 50,
        false, ("1;1;1"), ("50;" _MAX_ ";" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 1,
        false, ("1;1;1"), ("1;" _MAX_ ";" _MAX_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 99,
        false, ("1;1;1"), ("99;" _MAX_ ";" _MAX_)
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
INSTANTIATE_TEST_CASE_P(
    LessOrEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOrEqualOpcodeInFirstComponent));

// Greater, First component.
TRefineKeyRangeTestCase refineCasesForGreaterOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 50,
        false, ("51;" _MIN_ ";" _MIN_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 0,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::Greater, 1,
        false, ("2;" _MIN_ ";" _MIN_), ("100;100;100")
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
INSTANTIATE_TEST_CASE_P(
    GreaterInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOpcodeInFirstComponent));

// GreaterOrEqual, First component.
TRefineKeyRangeTestCase refineCasesForGreaterOrEqualOpcodeInFirstComponent[] = {
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 50,
        false, ("50;" _MIN_ ";" _MIN_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 1,
        false, ("1;1;1"), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 100,
        false, ("100;" _MIN_ ";" _MIN_), ("100;100;100")
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::GreaterOrEqual, 200,
        true, (""), ("")
    },
};
INSTANTIATE_TEST_CASE_P(
    GreaterOrEqualInFirstComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOrEqualOpcodeInFirstComponent));

////////////////////////////////////////////////////////////////////////////////

// Equal, Last component.
TRefineKeyRangeTestCase refineCasesForEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 50,
        false, ("1;1;50"), ("1;1;51")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 1,
        false, ("1;1;1"), ("1;1;2")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Equal, 99,
        false, ("1;1;99"), ("1;1;100")
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
INSTANTIATE_TEST_CASE_P(
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
        false, ("1;1;2"), ("1;1;100")
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
INSTANTIATE_TEST_CASE_P(
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
INSTANTIATE_TEST_CASE_P(
    LessInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOpcodeInLastComponent));

// LessOrEqual, Last component.
TRefineKeyRangeTestCase refineCasesForLessOrEqualOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 50,
        false, ("1;1;1"), ("1;1;51")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 1,
        false, ("1;1;1"), ("1;1;2")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::LessOrEqual, 99,
        false, ("1;1;1"), ("1;1;100")
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
INSTANTIATE_TEST_CASE_P(
    LessOrEqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForLessOrEqualOpcodeInLastComponent));

// Greater, Last component.
TRefineKeyRangeTestCase refineCasesForGreaterOpcodeInLastComponent[] = {
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 50,
        false, ("1;1;51"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 0,
        false, ("1;1;1"), ("1;1;100")
    },
    {
        ("1;1;1"), ("1;1;100"),
        "m", EBinaryOp::Greater, 1,
        false, ("1;1;2"), ("1;1;100")
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
INSTANTIATE_TEST_CASE_P(
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
INSTANTIATE_TEST_CASE_P(
    GreaterOrEqualInLastComponent,
    TRefineKeyRangeTest,
    ::testing::ValuesIn(refineCasesForGreaterOrEqualOpcodeInLastComponent));

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Test these cases:
//   - key is selecting a single point but restriction is sparse (k1 = 1; or
//     k1 = 1 & k2 = 1; or k2 = 2; (noop) k3 = 2; (noop))

TEST_F(TRefineKeyRangeTest, ContradictiveConjuncts)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(90));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(10));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, Lookup1)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(50));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "l"),
        Make<TIntegerLiteralExpression>(50));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    EXPECT_EQ(BuildKey("50;50;" _MIN_), result.first);
    EXPECT_EQ(BuildKey("50;50;" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, Lookup2)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(50));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "l"),
        Make<TIntegerLiteralExpression>(50));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "m"),
        Make<TIntegerLiteralExpression>(50));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And,
            Make<TBinaryOpExpression>(EBinaryOp::And,
                conj1, conj2), conj3));

    EXPECT_EQ(BuildKey("50;50;50"), result.first);
    EXPECT_EQ(BuildKey("50;50;51"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts1)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(10));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(90));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    EXPECT_EQ(BuildKey("10;" _MIN_ ";" _MIN_), result.first);
    EXPECT_EQ(BuildKey("90;" _MIN_ ";" _MIN_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts2)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(50));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>(0, "l"),
        Make<TIntegerLiteralExpression>(10));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>(0, "l"),
        Make<TIntegerLiteralExpression>(90));
    auto conj4 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "m"),
        Make<TIntegerLiteralExpression>(50));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And,
            Make<TBinaryOpExpression>(EBinaryOp::And,
                Make<TBinaryOpExpression>(EBinaryOp::And,
                    conj1, conj2), conj3), conj4));

    EXPECT_EQ(BuildKey("50;10;" _MIN_), result.first);
    EXPECT_EQ(BuildKey("50;90;" _MIN_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts3)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "k"),
        Make<TIntegerLiteralExpression>(50));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>(0, "m"),
        Make<TIntegerLiteralExpression>(50));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    EXPECT_EQ(BuildKey("50;" _MIN_ ";" _MIN_), result.first);
    EXPECT_EQ(BuildKey("50;" _MAX_ ";" _MAX_), result.second);
}

#undef _MIN_
#undef _MAX_

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryCoordinateTest, UsesKeyToPruneSplits)
{
    std::vector<TDataSplit> splits;

    splits.emplace_back(MakeSimpleSplit("//t", 1));
    SetLowerBound(&splits.back(), BuildKey("0;0;0"));
    SetUpperBound(&splits.back(), BuildKey("1;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 2));
    SetLowerBound(&splits.back(), BuildKey("1;0;0"));
    SetUpperBound(&splits.back(), BuildKey("2;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 3));
    SetLowerBound(&splits.back(), BuildKey("2;0;0"));
    SetUpperBound(&splits.back(), BuildKey("3;0;0"));

    EXPECT_CALL(CoordinateMock_, CanSplit(_))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true))
        .RetiresOnSaturation();
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0)))
        .WillOnce(Return(WrapInFuture(splits)));
    EXPECT_CALL(CoordinateMock_, Delegate(_, AllOf(
            HasCounter(2),
            HasLowerBound("1;2;3"),
            HasUpperBound("1;2;4"))))
        .WillOnce(Return(nullptr));

    EXPECT_NO_THROW({
        Coordinate("a from [//t] where k = 1 and l = 2 and m = 3");
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NQueryClient
} // namespace NYT
