#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/action_queue.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/query_client/plan_fragment.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/plan_node.h>
#include <ytlib/query_client/plan_helpers.h>
#include <ytlib/query_client/plan_visitor.h>
#include <ytlib/query_client/helpers.h>
#ifdef YT_USE_LLVM
#include <ytlib/query_client/cg_types.h>
#endif

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>

#include "versioned_table_client_ut.h"

#define _MIN_ "<\"type\"=\"min\">#"
#define _MAX_ "<\"type\"=\"max\">#"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TOwningKey& key, ::std::ostream* os)
{
    *os << KeyToYson(key.Get());
}

void PrintTo(const TKey& key, ::std::ostream* os)
{
    *os << KeyToYson(key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT


namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYPath;
using namespace NObjectClient;
using namespace NVersionedTableClient;

using ::testing::_;
using ::testing::StrictMock;
using ::testing::HasSubstr;
using ::testing::ContainsRegex;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::AllOf;

////////////////////////////////////////////////////////////////////////////////

class TPrepareCallbacksMock
    : public IPrepareCallbacks
{
public:
    MOCK_METHOD2(GetInitialSplit, TFuture<TErrorOr<TDataSplit>>(
        const TYPath&,
        TPlanContextPtr));
};

class TCoordinateCallbacksMock
    : public ICoordinateCallbacks
{
public:
    MOCK_METHOD2(GetReader, ISchemafulReaderPtr(
        const TDataSplit&,
        TPlanContextPtr));

    MOCK_METHOD1(CanSplit, bool(const TDataSplit&));

    MOCK_METHOD2(SplitFurther, TFuture<TErrorOr<TDataSplits>>(
        const TDataSplit&,
        TPlanContextPtr));

    MOCK_METHOD2(Regroup, TGroupedDataSplits(
        const TDataSplits&,
        TPlanContextPtr));

    MOCK_METHOD2(Delegate, std::pair<ISchemafulReaderPtr, TFuture<TErrorOr<TQueryStatistics>>>(
        const TPlanFragment&,
        const TDataSplit&));
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

MATCHER_P(HasSplitsCount, expectedCount, "")
{
    if (arg.size() != expectedCount) {
        *result_listener
            << "actual splits count is " << arg.size() << " while "
            << "expected count is " << expectedCount;
        return false;
    }

    return true;
}

MATCHER_P(HasLowerBound, encodedLowerBound, "")
{
    auto expected = BuildKey(encodedLowerBound);
    auto actual = GetLowerBoundFromDataSplit(arg);

    auto result = CompareRows(expected, actual);

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

    auto result = CompareRows(expected, actual);

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

TKeyColumns GetSampleKeyColumns2()
{
    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("l");
    keyColumns.push_back("m");
    keyColumns.push_back("s");
    return keyColumns;
}

TTableSchema GetSampleTableSchema()
{
    TTableSchema tableSchema;
    tableSchema.Columns().push_back({ "k", EValueType::Int64 });
    tableSchema.Columns().push_back({ "l", EValueType::Int64 });
    tableSchema.Columns().push_back({ "m", EValueType::Int64 });
    tableSchema.Columns().push_back({ "a", EValueType::Int64 });
    tableSchema.Columns().push_back({ "b", EValueType::Int64 });
    tableSchema.Columns().push_back({ "c", EValueType::Int64 });
    tableSchema.Columns().push_back({ "s", EValueType::String });
    tableSchema.Columns().push_back({ "u", EValueType::String });
    return tableSchema;
}

template <class T>
TFuture<TErrorOr<T>> WrapInFuture(const T& value)
{
    return MakeFuture(TErrorOr<T>(value));
}

TFuture<TErrorOr<void>> WrapVoidInFuture()
{
    return MakeFuture(TErrorOr<void>());
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

TDataSplit MakeSplit(const std::vector<TColumnSchema>& columns)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

    TKeyColumns keyColumns;
    SetKeyColumns(&dataSplit, keyColumns);

    TTableSchema tableSchema;
    tableSchema.Columns() = columns;
    SetTableSchema(&dataSplit, tableSchema);

    return dataSplit;
}

TFuture<TErrorOr<TDataSplit>> RaiseTableNotFound(
    const TYPath& path,
    TPlanContextPtr)
{
    return MakeFuture(TErrorOr<TDataSplit>(TError(Format(
        "Could not find table %v",
        path))));
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
            [&] { TPlanFragment::Prepare(&PrepareMock_, query); },
            matcher);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;

};

TEST_F(TQueryPrepareTest, Simple)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    TPlanFragment::Prepare(&PrepareMock_, "a, b FROM [//t] WHERE k > 3");
}

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectPrepareThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//bad/table", _))
        .WillOnce(Invoke(&RaiseTableNotFound));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//bad/table]",
        HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnNameInProject)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Undefined reference \"foo\""));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Undefined reference \"bar\""));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where a > 3.1415926",
        ContainsRegex("Type mismatch .* in expression \"a > 3.1415926\""));
}

TEST_F(TQueryPrepareTest, TooBigQuery)
{
    Stroka query = "k from [//t] where a in (0";
    for (int i = 1; i < 50 ; ++i) {
        query += ", " + ToString(i);
    }
    query += ")";

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Plan fragment depth limit exceeded"));
}

TEST_F(TQueryPrepareTest, ResultSchemaCollision)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "a as x, b as x FROM [//t] WHERE k > 3",
        ContainsRegex("Redefinition of column .*"));
}

////////////////////////////////////////////////////////////////////////////////

class TQueryCoordinateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
            .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));
    }

    void Coordinate(const Stroka& source)
    {
        YCHECK(!Coordinator_);
        Coordinator_.Emplace(
            &CoordinateMock_,
            TPlanFragment::Prepare(&PrepareMock_, source));

        Coordinator_->Run();
        CoordinatorFragment_ = Coordinator_->GetCoordinatorFragment();
        PeerFragments_ = Coordinator_->GetPeerFragments();
    }

    std::vector<const TDataSplit*> ExtractDataSplits(const TOperator* op)
    {
        std::vector<const TDataSplit*> result;
        Visit(op, [&result] (const TOperator* op) {
            if (auto* scanOp = op->As<TScanOperator>()) {
                for (const auto& dataSplit : scanOp->DataSplits()) {
                    result.emplace_back(&dataSplit);
                }
            }
        });
        return result;
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    StrictMock<TCoordinateCallbacksMock> CoordinateMock_;

    TNullable<TCoordinator> Coordinator_;

    TNullable<TPlanFragment> CoordinatorFragment_;
    TNullable<std::vector<TPlanFragment>> PeerFragments_;

};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    TDataSplits emptySplits;
    TGroupedDataSplits emptyGroupedSplits;

    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0), _))
        .WillOnce(Return(WrapInFuture(emptySplits)));

    EXPECT_NO_THROW({
        Coordinate("k from [//t]");
    });
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    TDataSplits singleSplit;
    TGroupedDataSplits singleGroupedSplit;

    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));
    singleGroupedSplit.push_back(singleSplit);

    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0), _))
        .WillOnce(Return(WrapInFuture(singleSplit)));
    EXPECT_CALL(CoordinateMock_, Regroup(HasSplitsCount(1), _))
        .WillOnce(Return(singleGroupedSplit));
    EXPECT_CALL(CoordinateMock_, Delegate(_, HasCounter(1)))
        .WillOnce(Return(std::make_pair(nullptr, TFuture<TErrorOr<NQueryClient::TQueryStatistics>>())));

    EXPECT_NO_THROW({
        Coordinate("k from [//t]");
    });
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

    template <class TTypedExpression, class... TArgs>
    const TTypedExpression* Make(TArgs&&... args)
    {
        return new (Context_.Get()) TTypedExpression(
            Context_.Get(),
            NullSourceLocation,
            std::forward<TArgs>(args)...);
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
            Make<TReferenceExpression>(testCase.ConstraintColumnName),
            Make<TLiteralExpression>(testCase.ConstraintValue)));

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
        false, ("1;1;1"), ("51;" _MIN_ ";" _MIN_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 1,
        false, ("1;1;1"), ("2;" _MIN_ ";" _MIN_)
    },
    {
        ("1;1;1"), ("100;100;100"),
        "k", EBinaryOp::LessOrEqual, 99,
        false, ("1;1;1"), ("100;" _MIN_ ";" _MIN_)
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

TEST_F(TRefineKeyRangeTest, ContradictiveConjuncts)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(90)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(10)));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    ExpectIsEmpty(result);
}

TEST_F(TRefineKeyRangeTest, Lookup1)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(50)));

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
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(50)));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And,
            Make<TBinaryOpExpression>(EBinaryOp::And,
                conj1, conj2), conj3));

    EXPECT_EQ(BuildKey("50;50;50"), result.first);
    EXPECT_EQ(BuildKey("50;50;51"), result.second);
}

TEST_F(TRefineKeyRangeTest, Range1)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Greater,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(0)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(100)));

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    auto result = RefineKeyRange(
        keyColumns,
        std::make_pair(BuildKey(""), BuildKey("1000000000")),
        Make<TBinaryOpExpression>(EBinaryOp::And,
                conj1, conj2));

    EXPECT_EQ(BuildKey("1"), result.first);
    EXPECT_EQ(BuildKey("100"), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleConjuncts1)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(10)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(90)));

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
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::GreaterOrEqual,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(10)));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(90)));
    auto conj4 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

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
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2));

    EXPECT_EQ(BuildKey("50;" _MIN_ ";" _MIN_), result.first);
    EXPECT_EQ(BuildKey("50;" _MAX_ ";" _MAX_), result.second);
}

TEST_F(TRefineKeyRangeTest, MultipleDisjuncts)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2);

    auto conj4 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(75)));
    auto conj5 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

    auto conj6 = Make<TBinaryOpExpression>(EBinaryOp::And, conj4, conj5);

    TRowBuffer rowBuffer;

    auto keyColumns = GetSampleKeyColumns();

    auto keyTrie = ExtractMultipleConstraints(
        Make<TBinaryOpExpression>(EBinaryOp::Or, conj3, conj6),
        keyColumns,
        &rowBuffer);

    std::vector<TKeyRange> result = GetRangesFromTrieWithinRange(
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        &rowBuffer,
        keyColumns.size(),
        keyTrie);

    EXPECT_EQ(result.size(), 2);

    EXPECT_EQ(BuildKey("50;" _MIN_ ";" _MIN_), result[0].first);
    EXPECT_EQ(BuildKey("50;" _MAX_ ";" _MAX_), result[0].second);

    EXPECT_EQ(BuildKey("75;" _MIN_ ";" _MIN_), result[1].first);
    EXPECT_EQ(BuildKey("75;" _MAX_ ";" _MAX_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, NotEqualToMultipleRanges)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::NotEqual,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(50)));

    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Greater,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(40)));

    auto conj4 = Make<TBinaryOpExpression>(EBinaryOp::Less,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(60)));

    auto conj5 = Make<TBinaryOpExpression>(
        EBinaryOp::And,
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, conj2),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj3, conj4));

    TRowBuffer rowBuffer;

    auto keyColumns = GetSampleKeyColumns();

    auto keyTrie = ExtractMultipleConstraints(
        conj5,
        keyColumns,
        &rowBuffer);

    std::vector<TKeyRange> result = GetRangesFromTrieWithinRange(
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        &rowBuffer,
        keyColumns.size(),
        keyTrie);

    EXPECT_EQ(result.size(), 2);

    EXPECT_EQ(BuildKey("50;41;" _MIN_), result[0].first);
    EXPECT_EQ(BuildKey("50;50;" _MIN_), result[0].second);

    EXPECT_EQ(BuildKey("50;51;" _MIN_), result[1].first);
    EXPECT_EQ(BuildKey("50;60;" _MIN_), result[1].second);
}

TEST_F(TRefineKeyRangeTest, RangesProduct)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(40)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(60)));

    auto disj1 = Make<TBinaryOpExpression>(
        EBinaryOp::Or,
        Make<TBinaryOpExpression>(EBinaryOp::Or, conj1, conj2),
        conj3);

    auto conj4 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(40)));
    auto conj5 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(50)));
    auto conj6 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(60)));

    auto disj2 = Make<TBinaryOpExpression>(
        EBinaryOp::Or,
        Make<TBinaryOpExpression>(EBinaryOp::Or, conj4, conj5),
        conj6);

    TRowBuffer rowBuffer;

    auto keyColumns = GetSampleKeyColumns();

    auto keyTrie = ExtractMultipleConstraints(
        Make<TBinaryOpExpression>(EBinaryOp::And, disj1, disj2),
        keyColumns,
        &rowBuffer);

    std::vector<TKeyRange> result = GetRangesFromTrieWithinRange(
        std::make_pair(BuildKey("1;1;1"), BuildKey("100;100;100")),
        &rowBuffer,
        keyColumns.size(),
        keyTrie);

    EXPECT_EQ(result.size(), 9);

    EXPECT_EQ(BuildKey("40;40;" _MIN_), result[0].first);
    EXPECT_EQ(BuildKey("40;40;" _MAX_), result[0].second);

    EXPECT_EQ(BuildKey("40;50;" _MIN_), result[1].first);
    EXPECT_EQ(BuildKey("40;50;" _MAX_), result[1].second);

    EXPECT_EQ(BuildKey("40;60;" _MIN_), result[2].first);
    EXPECT_EQ(BuildKey("40;60;" _MAX_), result[2].second);

    EXPECT_EQ(BuildKey("50;40;" _MIN_), result[3].first);
    EXPECT_EQ(BuildKey("50;40;" _MAX_), result[3].second);

    EXPECT_EQ(BuildKey("50;50;" _MIN_), result[4].first);
    EXPECT_EQ(BuildKey("50;50;" _MAX_), result[4].second);

    EXPECT_EQ(BuildKey("50;60;" _MIN_), result[5].first);
    EXPECT_EQ(BuildKey("50;60;" _MAX_), result[5].second);

    EXPECT_EQ(BuildKey("60;40;" _MIN_), result[6].first);
    EXPECT_EQ(BuildKey("60;40;" _MAX_), result[6].second);

    EXPECT_EQ(BuildKey("60;50;" _MIN_), result[7].first);
    EXPECT_EQ(BuildKey("60;50;" _MAX_), result[7].second);

    EXPECT_EQ(BuildKey("60;60;" _MIN_), result[8].first);
    EXPECT_EQ(BuildKey("60;60;" _MAX_), result[8].second);
}

TEST_F(TRefineKeyRangeTest, NormalizeShortKeys)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(1)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(2)));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(3)));

    auto result = RefineKeyRange(
        GetSampleKeyColumns(),
        std::make_pair(BuildKey("1"), BuildKey("2")),
        Make<TBinaryOpExpression>(EBinaryOp::And,
            Make<TBinaryOpExpression>(EBinaryOp::And,
                conj1, conj2), conj3));

    EXPECT_EQ(BuildKey("1;2;3"), result.first);
    EXPECT_EQ(BuildKey("1;2;4"), result.second);
}

TEST_F(TRefineKeyRangeTest, LookupIsPrefix)
{
    auto conj1 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("k"),
        Make<TLiteralExpression>(i64(50)));
    auto conj2 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("l"),
        Make<TLiteralExpression>(i64(50)));
    auto conj3 = Make<TBinaryOpExpression>(EBinaryOp::Equal,
        Make<TReferenceExpression>("m"),
        Make<TLiteralExpression>(i64(50)));

    auto conj4 = Make<TFunctionExpression>("is_prefix",
        Make<TLiteralExpression>("abc"),
        Make<TReferenceExpression>("s"));

    auto result = RefineKeyRange(
        GetSampleKeyColumns2(),
        std::make_pair(BuildKey("1;1;1;aaaa"), BuildKey("100;100;100;bbbbb")),
        Make<TBinaryOpExpression>(EBinaryOp::And, conj1, 
            Make<TBinaryOpExpression>(EBinaryOp::And, conj2, 
                Make<TBinaryOpExpression>(EBinaryOp::And, conj3, conj4))));

    EXPECT_EQ(BuildKey("50;50;50;abc"), result.first);
    EXPECT_EQ(BuildKey("50;50;50;abd"), result.second);
}
////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryCoordinateTest, UsesKeyToPruneSplits)
{
    TDataSplits splits;
    TGroupedDataSplits groupedSplits;

    splits.emplace_back(MakeSimpleSplit("//t", 1));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("0;0;0"));
    SetUpperBound(&splits.back(), BuildKey("1;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 2));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("1;0;0"));
    SetUpperBound(&splits.back(), BuildKey("2;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 3));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("2;0;0"));
    SetUpperBound(&splits.back(), BuildKey("3;0;0"));

    groupedSplits.push_back(splits);

    EXPECT_CALL(CoordinateMock_, CanSplit(_))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(CoordinateMock_, CanSplit(HasCounter(0)))
        .WillOnce(Return(true))
        .RetiresOnSaturation();
    EXPECT_CALL(CoordinateMock_, SplitFurther(HasCounter(0), _))
        .WillOnce(Return(WrapInFuture(splits)));
    EXPECT_CALL(CoordinateMock_, Regroup(HasSplitsCount(1), _))
        .WillOnce(Return(groupedSplits));
    EXPECT_CALL(CoordinateMock_, Delegate(_, HasCounter(1)))
        .WillOnce(Return(std::make_pair(nullptr, NYT::TFuture<NYT::TErrorOr<NYT::NQueryClient::TQueryStatistics>>())));

    EXPECT_NO_THROW({
        Coordinate("a from [//t] where k = 1 and l = 2 and m = 3");
    });
}

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_LLVM

class TEvaluateCallbacksMock
    : public IEvaluateCallbacks
{
public:
    MOCK_METHOD2(GetReader, ISchemafulReaderPtr(const TDataSplit&, TPlanContextPtr));

};

class TReaderMock
    : public ISchemafulReader
{
public:
    MOCK_METHOD1(Open, TAsyncError(const TTableSchema&));
    MOCK_METHOD1(Read, bool(std::vector<TUnversionedRow>*));
    MOCK_METHOD0(GetReadyEvent, TAsyncError());
};

class TWriterMock
    : public ISchemafulWriter
{
public:
    MOCK_METHOD2(Open, TAsyncError(const TTableSchema&, const TNullable<TKeyColumns>&));
    MOCK_METHOD0(Close, TAsyncError());
    MOCK_METHOD1(Write, bool(const std::vector<TUnversionedRow>&));
    MOCK_METHOD0(GetReadyEvent, TAsyncError());
};

TUnversionedOwningRow BuildRow(
    const Stroka& yson,
    TDataSplit& dataSplit,
    bool treatMissingAsNull = true)
{
    auto keyColumns = GetKeyColumnsFromDataSplit(dataSplit);
    auto tableSchema = GetTableSchemaFromDataSplit(dataSplit);
    auto nameTable = NVersionedTableClient::TNameTable::FromSchema(tableSchema);

    auto rowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(
        TYsonString(yson, EYsonType::MapFragment));

    TUnversionedOwningRowBuilder rowBuilder;
    auto addValue = [&] (int id, INodePtr value) {
        switch (value->GetType()) {
            case ENodeType::Int64:
                rowBuilder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                break;
            case ENodeType::Double:
                rowBuilder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                break;
            case ENodeType::String:
                rowBuilder.AddValue(MakeUnversionedStringValue(value->GetValue<Stroka>(), id));
                break;
            default:
                rowBuilder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).Data(), id));
                break;
        }
    };

    // Key
    for (int id = 0; id < static_cast<int>(keyColumns.size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        }
    }

    // Fixed values
    for (int id = static_cast<int>(keyColumns.size()); id < static_cast<int>(tableSchema.Columns().size()); ++id) {
        auto it = rowParts.find(nameTable->GetName(id));
        if (it != rowParts.end()) {
            addValue(id, it->second);
        } else if (treatMissingAsNull) {
            rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
        }
    }

    // Variable values
    for (const auto& pair : rowParts) {
        int id = nameTable->GetIdOrRegisterName(pair.first);
        if (id >= tableSchema.Columns().size()) {
            addValue(id, pair.second);
        }
    }

    return rowBuilder.GetRowAndReset();
}

class TQueryEvaluateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
            .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

        ReaderMock_ = New<StrictMock<TReaderMock>>();
        WriterMock_ = New<StrictMock<TWriterMock>>();

        ActionQueue_ = New<TActionQueue>("Test");
    }

    virtual void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    void Evaluate(
        const Stroka& query,
        const std::vector<TUnversionedOwningRow>& owningSource,
        const std::vector<TUnversionedOwningRow>& owningResult,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max())
    {
        auto result = BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .Guarded()
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                owningSource,
                owningResult,
                inputRowLimit,
                outputRowLimit)
            .Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

    void DoEvaluate(
        const Stroka& query,
        const std::vector<TUnversionedOwningRow>& owningSource,
        const std::vector<TUnversionedOwningRow>& owningResult,
        i64 inputRowLimit,
        i64 outputRowLimit)
    {
        std::vector<TRow> source(owningSource.size());
        std::vector<std::vector<TRow>> results;
        typedef const TRow(TUnversionedOwningRow::*TGetFunction)() const;

        std::transform(
            owningSource.begin(),
            owningSource.end(),
            source.begin(),
            std::mem_fn(TGetFunction(&TUnversionedOwningRow::Get)));

        for (auto iter = owningResult.begin(), end = owningResult.end(); iter != end;) {
            size_t writeSize = std::min(static_cast<int>(end - iter), NQueryClient::MaxRowsPerWrite);
            std::vector<TRow> result(writeSize);

            std::transform(
                iter,
                iter + writeSize,
                result.begin(),
                std::mem_fn(TGetFunction(&TUnversionedOwningRow::Get)));

            results.push_back(result);

            iter += writeSize;
        }
        
        EXPECT_CALL(EvaluateMock_, GetReader(_, _))
            .WillOnce(Return(ReaderMock_));

        EXPECT_CALL(*ReaderMock_, Open(_))
            .WillOnce(Return(WrapVoidInFuture()));

        EXPECT_CALL(*ReaderMock_, Read(_))
            .WillOnce(DoAll(SetArgPointee<0>(source), Return(false)));

        {
            testing::InSequence s;

            EXPECT_CALL(*WriterMock_, Open(_, _))
                .WillOnce(Return(WrapVoidInFuture()));
            for (auto& result : results) {
                EXPECT_CALL(*WriterMock_, Write(result))
                    .WillOnce(Return(true));
            }
            
            EXPECT_CALL(*WriterMock_, Close())
                .WillOnce(Return(WrapVoidInFuture()));
        }

        TEvaluator evaluator;
        evaluator.Run(
            &EvaluateMock_,
            TPlanFragment::Prepare(&PrepareMock_, query, inputRowLimit, outputRowLimit),
            WriterMock_);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    StrictMock<TEvaluateCallbacksMock> EvaluateMock_;
    TIntrusivePtr<StrictMock<TReaderMock>> ReaderMock_; 
    TIntrusivePtr<StrictMock<TWriterMock>> WriterMock_;
    TActionQueuePtr ActionQueue_;

};

TEST_F(TQueryEvaluateTest, Simple)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("a=4;b=5", simpleSplit, false));
    source.push_back(BuildRow("a=10;b=11", simpleSplit, false));

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=4;b=5", simpleSplit, false));
    result.push_back(BuildRow("a=10;b=11", simpleSplit, false));

    Evaluate("a, b FROM [//t]", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleBetweenAnd)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("a=4;b=5", simpleSplit, false));
    source.push_back(BuildRow("a=10;b=11", simpleSplit, false));
    source.push_back(BuildRow("a=15;b=11", simpleSplit, false));

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=10;b=11", simpleSplit, false));

    Evaluate("a, b FROM [//t] where a between 9 and 11", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleIn)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("a=4;b=5", simpleSplit, false));
    source.push_back(BuildRow("a=10;b=11", simpleSplit, false));
    source.push_back(BuildRow("a=15;b=11", simpleSplit, false));

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=4;b=5", simpleSplit, false));
    result.push_back(BuildRow("a=10;b=11", simpleSplit, false));

    Evaluate("a, b FROM [//t] where a in (4, 10)", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleWithNull)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("a=4;b=5", simpleSplit, true));
    source.push_back(BuildRow("a=10;b=11;c=9", simpleSplit, true));
    source.push_back(BuildRow("a=16", simpleSplit, true));

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=4;b=5", simpleSplit, true));
    result.push_back(BuildRow("a=10;b=11;c=9", simpleSplit, true));
    result.push_back(BuildRow("a=16", simpleSplit, true));

    Evaluate("a, b, c FROM [//t] where a > 3", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleWithNull2)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("a=1;b=2;c=3", simpleSplit, true));
    source.push_back(BuildRow("a=4", simpleSplit, true));
    source.push_back(BuildRow("a=5;b=5", simpleSplit, true));
    source.push_back(BuildRow("a=7;c=8", simpleSplit, true));
    source.push_back(BuildRow("a=10;b=1", simpleSplit, true));
    source.push_back(BuildRow("a=10;c=1", simpleSplit, true));

    std::vector<TColumnSchema> resultColumns;
    resultColumns.emplace_back("a", EValueType::Int64);
    resultColumns.emplace_back("x", EValueType::Int64);
    auto resultSplit = MakeSplit(resultColumns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=1;x=5", resultSplit, true));
    result.push_back(BuildRow("a=4;", resultSplit, true));
    result.push_back(BuildRow("a=5;", resultSplit, true));
    result.push_back(BuildRow("a=7;", resultSplit, true));

    Evaluate("a, b + c as x FROM [//t] where a < 10", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleStrings)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("s", EValueType::String);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("s=foo", simpleSplit, true));
    source.push_back(BuildRow("s=bar", simpleSplit, true));
    source.push_back(BuildRow("s=baz", simpleSplit, true));

    auto resultSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("s=foo", resultSplit, true));
    result.push_back(BuildRow("s=bar", resultSplit, true));
    result.push_back(BuildRow("s=baz", resultSplit, true));

    Evaluate("s FROM [//t]", source, result);
}

TEST_F(TQueryEvaluateTest, SimpleStrings2)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("s", EValueType::String);
    columns.emplace_back("u", EValueType::String);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("s=foo; u=x", simpleSplit, true));
    source.push_back(BuildRow("s=bar; u=y", simpleSplit, true));
    source.push_back(BuildRow("s=baz; u=x", simpleSplit, true));
    source.push_back(BuildRow("s=olala; u=z", simpleSplit, true));

    auto resultSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("s=foo; u=x", resultSplit, true));
    result.push_back(BuildRow("s=baz; u=x", resultSplit, true));

    Evaluate("s, u FROM [//t] where u = \"x\"", source, result);
}

TEST_F(TQueryEvaluateTest, HasPrefixStrings)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("s", EValueType::String);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    source.push_back(BuildRow("s=foobar", simpleSplit, true));
    source.push_back(BuildRow("s=bar", simpleSplit, true));
    source.push_back(BuildRow("s=baz", simpleSplit, true));

    auto resultSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("s=foobar", resultSplit, true));

    Evaluate("s FROM [//t] where is_prefix(\"foo\", s)", source, result);
}

TEST_F(TQueryEvaluateTest, Complex)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, false));
    }

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("x=0;t=200", simpleSplit, false));
    result.push_back(BuildRow("x=1;t=241", simpleSplit, false));

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x", source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex2)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, false));
    }

    std::vector<TColumnSchema> resultColumns;
    resultColumns.emplace_back("x", EValueType::Int64);
    resultColumns.emplace_back("q", EValueType::Int64);
    resultColumns.emplace_back("t", EValueType::Int64);
    auto resultSplit = MakeSplit(resultColumns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("x=0;q=0;t=200", resultSplit, false));
    result.push_back(BuildRow("x=1;q=0;t=241", resultSplit, false));

    Evaluate("x, q, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x, 0 as q", source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexBigResult)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    std::vector<TUnversionedOwningRow> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(BuildRow(Stroka() + "a=" + ToString(i) + ";b=" + ToString(i * 10), simpleSplit, false));
    }

    std::vector<TUnversionedOwningRow> result;

    for (size_t i = 2; i < 10000; ++i) {
        result.push_back(BuildRow(Stroka() + "x=" + ToString(i) + ";t=" + ToString(i * 10 + i), simpleSplit, false));
    }

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a as x", source, result);
}

TEST_F(TQueryEvaluateTest, ComplexWithNull)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, true));
    }

    std::vector<TColumnSchema> resultColumns;
    resultColumns.emplace_back("x", EValueType::Int64);
    resultColumns.emplace_back("t", EValueType::Int64);
    resultColumns.emplace_back("y", EValueType::Int64);
    auto resultSplit = MakeSplit(resultColumns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("x=1;t=251;y=250", resultSplit, true));
    result.push_back(BuildRow("x=0;t=200;y=200", resultSplit, true));
    result.push_back(BuildRow("y=6", resultSplit, true));

    Evaluate("x, sum(b) + x as t, sum(b) as y FROM [//t] group by a % 2 as x", source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexStrings)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("s", EValueType::String);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=10;s=x",
        "a=20;s=y",
        "a=30;s=x",
        "a=40;s=x",
        "a=42",
        "a=50;s=x",
        "a=60;s=y",
        "a=70;s=z",
        "a=72",
        "a=80;s=y",
        "a=85",
        "a=90;s=z"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, true));
    }

    std::vector<TColumnSchema> resultColumns;
    resultColumns.emplace_back("x", EValueType::String);
    resultColumns.emplace_back("t", EValueType::Int64);
    auto resultSplit = MakeSplit(resultColumns);

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("x=y;t=160", resultSplit, true));
    result.push_back(BuildRow("x=x;t=120", resultSplit, true));
    result.push_back(BuildRow("t=199", resultSplit, true));
    result.push_back(BuildRow("x=z;t=160", resultSplit, true));

    Evaluate("x, sum(b) as t FROM [//t] where b > 10 group by s as x", source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestIf)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, false));
    }

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("x=b;t=250", simpleSplit, false));
    result.push_back(BuildRow("x=a;t=200", simpleSplit, false));
    
    Evaluate("if(x = 4, \"a\", \"b\") as x, sum(b) as t FROM [//t] group by if(a % 2 = 0, 4, 5) as x", source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestInputRowLimit)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, false));
    }

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=2;b=20", simpleSplit, false));
    result.push_back(BuildRow("a=3;b=30", simpleSplit, false));

    Evaluate("a, b FROM [//t] where a > 1 and a < 9", source, result, 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOutputRowLimit)
{
    std::vector<TColumnSchema> columns;
    columns.emplace_back("a", EValueType::Int64);
    columns.emplace_back("b", EValueType::Int64);
    columns.emplace_back("c", EValueType::Int64);
    auto simpleSplit = MakeSplit(columns);

    const char* sourceRowsData[] = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    std::vector<TUnversionedOwningRow> source;
    for (auto row : sourceRowsData) {
        source.push_back(BuildRow(row, simpleSplit, false));
    }

    std::vector<TUnversionedOwningRow> result;
    result.push_back(BuildRow("a=2;b=20", simpleSplit, false));
    result.push_back(BuildRow("a=3;b=30", simpleSplit, false));
    result.push_back(BuildRow("a=4;b=40", simpleSplit, false));

    Evaluate("a, b FROM [//t] where a > 1 and a < 9", source, result, std::numeric_limits<i64>::max(), 3);

    SUCCEED();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT
