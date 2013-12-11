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

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/chunk_writer.h>

namespace NYT {
namespace NQueryClient {
namespace {

using namespace NYPath;
using namespace NObjectClient;

using ::testing::_;
using ::testing::StrictMock;
using ::testing::HasSubstr;
using ::testing::ContainsRegex;
using ::testing::Invoke;
using ::testing::Return;

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

MATCHER_P(DataSplitWithCounter, expectedCounterId, "")
{
    auto objectId = NQueryClient::GetObjectIdFromDataSplit(arg);
    auto cellId = NObjectClient::CellIdFromId(objectId);
    auto counterId = NObjectClient::CounterFromId(objectId);

    if (cellId != 0x42) {
        *result_listener << "cell id is bad";
        return false;
    }

    if (counterId != expectedCounterId) {
        *result_listener
            << "actual counter id is " << counterId << " while"
            << "expected counter id is " << expectedCounterId;
        return false;
    }

    return true;
}

template <class T>
TFuture<TErrorOr<T>> Wrap(T&& value)
{
    return MakeFuture(TErrorOr<typename std::decay<T>::type>(std::forward<T>(value)));
}

TDataSplit MakeSimpleSplit(const TYPath& path, ui64 counter = 0)
{
    TDataSplit dataSplit;

    ToProto(
        dataSplit.mutable_chunk_id(),
        MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

    TKeyColumns keyColumns;
    keyColumns.push_back("k");
    keyColumns.push_back("n");
    SetKeyColumns(&dataSplit, keyColumns);

    TTableSchema tableSchema;
    tableSchema.Columns().push_back({ "k", EValueType::Integer });
    tableSchema.Columns().push_back({ "n", EValueType::Integer });
    tableSchema.Columns().push_back({ "a", EValueType::Integer });
    tableSchema.Columns().push_back({ "b", EValueType::Integer });
    SetTableSchema(&dataSplit, tableSchema);

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
        .WillOnce(Return(Wrap(MakeSimpleSplit("//t"))));
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
        .WillOnce(Return(Wrap(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Table //t does not have column \"foo\" in its schema"));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(Wrap(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Table //t does not have column \"bar\" in its schema"));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t"))
        .WillOnce(Return(Wrap(MakeSimpleSplit("//t"))));

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
            .WillOnce(Return(Wrap(MakeSimpleSplit("//t"))));
    }

    void Coordinate(const Stroka& source)
    {
        TCoordinateController controller(
            &CoordinateMock_,
            TPlanFragment::Prepare(source, &PrepareMock_));

        controller.Run();

        // Here we heavily rely on fact that coordinator does not change context.
        CoordinatorPlan_ = controller.GetCoordinatorSplit();
        PeerPlans_ = controller.GetPeerSplits();
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    StrictMock<TCoordinateCallbacksMock> CoordinateMock_;

    TNullable<TPlanFragment> CoordinatorPlan_;
    TNullable<std::vector<TPlanFragment>> PeerPlans_;

};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    auto noSplits = Wrap(std::vector<TDataSplit>());
    EXPECT_CALL(CoordinateMock_, CanSplit(DataSplitWithCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(DataSplitWithCounter(0)))
        .WillOnce(Return(noSplits));

    EXPECT_THROW_THAT(
        [&] { Coordinate("k from [//t]"); },
        ContainsRegex("Input [0-9a-f\\-]* is empty"));
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    auto singleSplit = Wrap(std::vector<TDataSplit>(1, MakeSimpleSplit("//t", 1)));
    EXPECT_CALL(CoordinateMock_, CanSplit(DataSplitWithCounter(0)))
        .WillOnce(Return(true));
    EXPECT_CALL(CoordinateMock_, SplitFurther(DataSplitWithCounter(0)))
        .WillOnce(Return(singleSplit));
    EXPECT_CALL(CoordinateMock_, CanSplit(DataSplitWithCounter(1)))
        .WillOnce(Return(false));

    EXPECT_NO_THROW({ Coordinate("k from [//t]"); });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT
