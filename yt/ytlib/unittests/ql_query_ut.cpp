#include <yt/core/test_framework/framework.h>
#include "ql_helpers.h"
#include "udf/invalid_ir.h"

#ifdef YT_IN_ARCADIA
#include <library/resource/resource.h>
#else
#include "udf/test_udfs.h" // Y_IGNORE
#endif

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/config.h>
#include <yt/ytlib/query_client/coordinator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/folding_profiler.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query.pb.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions.h>
#include <yt/ytlib/query_client/functions_cg.h>
#include <yt/ytlib/query_client/functions_builder.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/pipe.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/collection_helpers.h>

#include <yt/core/ytree/convert.h>

#include <tuple>

// Tests:
// TQueryPrepareTest
// TJobQueryPrepareTest
// TQueryCoordinateTest
// TQueryEvaluateTest

namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TDataStatistics;

////////////////////////////////////////////////////////////////////////////////

class TQueryPrepareTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("PrepareTest");
    }

    virtual void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    template <class TMatcher>
    void ExpectPrepareThrowsWithDiagnostics(
        const TString& query,
        TMatcher matcher)
    {
        EXPECT_THROW_THAT(
            [&] {
                BIND([&] () {
                    PreparePlanFragment(&PrepareMock_, query);
                })
                .AsyncVia(ActionQueue_->GetInvoker())
                .Run()
                .Get()
                .ThrowOnError();
            },
            matcher);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TActionQueuePtr ActionQueue_;

};

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectPrepareThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadWhere)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "* from [//t] where (a = 1, b = 2)",
        HasSubstr("Expecting scalar expression"));
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
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Undefined reference \"foo\""));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Undefined reference \"bar\""));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where a > \"xyz\"",
        ContainsRegex("Type mismatch in expression"));
}

TEST_F(TQueryPrepareTest, TooBigQuery)
{
    TString query = "k from [//t] where a ";
    for (int i = 0; i < 50; ++i) {
        query += "+ " + ToString(i);
    }
    query += " > 0";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Maximum expression depth exceeded"));
}

TEST_F(TQueryPrepareTest, TooBigQuery2)
{
    TString query =
    R"(
        * from [//t]
        where
        (a = 3735 and s = 'd0b160b8-1d27-40ad-8cad-1b69c2187195') or
        (a = 4193 and s = 'd2c05a2c-fdee-4417-b2dd-cf58a51a6db2') or
        (a = 4365 and s = '07d1c433-3a42-473a-9d8f-45f21001dbe5') or
        (a = 4363 and s = 'f0168ae8-8f75-4dee-b113-510c43f2df30') or
        (a = 4346 and s = '4f1cf6d9-999b-4d9e-95c0-55b45ec96573') or
        (a = 4334 and s = '9db5716a-89ef-4a0b-bfe3-f0785f31f5f9') or
        (a = 4340 and s = '8b2b0701-8bd2-44ca-b90b-a9186acadcaf') or
        (a = 4329 and s = 'f0dbede6-ca7f-4e31-9e5e-2de9eb0dd672') or
        (a = 4211 and s = '4b10bebc-8633-4a2c-bb23-05abac483c54') or
        (a = 4351 and s = '64517493-57d5-4f1b-9fdb-393105b3fce7') or
        (a = 4321 and s = '4054e543-6119-4146-8922-3c026109eada') or
        (a = 4204 and s = '9d024b07-85c0-4939-bcea-22948812835f') or
        (a = 4315 and s = '9977f359-c8a8-499e-9af9-15326a90fcc8') or
        (a = 3954 and s = 'c08cb9ef-7797-4ab9-aa53-25fc43f1a7f6') or
        (a = 4338 and s = '06e00ee8-ec64-4aee-9ce2-986559edcc08') or
        (a = 4214 and s = 'b465b4ab-e4e7-46c1-aaac-22817ca6df82') or
        (a = 4344 and s = '6850e940-8afb-4b7e-b428-92996e0d7cc3') or
        (a = 4240 and s = 'd18fe358-6fb4-473e-bf26-496c860cfde1') or
        (a = 4337 and s = 'ddd125eb-2c07-4a8b-b8d2-295356df2253') or
        (a = 4348 and s = '8d791558-ec95-4b2d-9c17-f72e530c30ec') or
        (a = 4339 and s = '73ec1c2a-9258-4ffc-b875-c0f7ac58e1ba') or
        (a = 4343 and s = '79e4a031-958c-4905-9b8d-dd6acd18b253') or
        (a = 4328 and s = '8e785bb6-ca13-47fc-b2d7-de6fa86e842f') or
        (a = 4331 and s = '2665d0e0-49b2-416f-9220-c6c67ccec868') or
        (a = 4239 and s = 'b18a4d21-4c4a-43bc-a7d5-15007c43fd18') or
        (a = 4238 and s = '04720557-55ca-4e0c-be87-332eb43e6274') or
        (a = 4255 and s = 'c731fc35-b7b7-44ff-9075-3c2c2ec500ce') or
        (a = 4327 and s = 'ad9a16a0-129f-48db-9105-06919a24eb3f') or
        (a = 4335 and s = '94ee4985-c05d-4772-991c-781e1602ba0e') or
        (a = 4342 and s = '38db2da1-ea4d-40ba-85a3-beac8f7fc055') or
        (a = 4191 and s = 'a5f3264f-73fe-43bc-972d-97c51710f395') or
        (a = 4341 and s = '32f885b2-63dd-40b2-b8b5-9d2ed7221235') or
        (a = 4347 and s = '8ac67959-3e04-420f-bda5-a4bf5b45ea03') or
        (a = 4353 and s = 'ec994ea3-5cf8-4951-a409-8a04ad1cffaf') or
        (a = 4354 and s = '644f26c6-f4ee-4cac-8885-807be11941c3') or
        (a = 4212 and s = '322c92c1-4d3d-463d-8001-97557e9e93f9') or
        (a = 4325 and s = '932c592b-6ec4-4ec8-a4bd-79c4900996ab') or

        (a = 3735 and s = 'd0b160b8-1d27-40ad-8cad-1b69c2187195') or
        (a = 4193 and s = 'd2c05a2c-fdee-4417-b2dd-cf58a51a6db2') or
        (a = 4365 and s = '07d1c433-3a42-473a-9d8f-45f21001dbe5') or
        (a = 4363 and s = 'f0168ae8-8f75-4dee-b113-510c43f2df30') or
        (a = 4346 and s = '4f1cf6d9-999b-4d9e-95c0-55b45ec96573') or
        (a = 4334 and s = '9db5716a-89ef-4a0b-bfe3-f0785f31f5f9') or
        (a = 4340 and s = '8b2b0701-8bd2-44ca-b90b-a9186acadcaf') or
        (a = 4329 and s = 'f0dbede6-ca7f-4e31-9e5e-2de9eb0dd672') or
        (a = 4211 and s = '4b10bebc-8633-4a2c-bb23-05abac483c54') or
        (a = 4351 and s = '64517493-57d5-4f1b-9fdb-393105b3fce7') or
        (a = 4321 and s = '4054e543-6119-4146-8922-3c026109eada') or
        (a = 4204 and s = '9d024b07-85c0-4939-bcea-22948812835f') or
        (a = 4315 and s = '9977f359-c8a8-499e-9af9-15326a90fcc8') or
        (a = 3954 and s = 'c08cb9ef-7797-4ab9-aa53-25fc43f1a7f6') or
        (a = 4338 and s = '06e00ee8-ec64-4aee-9ce2-986559edcc08') or
        (a = 4214 and s = 'b465b4ab-e4e7-46c1-aaac-22817ca6df82') or
        (a = 4344 and s = '6850e940-8afb-4b7e-b428-92996e0d7cc3') or
        (a = 4240 and s = 'd18fe358-6fb4-473e-bf26-496c860cfde1') or
        (a = 4337 and s = 'ddd125eb-2c07-4a8b-b8d2-295356df2253') or
        (a = 4348 and s = '8d791558-ec95-4b2d-9c17-f72e530c30ec') or
        (a = 4339 and s = '73ec1c2a-9258-4ffc-b875-c0f7ac58e1ba') or
        (a = 4343 and s = '79e4a031-958c-4905-9b8d-dd6acd18b253') or
        (a = 4328 and s = '8e785bb6-ca13-47fc-b2d7-de6fa86e842f') or
        (a = 4331 and s = '2665d0e0-49b2-416f-9220-c6c67ccec868') or
        (a = 4239 and s = 'b18a4d21-4c4a-43bc-a7d5-15007c43fd18') or
        (a = 4238 and s = '04720557-55ca-4e0c-be87-332eb43e6274') or
        (a = 4255 and s = 'c731fc35-b7b7-44ff-9075-3c2c2ec500ce') or
        (a = 4327 and s = 'ad9a16a0-129f-48db-9105-06919a24eb3f') or
        (a = 4335 and s = '94ee4985-c05d-4772-991c-781e1602ba0e') or
        (a = 4342 and s = '38db2da1-ea4d-40ba-85a3-beac8f7fc055') or
        (a = 4191 and s = 'a5f3264f-73fe-43bc-972d-97c51710f395') or
        (a = 4341 and s = '32f885b2-63dd-40b2-b8b5-9d2ed7221235') or
        (a = 4347 and s = '8ac67959-3e04-420f-bda5-a4bf5b45ea03') or
        (a = 4353 and s = 'ec994ea3-5cf8-4951-a409-8a04ad1cffaf') or
        (a = 4354 and s = '644f26c6-f4ee-4cac-8885-807be11941c3') or
        (a = 4212 and s = '322c92c1-4d3d-463d-8001-97557e9e93f9') or
        (a = 4325 and s = '932c592b-6ec4-4ec8-a4bd-79c4900996ab')
    )";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Maximum expression depth exceeded"));
}


TEST_F(TQueryPrepareTest, BigQuery)
{
    TString query = "k from [//t] where a in (0";
    for (int i = 1; i < 1000; ++i) {
        query += ", " + ToString(i);
    }
    query += ")";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    PreparePlanFragment(&PrepareMock_, query);
}

TEST_F(TQueryPrepareTest, ResultSchemaCollision)
{
    ExpectPrepareThrowsWithDiagnostics(
        "a as x, b as x FROM [//t] WHERE k > 3",
        ContainsRegex("Alias \"x\" has been already used"));
}

TEST_F(TQueryPrepareTest, MisuseAggregateFunction)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(sum(a)) from [//t] group by k",
        ContainsRegex("Misuse of aggregate function .*"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(a) from [//t]",
        ContainsRegex("Misuse of aggregate function .*"));
}

TEST_F(TQueryPrepareTest, FailedTypeInference)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "null from [//t]",
        ContainsRegex("Type inference failed"));
}

TEST_F(TQueryPrepareTest, JoinColumnCollision)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//t] join [//s] using b",
        ContainsRegex("Column \"a\" occurs both in main and joined tables"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s", _))
        .WillOnce(Return(MakeFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "* from [//t] join [//s] using b",
        ContainsRegex("Column .* occurs both in main and joined tables"));
}

TEST_F(TQueryPrepareTest, SortMergeJoin)
{
    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        TTableSchema tableSchema({
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(cid))")),
            TColumnSchema("cid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("id", EValueType::Int64),
            TColumnSchema("__shard__", EValueType::Int64),
            TColumnSchema("PhraseID", EValueType::Int64),
            TColumnSchema("price", EValueType::Int64),
        });

        SetTableSchema(&dataSplit, tableSchema);

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//bids", _))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        TTableSchema tableSchema({
            TColumnSchema("ExportIDHash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(ExportID))")),
            TColumnSchema("ExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("GroupExportID", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("PhraseID", EValueType::Uint64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("UpdateTime", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Shows", EValueType::Int64),
            TColumnSchema("Clicks", EValueType::Int64),
        });

        SetTableSchema(&dataSplit, tableSchema);

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//DirectPhraseStat", _))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        TTableSchema tableSchema({
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(pid))")),
            TColumnSchema("pid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("__shard__", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("status", EValueType::Int64),
        });

        SetTableSchema(&dataSplit, tableSchema);

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//phrases", _))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe));

        TTableSchema tableSchema({
            TColumnSchema("hash", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("int64(farm_hash(cid))")),
            TColumnSchema("cid", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("__shard__", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("value", EValueType::Int64),
        });

        SetTableSchema(&dataSplit, tableSchema);

        EXPECT_CALL(PrepareMock_, GetInitialSplit("//campaigns", _))
            .WillRepeatedly(Return(MakeFuture(dataSplit)));
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//campaigns] C on D.cid = C.cid\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 2);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 2);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 4);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0);
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//campaigns] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 3);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 2);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 4);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0);
    }

    {
        TString queryString = "* from [//bids] D\n"
            "left join [//DirectPhraseStat] S on (D.cid, D.pid, uint64(D.PhraseID)) = (S.ExportID, S.GroupExportID, S.PhraseID)\n"
            "left join [//campaigns] C on (D.cid,D.__shard__) = (C.cid,C.__shard__)\n"
            "left join [//phrases] P on (D.pid,D.__shard__) = (P.pid,P.__shard__)";

        auto query = PreparePlanFragment(&PrepareMock_, queryString)->Query;

        EXPECT_EQ(query->JoinClauses.size(), 3);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 4);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 3);

        EXPECT_EQ(joinClauses[1]->ForeignKeyPrefix, 3);
        EXPECT_EQ(joinClauses[1]->CommonKeyPrefix, 2);

        EXPECT_EQ(joinClauses[2]->ForeignKeyPrefix, 3);
        EXPECT_EQ(joinClauses[2]->CommonKeyPrefix, 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TJobQueryPrepareTest
    : public ::testing::Test
{ };

TEST_F(TJobQueryPrepareTest, TruePredicate)
{
    ParseSource("* where true", EParseMode::JobQuery);
}

TEST_F(TJobQueryPrepareTest, FalsePredicate)
{
    ParseSource("* where false", EParseMode::JobQuery);
}

////////////////////////////////////////////////////////////////////////////////

class TQueryCoordinateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
            .WillOnce(Return(MakeFuture(MakeSimpleSplit("//t"))));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(config);

        MergeFrom(RangeExtractorMap.Get(), *BuiltinRangeExtractorMap);
    }

    void Coordinate(const TString& source, const TDataSplits& dataSplits, size_t subqueriesCount)
    {
        auto fragment = PreparePlanFragment(
            &PrepareMock_,
            source);

        auto buffer = New<TRowBuffer>();
        TRowRanges sources;
        for (const auto& split : dataSplits) {
            auto range = GetBothBoundsFromDataSplit(split);
            sources.emplace_back(
                buffer->Capture(range.first.Get()),
                buffer->Capture(range.second.Get()));
        }

        auto rowBuffer = New<TRowBuffer>();

        TQueryOptions options;
        options.RangeExpansionLimit = 1000;
        options.VerboseLogging = true;

        auto prunedRanges = GetPrunedRanges(
            fragment->Query,
            MakeId(EObjectType::Table, 0x42, 0, 0xdeadbabe),
            MakeSharedRange(std::move(sources), buffer),
            rowBuffer,
            ColumnEvaluatorCache_,
            RangeExtractorMap,
            options);

        EXPECT_EQ(prunedRanges.size(), subqueriesCount);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

    TRangeExtractorMapPtr RangeExtractorMap = New<TRangeExtractorMap>();
};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    TDataSplits emptySplits;

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", emptySplits, 0);
    });
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", singleSplit, 1);
    });
}

TEST_F(TQueryCoordinateTest, UsesKeyToPruneSplits)
{
    TDataSplits splits;

    splits.emplace_back(MakeSimpleSplit("//t", 1));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), YsonToKey("0;0;0"));
    SetUpperBound(&splits.back(), YsonToKey("1;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 2));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), YsonToKey("1;0;0"));
    SetUpperBound(&splits.back(), YsonToKey("2;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 3));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), YsonToKey("2;0;0"));
    SetUpperBound(&splits.back(), YsonToKey("3;0;0"));

    EXPECT_NO_THROW({
        Coordinate("a from [//t] where k = 1 and l = 2 and m = 3", splits, 1);
    });
}

TEST_F(TQueryCoordinateTest, SimpleIn)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t] where k in (1u, 2.0, 3)", singleSplit, 3);
    });
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFailureLocation,
    (Nowhere)
    (Codegen)
    (Execution)
);

class TReaderMock
    : public ISchemafulReader
{
public:
    MOCK_METHOD1(Read, bool(std::vector<TUnversionedRow>*));
    MOCK_METHOD0(GetReadyEvent, TFuture<void>());

    virtual TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }
};

class TWriterMock
    : public ISchemafulWriter
{
public:
    MOCK_METHOD0(Close, TFuture<void>());
    MOCK_METHOD1(Write, bool(const TRange<TUnversionedRow>&));
    MOCK_METHOD0(GetReadyEvent, TFuture<void>());
};

TOwningRow YsonToRow(
    const TString& yson,
    const TDataSplit& dataSplit,
    bool treatMissingAsNull = true)
{
    auto tableSchema = GetTableSchemaFromDataSplit(dataSplit);

    return NTableClient::YsonToSchemafulRow(yson, tableSchema, treatMissingAsNull);
}

TQueryStatistics DoExecuteQuery(
    const std::vector<TString>& source,
    TFunctionProfilerMapPtr functionProfilers,
    TAggregateProfilerMapPtr aggregateProfilers,
    EFailureLocation failureLocation,
    TConstQueryPtr query,
    ISchemafulWriterPtr writer,
    const TQueryBaseOptions& options,
    TJoinSubqueryProfiler joinProfiler = nullptr)
{
    std::vector<TOwningRow> owningSource;
    std::vector<TRow> sourceRows;

    auto readerMock = New<StrictMock<TReaderMock>>();

    for (const auto& row : source) {
        owningSource.push_back(NTableClient::YsonToSchemafulRow(row, query->GetReadSchema(), true));
    }

    sourceRows.resize(owningSource.size());
    typedef const TRow(TOwningRow::*TGetFunction)() const;

    std::transform(
        owningSource.begin(),
        owningSource.end(),
        sourceRows.begin(),
        std::mem_fn(TGetFunction(&TOwningRow::Get)));

    ON_CALL(*readerMock, Read(_))
        .WillByDefault(DoAll(SetArgPointee<0>(sourceRows), Return(false)));
    if (failureLocation != EFailureLocation::Codegen) {
        EXPECT_CALL(*readerMock, Read(_));
    }

    auto evaluator = New<TEvaluator>(New<TExecutorConfig>());
    return evaluator->Run(
        query,
        readerMock,
        writer,
        joinProfiler,
        functionProfilers,
        aggregateProfilers,
        options);
}

std::vector<TRow> OrderRowsBy(TRange<TRow> rows, const std::vector<TString>& columns, const TTableSchema& tableSchema)
{
    std::vector<int> indexes;
    for (const auto& column : columns) {
        indexes.push_back(tableSchema.GetColumnIndexOrThrow(column));
    }

    std::vector<TRow> result(rows.begin(), rows.end());
    std::sort(result.begin(), result.end(), [&] (TRow lhs, TRow rhs) {
        for (auto index : indexes) {
            if (lhs[index] == rhs[index]) {
                continue;
            } else {
                return lhs[index] < rhs[index];
            }
        }
        return false;
    });
    return result;
}

typedef std::function<void(TRange<TRow>, const TTableSchema&)> TResultMatcher;

TResultMatcher ResultMatcher(std::vector<TOwningRow> expectedResult)
{
    return [MOVE(expectedResult)] (TRange<TRow> result, const TTableSchema& tableSchema) {
        EXPECT_EQ(expectedResult.size(), result.Size());
        if (expectedResult.size() != result.Size()) {
            return;
        }

        for (int i = 0; i < expectedResult.size(); ++i) {
            auto expectedRow = expectedResult[i];
            auto row = result[i];
            EXPECT_EQ(expectedRow.GetCount(), row.GetCount());
            if (expectedRow.GetCount() != row.GetCount()) {
                continue;
            }
            for (int j = 0; j < expectedRow.GetCount(); ++j) {
                const auto& expectedValue = expectedRow[j];
                const auto& value = row[j];
                EXPECT_EQ(expectedValue.Type, value.Type);
                if (expectedValue.Type != value.Type) {
                    continue;
                }
                if (expectedValue.Type == EValueType::Any) {
                    // Slow path.
                    auto expectedYson = TYsonString(TString(expectedValue.Data.String, expectedValue.Length));
                    auto expectedStableYson = ConvertToYsonStringStable(ConvertToNode(expectedYson));
                    auto yson = TYsonString(TString(value.Data.String, value.Length));
                    auto stableYson = ConvertToYsonStringStable(ConvertToNode(yson));
                    EXPECT_EQ(expectedStableYson, stableYson);
                } else {
                    // Fast path.
                    EXPECT_EQ(expectedValue, value);
                }
            }
        }
    };
}

TResultMatcher OrderedResultMatcher(
    std::vector<TOwningRow> expectedResult,
    std::vector<TString> columns)
{
    return [MOVE(expectedResult), MOVE(columns)] (TRange<TRow> result, const TTableSchema& tableSchema) {
        EXPECT_EQ(expectedResult.size(), result.Size());

        auto sortedResult = OrderRowsBy(result, columns, tableSchema);

        for (int i = 0; i < expectedResult.size(); ++i) {
            EXPECT_EQ(sortedResult[i], expectedResult[i]);
        }
    };
}

class TQueryEvaluateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("Test");

        auto bcImplementations = UDF_BC(test_udfs);

        MergeFrom(TypeInferrers_.Get(), *BuiltinTypeInferrersMap);
        MergeFrom(FunctionProfilers_.Get(), *BuiltinFunctionProfilers);
        MergeFrom(AggregateProfilers_.Get(), *BuiltinAggregateProfilers);

        TFunctionRegistryBuilder builder(
            TypeInferrers_.Get(),
            FunctionProfilers_.Get(),
            AggregateProfilers_.Get());

        builder.RegisterFunction(
            "abs_udf",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);
        builder.RegisterFunction(
            "exp_udf",
            std::vector<TType>{EValueType::Int64, EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);
        builder.RegisterFunction(
            "strtol_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Uint64,
            bcImplementations,
            ECallingConvention::Simple);
        builder.RegisterFunction(
            "tolower_udf",
            std::vector<TType>{EValueType::String},
            EValueType::String,
            bcImplementations,
            ECallingConvention::Simple);
        builder.RegisterFunction(
            "is_null_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Boolean,
            bcImplementations,
            ECallingConvention::UnversionedValue);
        builder.RegisterFunction(
            "sum_udf",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            EValueType::Int64,
            bcImplementations);
        builder.RegisterFunction(
            "seventyfive",
            std::vector<TType>{},
            EValueType::Uint64,
            bcImplementations,
            ECallingConvention::Simple);

        ///

        builder.RegisterFunction(
            "invalid_ir",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            TSharedRef(invalid_ir_bc, invalid_ir_bc_len, nullptr),
            ECallingConvention::Simple);

        builder.RegisterFunction(
            "abs_udf_arity",
            "abs_udf",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Int64, EValueType::Int64},
            EValueType::Null,
            EValueType::Int64,
            bcImplementations,
            GetCallingConvention(ECallingConvention::Simple));

        builder.RegisterFunction(
            "abs_udf_double",
            "abs_udf",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Double},
            EValueType::Null,
            EValueType::Int64,
            bcImplementations,
            GetCallingConvention(ECallingConvention::Simple));

        builder.RegisterFunction(
            "throw_if_negative_udf",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            bcImplementations,
            ECallingConvention::Simple);

    }

    virtual void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    TQueryPtr Evaluate(
        const TString& query,
        const TDataSplit& dataSplit,
        const std::vector<TString>& owningSource,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max())
    {
        std::vector<std::vector<TString>> owningSources = {
            owningSource
        };
        std::map<TString, TDataSplit> dataSplits = {
            {"//t", dataSplit}
        };

        return BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                resultMatcher,
                inputRowLimit,
                outputRowLimit,
                EFailureLocation::Nowhere)
            .Get()
            .ValueOrThrow();
    }

    TQueryPtr Evaluate(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        const std::vector<std::vector<TString>>& owningSources,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max())
    {
        return BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                resultMatcher,
                inputRowLimit,
                outputRowLimit,
                EFailureLocation::Nowhere)
            .Get()
            .ValueOrThrow();
    }

    TQueryPtr EvaluateExpectingError(
        const TString& query,
        const TDataSplit& dataSplit,
        const std::vector<TString>& owningSource,
        EFailureLocation failureLocation,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max())
    {
        std::vector<std::vector<TString>> owningSources = {
            owningSource
        };
        std::map<TString, TDataSplit> dataSplits = {
            {"//t", dataSplit}
        };

        return BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                [] (TRange<TRow>, const TTableSchema&) { },
                inputRowLimit,
                outputRowLimit,
                failureLocation)
            .Get()
            .ValueOrThrow();
    }

    TQueryPtr DoEvaluate(
        const TString& query,
        const std::map<TString, TDataSplit>& dataSplits,
        const std::vector<std::vector<TString>>& owningSources,
        const TResultMatcher& resultMatcher,
        i64 inputRowLimit,
        i64 outputRowLimit,
        EFailureLocation failureLocation)
    {
        for (const auto& dataSplit : dataSplits) {
            EXPECT_CALL(PrepareMock_, GetInitialSplit(dataSplit.first, _))
                .WillOnce(Return(MakeFuture(dataSplit.second)));
        }

        auto fetchFunctions = [&] (const std::vector<TString>& /*names*/, const TTypeInferrerMapPtr& typeInferrers) {
            MergeFrom(typeInferrers.Get(), *TypeInferrers_);
        };

        TQueryBaseOptions options;
        options.InputRowLimit = inputRowLimit;
        options.OutputRowLimit = outputRowLimit;

        size_t sourceIndex = 1;

        auto prepareAndExecute = [&] () {
            auto fragment = PreparePlanFragment(
                &PrepareMock_,
                query,
                fetchFunctions);
            const auto& primaryQuery = fragment->Query;

            auto profileCallback = [&] (TQueryPtr subquery, TConstJoinClausePtr joinClause) mutable {
                auto rows = owningSources[sourceIndex++];

                return [&, rows, subquery, joinClause] (std::vector<TRow> keys, TRowBufferPtr permanentBuffer)
                mutable {
                    TDataRanges dataSource;
                    TQueryPtr preparedSubquery;
                    std::tie(preparedSubquery, dataSource) = GetForeignQuery(
                        subquery,
                        joinClause,
                        std::move(keys),
                        permanentBuffer);

                    auto pipe = New<NTableClient::TSchemafulPipe>();

                    DoExecuteQuery(
                        rows,
                        FunctionProfilers_,
                        AggregateProfilers_,
                        failureLocation,
                        preparedSubquery,
                        pipe->GetWriter(),
                        options);

                    return pipe->GetReader();
                };
            };


            ISchemafulWriterPtr writer;
            TFuture<IUnversionedRowsetPtr> asyncResultRowset;

            std::tie(writer, asyncResultRowset) = CreateSchemafulRowsetWriter(primaryQuery->GetTableSchema());

            DoExecuteQuery(
                owningSources.front(),
                FunctionProfilers_,
                AggregateProfilers_,
                failureLocation,
                primaryQuery,
                writer,
                options,
                profileCallback);

            auto resultRowset = WaitFor(asyncResultRowset)
                .ValueOrThrow();
            resultMatcher(resultRowset->GetRows(), TTableSchema(primaryQuery->GetTableSchema()));

            return primaryQuery;
        };

        if (failureLocation != EFailureLocation::Nowhere) {
            EXPECT_THROW(prepareAndExecute(), TErrorException);
            return nullptr;
        } else {
            return prepareAndExecute();
        }
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TActionQueuePtr ActionQueue_;

    TTypeInferrerMapPtr TypeInferrers_ = New<TTypeInferrerMap>();
    TFunctionProfilerMapPtr FunctionProfilers_ = New<TFunctionProfilerMap>();
    TAggregateProfilerMapPtr AggregateProfilers_ = New<TAggregateProfilerMap>();

};

std::vector<TOwningRow> YsonToRows(std::initializer_list<const char*> rowsData, const TDataSplit& split)
{
    std::vector<TOwningRow> result;

    for (auto row : rowsData) {
        result.push_back(YsonToRow(row, split, true));
    }

    return result;
}

TEST_F(TQueryEvaluateTest, Simple)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SelectAll)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("* FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, FilterNulls1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where b > 0", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, FilterNulls2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=6",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where b > 0 or is_null(b)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleCmpInt)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=6;b=6"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleCmpString)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"a\";b=\"aa\"",
        "a=\"aa\";b=\"aa\""
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleBetweenAnd)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11",
        "a=15;b=11"
    };

    auto result = YsonToRows({
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a between 9 and 11", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleIn)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=-10;b=11",
        "a=15;b=11"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=-10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a in (4.0, -10)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, BigIn)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    TStringBuilder inBuilder;
    inBuilder.AppendString("a in (");

    for (int i = 0; i < 50; ++ i) {
        if (i != 0) {
            inBuilder.AppendString(", ");
        }
        inBuilder.AppendFormat("%v", i * 2);
    }

    inBuilder.AppendString(")");

    std::vector<TString> source = {
        "a=4",
        "a=10",
        "a=15",
        "a=17",
        "a=18",
        "a=22",
        "a=31",
    };

    auto result = YsonToRows({
        "a=4",
        "a=10",
        "a=18",
        "a=22",
    }, split);

    Evaluate("a FROM [//t] where " + inBuilder.Flush(), split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleInWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "b=1",
        "a=2",
        "a=2;b=1",
        ""
    };

    auto result = YsonToRows({
        "b=1",
        "a=2",
    }, split);

    Evaluate("a, b FROM [//t] where (a, b) in ((null, 1), (2, null))", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransform)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4",
        "a=-10",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "x=17",
        "",
    }, resultSplit);

    Evaluate("transform(a, (4.0, -10), (13, 17)) as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, BigTransform)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    TStringBuilder transformBuilder;
    transformBuilder.AppendString("transform(a, (");

    for (int i = -50; i < 50; ++ i) {
        if (i != -50) {
            transformBuilder.AppendString(", ");
        }
        transformBuilder.AppendFormat("%v", i);
    }

    transformBuilder.AppendString("), (");

    for (int i = -50; i < 50; ++ i) {
        if (i != -50) {
            transformBuilder.AppendString(", ");
        }
        transformBuilder.AppendFormat("%v", -i);
    }

    transformBuilder.AppendString("))");

    std::vector<TString> source = {
        "a=4",
        "a=-10",
        "a=7",
        "a=60",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=-4",
        "x=10",
        "x=-7",
        "",
    }, resultSplit);

    Evaluate(transformBuilder.Flush() + " as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransform2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=4;b=p",
        "a=-10;b=q",
        "a=-10;b=s",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "",
        "x=17",
        "",
    }, resultSplit);

    Evaluate(
        "transform((a, b), ((4.0, 'p'), (-10, 's')), (13, 17)) as x FROM [//t]",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleTransformWithDefault)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String}
    });

    std::vector<TString> source = {
        "a=4;b=p",
        "a=-10;b=q",
        "a=-10;b=s",
        "a=15"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=13",
        "x=-9",
        "x=17",
        "x=16",
    }, resultSplit);

    Evaluate(
        "transform((a, b), ((4.0, 'p'), (-10, 's')), (13, 17), a + 1) as x FROM [//t]",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    };

    auto result = YsonToRows({
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    }, split);

    Evaluate("a, b, c FROM [//t] where a > 3", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleWithNull2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=2;c=3",
        "a=4",
        "a=5;b=5",
        "a=7;c=8",
        "a=10;b=1",
        "a=10;c=1"
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;x=5",
        "a=4;",
        "a=5;",
        "a=7;"
    }, resultSplit);

    Evaluate("a, b + c as x FROM [//t] where a < 10", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, Strings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        ""
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"\\x0F\\xC7\\x84~\\0@\\0\\0<\\0\\0@\\x99l`\\x16\""
    }, resultSplit);

    Evaluate("\"\\x0F\\xC7\\x84~\\0@\\0\\0<\\0\\0@\\x99l`\\x16\" as result FROM [//t]", split, source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foo",
        "s=bar",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foo",
        "s=bar",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, SimpleStrings2)
{
    auto split = MakeSplit({
        {"s", EValueType::String},
        {"u", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foo; u=x",
        "s=bar; u=y",
        "s=baz; u=x",
        "s=olala; u=z"
    };

    auto result = YsonToRows({
        "s=foo; u=x",
        "s=baz; u=x"
    }, split);

    Evaluate("s, u FROM [//t] where u = \"x\"", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, IsPrefixStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foobar",
        "s=bar",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foobar"
    }, split);

    Evaluate("s FROM [//t] where is_prefix(\"foo\", s)", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, IsSubstrStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "s=foobar",
        "s=barfoo",
        "s=abc",
        "s=\"baz foo bar\"",
        "s=\"baz fo bar\"",
        "s=xyz",
        "s=baz"
    };

    auto result = YsonToRows({
        "s=foobar",
        "s=barfoo",
        "s=\"baz foo bar\"",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t] where is_substr(\"foo\", s) or is_substr(s, \"XX baz YY\")", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, GroupByBool)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=%false;t=200",
        "x=%true;t=240"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotals)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
        "x=%false;t=200",
        "x=%true;t=240",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals", split,
        source, ResultMatcher(resultWithTotals));

    auto resultWithTotalsAfterHaving = YsonToRows({
        "x=%true;t=240",
        "t=240"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x having t > 200 with totals", split,
        source, ResultMatcher(resultWithTotalsAfterHaving));

    auto resultWithTotalsBeforeHaving = YsonToRows({
        "x=%true;t=240",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals having t > 200", split,
        source, ResultMatcher(resultWithTotalsBeforeHaving));

    auto resultWithTotalsBeforeHaving2 = YsonToRows({
        "x=%false;t=200",
        "t=440"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x with totals having t < 220", split,
        source, ResultMatcher(resultWithTotalsBeforeHaving2));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotalsNulls)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
    }, resultSplit);

    EXPECT_THROW_THAT(
        [&] {
            Evaluate("x, sum(b) as t FROM [//t] group by a % 2 as x with totals", split,
                source, [] (TRange<TRow> result, const TTableSchema& tableSchema) { });
        },
        HasSubstr("Null values in group key"));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, GroupWithTotalsEmpty)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto resultWithTotals = YsonToRows({
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] group by a % 2 as x with totals", split,
        source, ResultMatcher(resultWithTotals));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexWithAliases)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("a % 2 as x, sum(b) + x as t FROM [//t] where a > 1 group by x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"q", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=0;q=0;t=200",
        "x=1;q=0;t=241"
    }, resultSplit);

    Evaluate("x, q, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x, 0 as q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexBigResult)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(TString() + "a=" + ToString(i) + ";b=" + ToString(i * 10));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    std::vector<TOwningRow> result;

    for (size_t i = 2; i < 10000; ++i) {
        result.push_back(YsonToRow(TString() + "x=" + ToString(i) + ";t=" + ToString(i * 10 + i), resultSplit, false));
    }

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a as x", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, ComplexWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1;t=251;y=250",
        "x=0;t=200;y=200",
        "y=6"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t, sum(b) as y FROM [//t] group by a % 2 as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = YsonToRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having a = 1", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = YsonToRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having sum(b) = 20", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
    }, resultSplit);

    Evaluate("a as x FROM [//t] group by a having sum(b) = 20", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, IsNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64}
    });

    auto result = YsonToRows({
        "b=1",
        "b=2",
        "b=3"
    }, resultSplit);

    Evaluate("b FROM [//t] where is_null(a)", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, DoubleSum)
{
    auto split = MakeSplit({
        {"a", EValueType::Double}
    });

    std::vector<TString> source = {
        "a=1.",
        "a=1.",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=2.;t=3"
    }, resultSplit);

    Evaluate("sum(a) as x, sum(1) as t FROM [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, ComplexStrings)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160"
    }, resultSplit);

    Evaluate("x, sum(a) as t FROM [//t] where a > 10 group by s as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexStringsLower)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
        "a=XyZ;s=one",
        "a=aB1C;s=two",
        "a=cs1dv;s=three",
        "a=HDs;s=four",
        "a=kIu;s=five",
        "a=trg1t;s=six"
    };

    auto resultSplit = MakeSplit({
        {"s", EValueType::String}
    });

    auto result = YsonToRows({
        "s=one",
        "s=two",
        "s=four",
        "s=five"
    }, resultSplit);

    Evaluate("s FROM [//t] where lower(a) in (\"xyz\",\"ab1c\",\"hds\",\"kiu\")", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestIf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);

    Evaluate("if(q = 4, \"a\", \"b\") as x, double(sum(b)) + 1.0 as t FROM [//t] group by if(a % 2 = 0, 4, 5) as"
                 " q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestInputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto result = YsonToRows({
        "a=2;b=20",
        "a=3;b=30"
    }, split);

    Evaluate("a, b FROM [//t] where uint64(a) > 1 and uint64(a) < 9", split, source, ResultMatcher(result), 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOutputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto result = YsonToRows({
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40"
    }, split);

    Evaluate("a, b FROM [//t] where a > 1 and a < 9", split, source, ResultMatcher(result), std::numeric_limits<i64>::max(), 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOutputRowLimit2)
{
    auto split = MakeSplit({});

    std::vector<TString> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(TString());
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    result.push_back(YsonToRow(TString() + "x=" + ToString(10000), resultSplit, false));

    Evaluate("sum(1) as x FROM [//t] group by 0 as q", split, source, ResultMatcher(result), std::numeric_limits<i64>::max(),
             100);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestTypeInference)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);

    Evaluate("if(int64(q) = 4, \"a\", \"b\") as x, double(sum(uint64(b) * 1)) + 1 as t FROM [//t] group by if"
                 "(a % 2 = 0, double(4), 5) as q", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinEmpty)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=3;b=30",
        "a=5;b=50",
        "a=7;b=70",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=2;b=20",
        "c=4;b=40",
        "c=6;b=60",
        "c=8;b=80"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({ }, resultSplit);

    Evaluate("sum(a) as x, sum(b) as y, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple2)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources,
             OrderedResultMatcher(result, {"x"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple3)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple4)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple5)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinLimit)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4",
        "a=5"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=3",
        "a=4",
        "a=5",
        "a=6"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=2",
        "x=3",
        "x=4",
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        ResultMatcher(result),
        std::numeric_limits<i64>::max(), 4);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinLimit2)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1",
        "a=1",
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=1",
        "x=1",
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        ResultMatcher(result),
        std::numeric_limits<i64>::max(), 5);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinLimit3)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4",
        "a=5",
        "a=6",
        "a=7"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=7",
        "a=5",
        "a=3",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=3"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a",
        splits,
        sources,
        OrderedResultMatcher(result, {"x"}),
        std::numeric_limits<i64>::max(), 4);

    result = YsonToRows({
        "x=1",
        "x=3",
        "x=5",
        "x=7"
    }, resultSplit);

    Evaluate(
        "a as x FROM [//left] join [//right] using a limit 4",
        splits,
        sources,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinLimit4)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"ut", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"v", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;ut=1;b=30",
        "a=1;ut=2;b=20",
        "a=2;ut=3;b=10",
        "a=2;ut=4;b=30",
        "a=3;ut=5;b=20",
        "a=4;ut=6;b=10"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64, ESortOrder::Ascending},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "b=10;c=100",
        "b=20;c=200",
        "b=30;c=300"
    });

    auto resultSplit = MakeSplit({
        {"a.ut", EValueType::Int64},
        {"b.c", EValueType::Int64},
        {"a.b", EValueType::Int64},
        {"b.b", EValueType::Int64}
    });

    auto result = YsonToRows({
        "\"a.ut\"=1;\"b.c\"=300;\"a.b\"=30;\"b.b\"=30",
        "\"a.ut\"=2;\"b.c\"=200;\"a.b\"=20;\"b.b\"=20",
        "\"a.ut\"=3;\"b.c\"=100;\"a.b\"=10;\"b.b\"=10",
        "\"a.ut\"=4;\"b.c\"=300;\"a.b\"=30;\"b.b\"=30",
        "\"a.ut\"=5;\"b.c\"=200;\"a.b\"=20;\"b.b\"=20",
        "\"a.ut\"=6;\"b.c\"=100;\"a.b\"=10;\"b.b\"=10"

    }, resultSplit);

    for (size_t limit = 1; limit <= 6; ++limit) {
        std::vector<TOwningRow> currentResult(result.begin(), result.begin() + limit);
        Evaluate(
            Format("a.ut, b.c, a.b, b.b FROM [//left] a join [//right] b on a.b=b.b limit %v", limit),
            splits,
            sources,
            ResultMatcher(currentResult));
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinLimit5)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"publisherId", EValueType::String, ESortOrder::Ascending},
        {"itemId", EValueType::Int64}
    }, 0);

    splits["//publishers"] = leftSplit;
    sources.push_back({
        "publisherId=\"5903739ad7d0a6e07ad1fb93\";itemId=3796616032221332447",
        "publisherId=\"5908961de3cda81ba288b664\";itemId=847311080463071787",
        "publisherId=\"5909bd2dd7d0a68351e66077\";itemId=-3463642079005455542",
        "publisherId=\"5912f1e27ddde8c264b56f0c\";itemId=2859920047593648390",
        "publisherId=\"5912f1f88e557d5b22ff7077\";itemId=-5478070133262294529",
        "publisherId=\"591446067ddde805266009b5\";itemId=-5089939500492155348",
        "publisherId=\"591464507ddde805266009b8\";itemId=3846436484159153735",
        "publisherId=\"591468bce3cda8db9996fa89\";itemId=2341245309580180142",
        "publisherId=\"5914c6678e557dcf3bf713cf\";itemId=-6844788529441593571",
        "publisherId=\"5915869a7ddde805266009bb\";itemId=-6883609521883689",
        "publisherId=\"5918c7f8e3cda83873187c37\";itemId=-896633843529240754",
        "publisherId=\"591939f67ddde8632415d4ce\";itemId=-2679935711711852631",
        "publisherId=\"59195b327ddde8632415d4d1\";itemId=8410938732504570842"
    });

    auto rightSplit = MakeSplit({
        {"publisherId", EValueType::String, ESortOrder::Ascending},
        {"timestamp", EValueType::Uint64}
    }, 1);

    splits["//draft"] = rightSplit;
    sources.push_back({
        "publisherId=\"591446067ddde805266009b5\";timestamp=1504706169u",
        "publisherId=\"591468bce3cda8db9996fa89\";timestamp=1504706172u",
        "publisherId=\"5914c6678e557dcf3bf713cf\";timestamp=1504706178u",
        "publisherId=\"5918c7f8e3cda83873187c37\";timestamp=1504706175u",
    });

    auto resultSplit = MakeSplit({
        {"publisherId", EValueType::String}
    });

    auto result = YsonToRows({
        "publisherId=\"5903739ad7d0a6e07ad1fb93\"",
        "publisherId=\"5908961de3cda81ba288b664\"",
        "publisherId=\"5909bd2dd7d0a68351e66077\"",
        "publisherId=\"5912f1e27ddde8c264b56f0c\"",
        "publisherId=\"5912f1f88e557d5b22ff7077\"",
        "publisherId=\"591446067ddde805266009b5\"",
        "publisherId=\"591464507ddde805266009b8\"",
        "publisherId=\"591468bce3cda8db9996fa89\"",
        "publisherId=\"5914c6678e557dcf3bf713cf\"",
        "publisherId=\"5915869a7ddde805266009bb\"",
        "publisherId=\"5918c7f8e3cda83873187c37\"",
        "publisherId=\"591939f67ddde8632415d4ce\"",
        "publisherId=\"59195b327ddde8632415d4d1\""
    }, resultSplit);

    for (size_t limit = 1; limit <= 13; ++limit) {
        std::vector<TOwningRow> currentResult(result.begin(), result.begin() + limit);
        Evaluate(
            Format("[publisherId] FROM [//publishers] LEFT JOIN [//draft] USING [publisherId] LIMIT %v", limit),
            splits,
            sources,
            ResultMatcher(currentResult));
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinNonPrefixColumns)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        TColumnSchema("x", EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("y", EValueType::String)
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "x=a",
        "x=b",
        "x=c"
    });

    auto rightSplit = MakeSplit({
        TColumnSchema("a", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("x", EValueType::String)
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"a", EValueType::Int64},
        {"y", EValueType::String}
    });

    auto result = YsonToRows({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    }, resultSplit);

    Evaluate("x, a, y FROM [//left] join [//right] using x", splits, sources,
             OrderedResultMatcher(result, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinManySimple)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    splits["//a"] = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String}
    }, 0);
    sources.push_back({
        "a=2;c=b",
        "a=3;c=c",
        "a=4;c=a"
    });

    splits["//b"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::String},
        {"d", EValueType::String}
    }, 1);
    sources.push_back({
        "b=100;c=a;d=X",
        "b=200;c=b;d=Y",
        "b=300;c=c;d=X",
        "b=400;c=a;d=Y",
        "b=500;c=b;d=X",
        "b=600;c=c;d=Y"
    });

    splits["//c"] = MakeSplit({
        {"d", EValueType::String},
        {"e", EValueType::Int64},
    }, 2);
    sources.push_back({
        "d=X;e=1234",
        "d=Y;e=5678"
    });


    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String},
        {"b", EValueType::Int64},
        {"d", EValueType::String},
        {"e", EValueType::Int64}
    });

    auto result = YsonToRows({
         "a=2;c=b;b=200;d=Y;e=5678",
         "a=2;c=b;b=500;d=X;e=1234",
         "a=3;c=c;b=300;d=X;e=1234",
         "a=3;c=c;b=600;d=Y;e=5678",
         "a=4;c=a;b=100;d=X;e=1234",
         "a=4;c=a;b=400;d=Y;e=5678"
    }, resultSplit);

    Evaluate(
        "a, c, b, d, e from [//a] join [//b] using c join [//c] using d",
        splits,
        sources,
        OrderedResultMatcher(result, {"a", "b"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestSortMergeJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=3;b=30",
        "a=5;b=50",
        "a=7;b=70",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"c", EValueType::Int64, ESortOrder::Ascending},
        {"d", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;d=10",
        "c=2;d=20",
        "c=4;d=40",
        "c=5;d=50",
        "c=7;d=70",
        "c=8;d=80"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"d", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=10;d=10",
        "a=5;b=50;d=50",
        "a=7;b=70;d=70"
    }, resultSplit);

    auto query = Evaluate("a, b, d FROM [//left] join [//right] on a = c", splits, sources, ResultMatcher(result));

    EXPECT_EQ(query->JoinClauses.size(), 1);
    const auto& joinClauses = query->JoinClauses;

    EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 1);
    EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestPartialSortMergeJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64, ESortOrder::Ascending},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=2;c=1 ",
        "a=1;b=3;c=2 ",
        "a=1;b=1;c=3 ",
        "a=1;b=4;c=4 ",
        "a=2;b=4;c=5 ",
        "a=2;b=3;c=6 ",
        "a=2;b=1;c=7 ",
        "a=2;b=2;c=8 ",
        "a=3;b=1;c=9 ",
        "a=3;b=4;c=10",
        "a=3;b=3;c=11",
        "a=3;b=2;c=12",
        "a=4;b=8;c=13",
        "a=4;b=7;c=14"
    });

    auto rightSplit = MakeSplit({
        {"d", EValueType::Int64, ESortOrder::Ascending},
        {"e", EValueType::Int64, ESortOrder::Ascending},
        {"f", EValueType::Int64},
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "d=1;e=1;f=3 ",
        "d=1;e=2;f=1 ",
        "d=1;e=3;f=2 ",
        "d=1;e=4;f=4 ",
        "d=2;e=1;f=7 ",
        "d=2;e=2;f=8 ",
        "d=2;e=3;f=6 ",
        "d=2;e=4;f=5 ",
        "d=3;e=1;f=9 ",
        "d=3;e=2;f=12",
        "d=3;e=3;f=11",
        "d=3;e=4;f=10",
        "d=4;e=7;f=14",
        "d=4;e=8;f=13",

    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
        {"d", EValueType::Int64},
        {"e", EValueType::Int64},
        {"f", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=2;c=1 ;d=1;e=2;f=1 ",
        "a=1;b=3;c=2 ;d=1;e=3;f=2 ",
        "a=1;b=1;c=3 ;d=1;e=1;f=3 ",
        "a=1;b=4;c=4 ;d=1;e=4;f=4 ",
        "a=2;b=4;c=5 ;d=2;e=4;f=5 ",
        "a=2;b=3;c=6 ;d=2;e=3;f=6 ",
        "a=2;b=1;c=7 ;d=2;e=1;f=7 ",
        "a=2;b=2;c=8 ;d=2;e=2;f=8 ",
        "a=3;b=1;c=9 ;d=3;e=1;f=9 ",
        "a=3;b=4;c=10;d=3;e=4;f=10",
        "a=3;b=3;c=11;d=3;e=3;f=11",
        "a=3;b=2;c=12;d=3;e=2;f=12",
        "a=4;b=8;c=13;d=4;e=8;f=13",
        "a=4;b=7;c=14;d=4;e=7;f=14",
    }, resultSplit);

    {
        auto query = Evaluate("a, b, c, d, e, f FROM [//left] join [//right] on (a, b) = (d, e)",
            splits,
            sources,
            OrderedResultMatcher(result, {"c"}));

        EXPECT_EQ(query->JoinClauses.size(), 1);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 2);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1);
    }

    {
        auto rightSplit = MakeSplit({
            {"d", EValueType::Int64, ESortOrder::Ascending},
            {"e", EValueType::Int64},
            {"f", EValueType::Int64},
        }, 1);
        splits["//right"] = rightSplit;
        sources[1] = {
            "d=1;e=4;f=4 ",
            "d=1;e=1;f=3 ",
            "d=1;e=3;f=2 ",
            "d=1;e=2;f=1 ",
            "d=2;e=2;f=8 ",
            "d=2;e=4;f=5 ",
            "d=2;e=1;f=7 ",
            "d=2;e=3;f=6 ",
            "d=3;e=2;f=12",
            "d=3;e=3;f=11",
            "d=3;e=4;f=10",
            "d=3;e=1;f=9 ",
            "d=4;e=7;f=14",
            "d=4;e=8;f=13"
        };

        auto query = Evaluate("a, b, c, d, e, f FROM [//left] join [//right] on (a, b) = (d, e)",
            splits,
            sources,
            OrderedResultMatcher(result, {"c"}));

        EXPECT_EQ(query->JoinClauses.size(), 1);
        const auto& joinClauses = query->JoinClauses;

        EXPECT_EQ(joinClauses[0]->ForeignKeyPrefix, 1);
        EXPECT_EQ(joinClauses[0]->CommonKeyPrefix, 1);
    }

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=25;z=1",
        "x=20;z=0",
    }, resultSplit);

    Evaluate("sum(a) as x, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, ResultMatcher(result));
    Evaluate("sum(a) as x, z FROM [//left] join [//right] on b = b group by c % 2 as z", splits, sources, ResultMatcher(result));
    Evaluate("sum(l.a) as x, z FROM [//left] as l join [//right] as r on l.b = r.b group by r.c % 2 as z", splits, sources, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestLeftJoin)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;b=10",
        "c=3;b=30",
        "c=5;b=50",
        "c=8;b=80",
        "c=9;b=90"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;b=10;c=1",
        "a=2;b=20",
        "a=3;b=30;c=3",
        "a=4;b=40",
        "a=5;b=50;c=5",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80;c=8",
        "a=9;b=90;c=9"
    }, resultSplit);

    Evaluate(
        "a, b, c FROM [//left] left join [//right] using b",
        splits,
        sources,
        OrderedResultMatcher(result, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestLeftJoinWithCondition)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    }, 0);

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2",
        "a=3",
        "a=4"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64},
    }, 1);

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1;b=1;c=1",
        "a=1;b=2;c=1",
        "a=1;b=3;c=1",
        "a=2;b=1;c=1",
        "a=2;b=3;c=1",
        "a=3;b=1;c=1"
    });

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::Int64}
    });

    auto result = YsonToRows({
        "a=1;s=1",
        "a=4"
    }, resultSplit);

    Evaluate(
        "a, sum(c) as s FROM [//left] left join [//right] using a where b = 2 or b = # group by a",
        splits,
        sources,
        OrderedResultMatcher(result, {"a"}));

    auto result2 = YsonToRows({
        "a=1;s=1",
        "a=2",
        "a=3",
        "a=4"
    }, resultSplit);

    Evaluate(
        "a, sum(c) as s FROM [//left] left join [//right] using a and b = 2 group by a",
        splits,
        sources,
        OrderedResultMatcher(result2, {"a"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexAlias)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<TString> source = {
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

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160"
    }, resultSplit);

    Evaluate("x, sum(p.a) as t FROM [//t] as p where p.a > 10 group by p.s as x", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinMany)
{
    std::map<TString, TDataSplit> splits;
    std::vector<std::vector<TString>> sources;

    splits["//primary"] = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    }, 0);
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    splits["//secondary"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    }, 1);
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    splits["//tertiary"] = MakeSplit({
        {"c", EValueType::Int64},
        {"d", EValueType::Int64}
    }, 2);
    sources.push_back({
        "c=1;d=10",
        "c=2;d=20",
        "c=3;d=30",
        "c=4;d=40",
        "c=5;d=50",
        "c=6;d=60",
        "c=7;d=70",
        "c=8;d=80",
        "c=9;d=90"
    });


    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=20;y=200;z=0",
        "x=25;y=250;z=1"
    }, resultSplit);

    Evaluate(
        "sum(a) as x, sum(d) as y, z FROM [//primary] join [//secondary] using b join [//tertiary] using c group by c % 2 as z",
        splits,
        sources,
        OrderedResultMatcher(result, {"x"}));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOrderBy)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;

    for (int i = 0; i < 10000; ++i) {
        auto value = std::rand() % 100000 + 10000;
        source.push_back(TString() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    for (int i = 0; i < 10000; ++i) {
        auto value = 10000 - i;
        source.push_back(TString() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    std::vector<TOwningRow> result;
    for (const auto& row : source) {
        result.push_back(YsonToRow(row, split, false));
    }

    std::vector<TOwningRow> limitedResult;

    std::sort(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a * a limit 100", split, source, ResultMatcher(limitedResult));

    std::reverse(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a * 3 - 1 desc limit 100", split, source, ResultMatcher(limitedResult));


    source.clear();
    for (int i = 0; i < 10; ++i) {
        auto value = 10 - i;
        source.push_back(TString() + "a=" + ToString(i % 3) + ";b=" + ToString(value));
    }

    result.clear();
    for (const auto& row : source) {
        result.push_back(YsonToRow(row, split, false));
    }

    EXPECT_THROW_THAT(
        [&] {
            Evaluate("* FROM [//t] order by 0.0 / double(a) limit 100", split, source, ResultMatcher(result));
        },
        HasSubstr("Comparison with NaN"));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestGroupByTotalsOrderBy)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<std::pair<i64, i64>> sourceValues;
    for (int i = 0; i < 10000; ++i) {
        auto value = std::rand() % 100000 + 10000;
        sourceValues.emplace_back(value, value * 10);
    }

    for (int i = 0; i < 10000; ++i) {
        auto value = 10000 - i;
        sourceValues.emplace_back(value, value * 10);
    }

    std::vector<std::pair<i64, i64>> groupedValues(200, std::make_pair(0, 0));
    i64 totalSum = 0;
    for (const auto& row : sourceValues) {
        i64 x = row.first % 200;
        groupedValues[x].first = x;
        groupedValues[x].second += row.second;
        totalSum += row.second;
    }

    std::sort(
        groupedValues.begin(),
        groupedValues.end(), [] (const std::pair<i64, i64>& lhs, const std::pair<i64, i64>& rhs) {
            return lhs.second < rhs.second;
        });

    groupedValues.resize(50);

    std::vector<TString> source;
    for (const auto& row : sourceValues) {
        source.push_back(TString() + "a=" + ToString(row.first) + ";b=" + ToString(row.second));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    result.push_back(YsonToRow("y=" + ToString(totalSum), resultSplit, true));

    for (const auto& row : groupedValues) {
        TString resultRow = TString() + "x=" + ToString(row.first) + ";y=" + ToString(row.second);
        result.push_back(YsonToRow(resultRow, resultSplit, false));
    }

    Evaluate("x, sum(b) as y FROM [//t] group by a % 200 as x with totals order by y limit 50",
        split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
        "a=-2;b=20",
        "a=9;b=90",
        "a=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2",
        "x=9",
        "x=10"
    }, resultSplit);

    Evaluate("abs_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestZeroArgumentUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
    });

    std::vector<TString> source = {
        "a=1u",
        "a=2u",
        "a=75u",
        "a=10u",
        "a=75u",
        "a=10u",
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "a=75u",
        "a=75u"
    }, resultSplit);

    Evaluate("a FROM [//t] where a = seventyfive()", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfImpl)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
    };

    EvaluateExpectingError("invalid_ir(a) as x FROM [//t]", split, source, EFailureLocation::Codegen);
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfArity)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
    };

    EvaluateExpectingError("abs_udf_arity(a, b) as x FROM [//t]", split, source, EFailureLocation::Codegen);
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfType)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;b=10",
    };

    EvaluateExpectingError("abs_udf_double(a) as x FROM [//t]", split, source, EFailureLocation::Codegen,
    std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());
}

TEST_F(TQueryEvaluateTest, TestUdfNullPropagation)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;",
        "a=-2;b=-20",
        "a=9;",
        "b=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "",
        "x=20",
        "",
        "x=10"
    }, resultSplit);

    Evaluate("abs_udf(b) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfNullPropagation2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1;",
        "a=2;b=10",
        "b=9",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "",
        "x=1024",
        "",
        ""
    }, resultSplit);

    Evaluate("exp_udf(a, b) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfStringArgument)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"123\"",
        "a=\"50\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "x=123u",
        "x=50u",
        "x=0u",
        ""
    }, resultSplit);

    Evaluate("strtol_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfStringResult)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"HELLO\"",
        "a=\"HeLlO\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String}
    });

    auto result = YsonToRows({
        "x=\"hello\"",
        "x=\"hello\"",
        "x=\"\"",
        ""
    }, resultSplit);

    Evaluate("tolower_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUnversionedValueUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"Hello\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%false",
        "x=%true"
    }, resultSplit);

    Evaluate("is_null_udf(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetInt64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Int64}
    });

    auto result = YsonToRows({
        "result=4",
        "result=2",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_int64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetInt64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Int64}
    });

    auto result = YsonToRows({
        "result=4",
        "result=2",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_int64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetInt64Fail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_int64(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("get_int64(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetUint64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "result=4u",
        "result=2u",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_uint64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetUint64)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "result=4u",
        "result=2u",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_uint64(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetUint64Fail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4u};d=[1u;2u]};ypath=\"/b/d\"",
        "yson={b={c=4u};d=[1u;2u]};ypath=\"/d/2\"",
        "yson={b={c=4u};d=[1u;2]};ypath=\"/d/1\"",
        "yson={b={c=4u}d=[1u;2u}};ypath=\"/d/1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"/d1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"//d/1\"",
        "yson={b={c=4u};d=[1u;2u}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_uint64(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("get_uint64(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetDouble)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4.};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2.]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Double}
    });

    auto result = YsonToRows({
        "result=4.",
        "result=2.",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_double(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetDouble)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4.};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;2.]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Double}
    });

    auto result = YsonToRows({
        "result=4.",
        "result=2.",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_double(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetDoubleFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_double(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("get_double(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetBoolean)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=%true};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;%false]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%true",
        "result=%false",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_boolean(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetBoolean)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=%false};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;%true]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%false",
        "result=%true",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_boolean(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetBooleanFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_boolean(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("get_boolean(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathTryGetString)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"hello\"};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;\"world\"]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"hello\"",
        "result=\"world\"",
        "",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("try_get_string(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetString)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"here\"};d=[1;2]};ypath=\"/b/c\"",
        "yson={b={c=4};d=[1;\"there\"]};ypath=\"/d/1\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::String}
    });

    auto result = YsonToRows({
        "result=\"here\"",
        "result=\"there\"",
        "",
        "",
        "",
    }, resultSplit);

    Evaluate("get_string(yson, ypath) as result FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetStringFail)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath", EValueType::String},
    });

    std::vector<TString> source = {
        "",
        "yson={b={c=4};d=[1;2]};ypath=\"/b/d\"",
        "yson={b={c=4};d=[1;2]};ypath=\"/d/2\"",
        "yson={b={c=4};d=[1;2u]};ypath=\"/d/1\"",
        "yson={b={c=4}d=[1;2}};ypath=\"/d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/d1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"//d/1\"",
        "yson={b={c=4};d=[1;2}};ypath=\"/@d/1\"",
    };

    EvaluateExpectingError("try_get_string(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("get_string(yson, ypath) as result FROM [//t]", split, source, EFailureLocation::Execution);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, YPathGetAny)
{
    auto split = MakeSplit({
        {"yson", EValueType::Any},
        {"ypath0", EValueType::String},
        {"ypath1", EValueType::String},
        {"value", EValueType::String},
    });

    std::vector<TString> source = {
        "yson={b={c=\"here\"};d=[1;2]};ypath0=\"/b\";ypath1=\"/c\";value=\"here\"",
        "yson={b={c=4};d=[1;\"there\"]};ypath0=\"/d\";ypath1=\"/1\";value=\"there\"",
        "",
        "yson={b={c=4};d=[1;2]}",
        "ypath0=\"/d/1\"",
    };

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "result=%true",
        "result=%true",
        "result=%true",
        "result=%true",
        "result=%true",
    }, resultSplit);

    Evaluate("get_any(get_any(yson, ypath0), ypath1) = value as result FROM [//t]",
        split,
        source,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, CompareAny)
{
    auto split = MakeSplit({
        {"a", EValueType::Any},
        {"b", EValueType::Any}
    });

    std::vector<TString> source = {
        "a=%false;b=%true;",
        "a=%false;b=%false;",
        "a=1;b=2;",
        "a=1;b=1;",
        "a=1u;b=2u;",
        "a=1u;b=1u;",
        "a=1.0;b=2.0;",
        "a=1.0;b=1.0;",
        "a=x;b=y;",
        "a=x;b=x;",
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean},
        {"r6", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false",
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5, a != b as r6 FROM [//t]",
        split,
        source,
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, CompareAnyMixed)
{
    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean},
        {"r6", EValueType::Boolean},
        {"r7", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false;r6=%true;r7=%true",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true;r6=%false;r7=%true",
    }, resultSplit);

    TString query = "a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5, a != b as r6, a < b = b "
        "> a and a > b = b < a as r7 FROM [//t]";

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Boolean},
        }), {
            "a=%false;b=%true;",
            "a=%false;b=%false;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Int64},
        }), {
            "a=1;b=2;",
            "a=1;b=1;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Uint64},
        }), {
            "a=1u;b=2u;",
            "a=1u;b=1u;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::Double},
        }), {
            "a=1.0;b=2.0;",
            "a=1.0;b=1.0;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Any},
            {"b", EValueType::String},
        }), {
            "a=x;b=y;",
            "a=x;b=x;"},
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ToAnyAndCompare)
{
    auto resultSplit = MakeSplit({
        {"r", EValueType::Boolean}
    });

    auto result = YsonToRows({
        "r=%true",
    }, resultSplit);

    TString query = "to_any(a) = a FROM [//t]";

    Evaluate(query, MakeSplit({
            {"a", EValueType::Boolean}
        }), {
            "a=%false;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Int64}
        }), {
            "a=1;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Uint64}
        }), {
            "a=1u;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::Double}
        }), {
            "a=1.0;"},
        ResultMatcher(result));

    Evaluate(query, MakeSplit({
            {"a", EValueType::String}
        }), {
            "a=x;"},
        ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestVarargUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=1",
        "a=2"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = YsonToRows({
        "x=1",
        "x=2"
    }, resultSplit);

    Evaluate("a as x FROM [//t] where sum_udf(7, 3, a) in (11u, 12)", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestFarmHash)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String},
        {"c", EValueType::Boolean}
    });

    std::vector<TString> source = {
        "a=3;b=\"hello\";c=%true",
        "a=54;c=%false"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = YsonToRows({
        "x=13185060272037541714u",
        "x=1607147011416532415u"
    }, resultSplit);

    Evaluate("farm_hash(a, b, c) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexParseError)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"hello\"",
        "a=\"hell\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    EvaluateExpectingError("regex_full_match(\"hel[a-z)\", a) as x FROM [//t]", split, source, EFailureLocation::Execution, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());
}

TEST_F(TQueryEvaluateTest, TestRegexFullMatch)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"hello\"",
        "a=\"hell\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    Evaluate("regex_full_match(\"hel[a-z]\", a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexPartialMatch)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"xx\"",
        "a=\"x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "x=%false",
        "x=%true",
        "x=%false",
    }, resultSplit);

    Evaluate("regex_partial_match(\"[0-9]+\", a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexReplaceFirst)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"x43x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"x_x43x\"",
        "",
    }, resultSplit);

    Evaluate("regex_replace_first(\"[0-9]+\", a, \"_\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexReplaceAll)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"x43x43x\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"x_x_x\"",
        "",
    }, resultSplit);

    Evaluate("regex_replace_all(\"[0-9]+\", a, \"_\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexExtract)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"Send root@ya.com an email.\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"root at ya\"",
        "",
    }, resultSplit);

    Evaluate("regex_extract(\"([a-z]*)@(.*).com\", a, \"\\\\1 at \\\\2\") as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestRegexEscape)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"1.5\"",
        "",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
    });

    auto result = YsonToRows({
        "x=\"1\\\\.5\"",
        "",
    }, resultSplit);

    Evaluate("regex_escape(a) as x FROM [//t]", split, source, ResultMatcher(result));

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestAverageAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3",
        "a=53",
        "a=8",
        "a=24",
        "a=33"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double}
    });

    auto result = YsonToRows({
        "x=24.2",
    }, resultSplit);

    Evaluate("avg(a) as x from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, TestAverageAgg2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3;b=3;c=1",
        "a=53;b=2;c=3",
        "a=8;b=5;c=32",
        "a=24;b=7;c=4",
        "a=33;b=4;c=9",
        "a=33;b=3;c=43",
        "a=23;b=0;c=0",
        "a=33;b=8;c=2"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Double},
        {"x", EValueType::Int64},
        {"r2", EValueType::Int64},
        {"r3", EValueType::Double},
        {"r4", EValueType::Int64},
    });

    auto result = YsonToRows({
        "r1=17.0;x=1;r2=43;r3=20.0;r4=3",
        "r1=35.5;x=0;r2=9;r3=3.5;r4=23"
    }, resultSplit);

    Evaluate("avg(a) as r1, x, max(c) as r2, avg(c) as r3, min(a) as r4 from [//t] group by b % 2 as x", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, TestAverageAgg3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source = {
        "a=3;b=1",
        "b=1",
        "b=0",
        "a=7;b=1",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"x", EValueType::Double}
    });

    auto result = YsonToRows({
        "b=1;x=5.0",
        "b=0"
    }, resultSplit);

    Evaluate("b, avg(a) as x from [//t] group by b", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, TestStringAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<TString> source = {
        "a=\"one\"",
        "a=\"two\"",
        "a=\"three\"",
        "a=\"four\"",
        "a=\"fo\"",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::String},
    });

    auto result = YsonToRows({
        "b=\"fo\";c=\"two\"",
    }, resultSplit);

    Evaluate("min(a) as b, max(a) as c from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, WronglyTypedAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<TString> source = {
        "a=\"\""
    };

    EvaluateExpectingError("avg(a) from [//t] group by 1", split, source, EFailureLocation::Codegen);
}

TEST_F(TQueryEvaluateTest, CardinalityAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 2000; j++) {
            source.push_back("a=" + ToString(j));
        }
    }

    auto resultSplit = MakeSplit({
        {"upper", EValueType::Boolean},
        {"lower", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "upper=%true;lower=%true"
    }, resultSplit);

    Evaluate("cardinality(a) < 2020 as upper, cardinality(a) > 1980 as lower from [//t] group by 1", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 2000; j++) {
            source.push_back("a=" + ToString(j));
        }
    }

    auto resultSplit = MakeSplit({
        {"upper", EValueType::Boolean},
        {"lower", EValueType::Boolean},
    });

    auto result = YsonToRows({
        "upper=%true;lower=%true",
        "upper=%true;lower=%true"
    }, resultSplit);

    Evaluate(
        "cardinality(a) < 2020 as upper, cardinality(a) > 1980 as lower from [//t] group by 1 with totals",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 12; i++) {
        for (int j = 0; j < 1000 * (i % 3 + 1); j++) {
            source.push_back("a=" + ToString(j) + "; b=" + ToString(i % 3 + 1));
        }
    }

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean},
        {"total", EValueType::Boolean},
        {"b", EValueType::Int64},
    });

    auto result = YsonToRows({
        "result=%true;total=%false;b=2",
        "result=%true;total=%true;b=3",
        "result=%true;total=%true"
    }, resultSplit);

    Evaluate(
        "(int64(cardinality(a)) - b * 1000) between (-b * 10) and (b * 10) as result,"
        "(int64(cardinality(a)) - 3000) between -30 and 30 as total, b "
        "from [//t] group by b having cardinality(a) > 1500 with totals",
        split,
        source,
        ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, CardinalityAggregateTotals3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<TString> source;
    for (int i = 0; i < 16; i++) {
        for (int j = 0; j < 500; j++) {
            source.push_back("a=" + ToString(j + 1000 * (i % 3)) + "; b=" + ToString(i % 3));
        }
    }

    auto resultSplit = MakeSplit({
        {"result", EValueType::Boolean},
        {"total", EValueType::Boolean},
        {"b", EValueType::Int64},
    });

    auto result = YsonToRows({
        "result=%true;total=%false;b=1",
        "result=%true;total=%false;b=2",
        "result=%false;total=%true"
    }, resultSplit);

    Evaluate(
        "int64(cardinality(a)) between 490 and 510 as result,"
        "int64(cardinality(a)) between 990 and 1010 as total, b "
        "from [//t] group by b having min(a) > 400 with totals",
        split,
        source,
        ResultMatcher(result));

}

TEST_F(TQueryEvaluateTest, TestCasts)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
        {"b", EValueType::Int64},
        {"c", EValueType::Double}
    });

    std::vector<TString> source = {
        "a=3u;b=34",
        "c=1.23",
        "a=12u",
        "b=0;c=1.0",
        "a=5u",
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Int64},
        {"r2", EValueType::Double},
        {"r3", EValueType::Uint64},
    });

    auto result = YsonToRows({
        "r1=3;r2=34.0",
        "r3=1u",
        "r1=12",
        "r2=0.0;r3=1u",
        "r1=5",
    }, resultSplit);

    Evaluate("int64(a) as r1, double(b) as r2, uint64(c) as r3 from [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, TestUdfException)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
    });

    std::vector<TString> source = {
        "a=-3",
    };

    auto resultSplit = MakeSplit({
        {"r", EValueType::Int64},
    });

    auto result = YsonToRows({
    }, resultSplit);

    EvaluateExpectingError("throw_if_negative_udf(a) from [//t]", split, source, EFailureLocation::Execution);
}

TEST_F(TQueryEvaluateTest, TestMakeMapSuccess)
{
    auto split = MakeSplit({
        {"v_any", EValueType::Any},
        {"v_null", EValueType::Any}
    });

    std::vector<TString> source = {
        "v_any={hello=world}"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Any},
    });

    auto result = YsonToRows({
        "x={"
        "  k_int=1;"
        "  k_uint=2u;"
        "  k_bool=%true;"
        "  k_double=3.14;"
        "  k_any={hello=world};"
        "  k_null=#;"
        "}",
    }, resultSplit);

    Evaluate(
        "make_map("
        "  \"k_int\", 1, "
        "  \"k_uint\", 2u, "
        "  \"k_bool\", %true, "
        "  \"k_double\", 3.14, "
        "  \"k_any\", v_any, "
        "  \"k_null\", v_null"
        ") as x FROM [//t]", split, source, ResultMatcher(result));
}

TEST_F(TQueryEvaluateTest, TestMakeMapFailure)
{
    auto split = MakeSplit({
        {"a", EValueType::Any}
    });

    std::vector<TString> source = {
        "a=1"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Any},
    });

    EvaluateExpectingError("make_map(\"a\") as x FROM [//t]", split, source, EFailureLocation::Execution);
    EvaluateExpectingError("make_map(1, 1) as x FROM [//t]", split, source, EFailureLocation::Execution);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT
