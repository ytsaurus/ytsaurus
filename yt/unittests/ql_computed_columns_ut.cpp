#include "stdafx.h"
#include "framework.h"

#include "ql_helpers.h"

#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/query_preparer.h>

#include <ytlib/query_client/column_evaluator.h>
#include <ytlib/query_client/config.h>

// Tests:
// TComputedColumnTest

namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TComputedColumnTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::vector<const char*>>
{
protected:
    virtual void SetUp() override
    {
        SetUpSchema();

        EXPECT_CALL(PrepareMock_, GetInitialSplit(_, _))
            .WillRepeatedly(Invoke(this, &TComputedColumnTest::MakeSimpleSplit));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(config, CreateBuiltinFunctionRegistry());
    }

    std::vector<TKeyRange> Coordinate(const Stroka& source)
    {
        auto planFragment = PreparePlanFragment(&PrepareMock_, source, CreateBuiltinFunctionRegistry());
        auto rowBuffer = New<TRowBuffer>();
        auto prunedSplits = GetPrunedRanges(
            planFragment->Query,
            planFragment->DataSources,
            rowBuffer,
            ColumnEvaluatorCache_,
            CreateBuiltinFunctionRegistry(),
            1000,
            true);

        return GetRangesFromSources(prunedSplits);
    }

    std::vector<TKeyRange> CoordinateForeign(const Stroka& source)
    {
        auto planFragment = PreparePlanFragment(&PrepareMock_, source, CreateBuiltinFunctionRegistry());

        const auto& query = planFragment->Query;

        TDataSources foreignSplits{{query->JoinClauses[0]->ForeignDataId, {
                planFragment->KeyRangesRowBuffer->Capture(MinKey().Get()),
                planFragment->KeyRangesRowBuffer->Capture(MaxKey().Get())}
            }};

        auto rowBuffer = New<TRowBuffer>();
        auto prunedSplits = GetPrunedRanges(
            query->WhereClause,
            query->JoinClauses[0]->ForeignTableSchema,
            TableSchemaToKeyColumns(
                query->JoinClauses[0]->RenamedTableSchema,
                query->JoinClauses[0]->ForeignKeyColumnsCount),
            foreignSplits,
            rowBuffer,
            ColumnEvaluatorCache_,
            CreateBuiltinFunctionRegistry(),
            1000,
            true);

        return GetRangesFromSources(prunedSplits);
    }

    void SetSchema(const TTableSchema& schema, const TKeyColumns& keyColumns)
    {
        Schema_ = schema;
        KeyColumns_ = keyColumns;
    }

    void SetSecondarySchema(const TTableSchema& schema, const TKeyColumns& keyColumns)
    {
        SecondarySchema_ = schema;
        SecondaryKeyColumns_ = keyColumns;
    }

private:
    void SetUpSchema()
    {
        TTableSchema tableSchema;
        tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("l * 2"));
        tableSchema.Columns().emplace_back("l", EValueType::Int64);
        tableSchema.Columns().emplace_back("m", EValueType::Int64);
        tableSchema.Columns().emplace_back("a", EValueType::Int64);

        TKeyColumns keyColumns{"k", "l", "m"};

        SetSchema(tableSchema, keyColumns);
    }

    TFuture<TDataSplit> MakeSimpleSplit(const TYPath& path, ui64 counter = 0)
    {
        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, counter, 0xdeadbabe));

        if (path == "//t") {
            SetKeyColumns(&dataSplit, KeyColumns_);
            SetTableSchema(&dataSplit, Schema_);
        } else {
            SetKeyColumns(&dataSplit, SecondaryKeyColumns_);
            SetTableSchema(&dataSplit, SecondarySchema_);
        }

        return WrapInFuture(dataSplit);
    }

    std::vector<TKeyRange> GetRangesFromSources(const TGroupedRanges& groupedRanges)
    {
        std::vector<TKeyRange> ranges;

        for (const auto& group : groupedRanges) {
            for (const auto& range : group) {
                ranges.push_back(TKeyRange(TOwningKey(range.first), TOwningKey(range.second)));
            }
        }

        std::sort(ranges.begin(), ranges.end());
        return ranges;
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    TTableSchema Schema_;
    TKeyColumns KeyColumns_;
    TTableSchema SecondarySchema_;
    TKeyColumns SecondaryKeyColumns_;
};

TEST_F(TComputedColumnTest, NoKeyColumnsInPredicate)
{
    auto query = Stroka("k from [//t] where a = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey(_MIN_), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Simple)
{
    auto query = Stroka("a from [//t] where l = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("20;10;"), result[0].first);
    EXPECT_EQ(BuildKey("20;10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Inequality)
{
    auto query = Stroka("a from [//t] where l < 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey(_MIN_), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Composite)
{
    auto query = Stroka("a from [//t] where l = 10 and m > 0 and m < 50");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("20;10;0;" _MAX_), result[0].first);
    EXPECT_EQ(BuildKey("20;10;50;"), result[0].second);
}

TEST_F(TComputedColumnTest, Vector)
{
    auto query = Stroka("a from [//t] where l in (1,2,3)");
    auto result = Coordinate(query);

    EXPECT_EQ(3, result.size());

    EXPECT_EQ(BuildKey("2;1;"), result[0].first);
    EXPECT_EQ(BuildKey("2;1;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("4;2;"), result[1].first);
    EXPECT_EQ(BuildKey("4;2;" _MAX_), result[1].second);
    EXPECT_EQ(BuildKey("6;3;"), result[2].first);
    EXPECT_EQ(BuildKey("6;3;" _MAX_), result[2].second);
}

TEST_F(TComputedColumnTest, ComputedKeyInPredicate)
{
    auto query = Stroka("a from [//t] where (k,l) >= (10,20) ");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("10;20;"), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, ComputedColumnLast)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64);
    tableSchema.Columns().emplace_back("l", EValueType::Int64, Null, Stroka("k + 3"));
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where k = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("10;13;"), result[0].first);
    EXPECT_EQ(BuildKey("10;13;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Complex1)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64);
    tableSchema.Columns().emplace_back("l", EValueType::Int64, Null, Stroka("n + 1"));
    tableSchema.Columns().emplace_back("m", EValueType::Int64, Null, Stroka("o + 2"));
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("o", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n", "o"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where k = 10 and n = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("10;21;"), result[0].first);
    EXPECT_EQ(BuildKey("10;21;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Complex2)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64);
    tableSchema.Columns().emplace_back("l", EValueType::Int64, Null, Stroka("n + 1"));
    tableSchema.Columns().emplace_back("m", EValueType::Int64, Null, Stroka("o + 2"));
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("o", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n", "o"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where (k,n) in ((10,20),(50,60))");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey("10;21;"), result[0].first);
    EXPECT_EQ(BuildKey("10;21;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("50;61;"), result[1].first);
    EXPECT_EQ(BuildKey("50;61;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, Complex3)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64);
    tableSchema.Columns().emplace_back("l", EValueType::Int64, Null, Stroka("o + 1"));
    tableSchema.Columns().emplace_back("m", EValueType::Int64, Null, Stroka("o + 2"));
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("o", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n", "o"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where k = 10 and n = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("10;"), result[0].first);
    EXPECT_EQ(BuildKey("10;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far1)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("m + 1"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where m = 10");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("11;"), result[0].first);
    EXPECT_EQ(BuildKey("11;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far2)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("n + 1"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where n = 10 and l = 20");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey("11;20;"), result[0].first);
    EXPECT_EQ(BuildKey("11;20;" _MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Far3)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("n + 1"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where (n,l) in ((10,20), (30,40))");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey("11;20;"), result[0].first);
    EXPECT_EQ(BuildKey("11;20;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("31;40;"), result[1].first);
    EXPECT_EQ(BuildKey("31;40;" _MAX_), result[1].second);
}

TEST_F(TComputedColumnTest, Far4)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("n + 1"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("n", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where n in (10,30) and l in (20,40)");
    auto result = Coordinate(query);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(BuildKey("11;20;"), result[0].first);
    EXPECT_EQ(BuildKey("11;20;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("11;40;"), result[1].first);
    EXPECT_EQ(BuildKey("11;40;" _MAX_), result[1].second);
    EXPECT_EQ(BuildKey("31;20;"), result[2].first);
    EXPECT_EQ(BuildKey("31;20;" _MAX_), result[2].second);
    EXPECT_EQ(BuildKey("31;40;"), result[3].first);
    EXPECT_EQ(BuildKey("31;40;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, NoComputedColumns)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64);
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where a = 0");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey(_MIN_), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Modulo0)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("l % 2"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where a = 0");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey(_MIN_), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Modulo1)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("l % 2"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where l > 0 and l <= 2000");
    auto result = Coordinate(query);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(BuildKey(_NULL_ ";0;" _MAX_), result[0].first);
    EXPECT_EQ(BuildKey(_NULL_ ";2000;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("-1;0;" _MAX_), result[1].first);
    EXPECT_EQ(BuildKey("-1;2000;" _MAX_), result[1].second);
    EXPECT_EQ(BuildKey("0;0;" _MAX_), result[2].first);
    EXPECT_EQ(BuildKey("0;2000;" _MAX_), result[2].second);
    EXPECT_EQ(BuildKey("1;0;" _MAX_), result[3].first);
    EXPECT_EQ(BuildKey("1;2000;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Modulo2)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Uint64, Null, Stroka("n % 1u"));
    tableSchema.Columns().emplace_back("l", EValueType::Uint64, Null, Stroka("n % 1u"));
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("n", EValueType::Uint64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m", "n"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where m = 1");
    auto result = Coordinate(query);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(BuildKey(_NULL_ ";" _NULL_ ";1;"), result[0].first);
    EXPECT_EQ(BuildKey(_NULL_ ";" _NULL_ ";1;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey(_NULL_ ";0u;1;"), result[1].first);
    EXPECT_EQ(BuildKey(_NULL_ ";0u;1;" _MAX_), result[1].second);
    EXPECT_EQ(BuildKey("0u;" _NULL_ ";1;"), result[2].first);
    EXPECT_EQ(BuildKey("0u;" _NULL_ ";1;" _MAX_), result[2].second);
    EXPECT_EQ(BuildKey("0u;0u;1;"), result[3].first);
    EXPECT_EQ(BuildKey("0u;0u;1;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Modulo3)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Uint64, Null, Stroka("m % 1u"));
    tableSchema.Columns().emplace_back("l", EValueType::Uint64, Null, Stroka("m % 1u"));
    tableSchema.Columns().emplace_back("m", EValueType::Uint64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t]");
    auto result = Coordinate(query);

    EXPECT_EQ(1, result.size());

    EXPECT_EQ(BuildKey(_MIN_), result[0].first);
    EXPECT_EQ(BuildKey(_MAX_), result[0].second);
}

TEST_F(TComputedColumnTest, Divide1)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("l / 2"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where l >= 3 and l < 6");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey("1;3"), result[0].first);
    EXPECT_EQ(BuildKey("1;4"), result[0].second);
    EXPECT_EQ(BuildKey("2;4"), result[1].first);
    EXPECT_EQ(BuildKey("2;6"), result[1].second);
}

TEST_F(TComputedColumnTest, Divide2)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("m / 3"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64, Null, Stroka("m / 4"));
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where m > 0 and m <= 6");
    auto result = Coordinate(query);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(BuildKey("0;0;0;" _MAX_), result[0].first);
    EXPECT_EQ(BuildKey("0;0;3"), result[0].second);
    EXPECT_EQ(BuildKey("1;0;3"), result[1].first);
    EXPECT_EQ(BuildKey("1;0;4"), result[1].second);
    EXPECT_EQ(BuildKey("1;1;4"), result[2].first);
    EXPECT_EQ(BuildKey("1;1;6"), result[2].second);
    EXPECT_EQ(BuildKey("2;1;6"), result[3].first);
    EXPECT_EQ(BuildKey("2;1;6;" _MAX_), result[3].second);
}

TEST_F(TComputedColumnTest, Divide3)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Uint64, Null, Stroka("m / 2u"));
    tableSchema.Columns().emplace_back("l", EValueType::Uint64, Null, Stroka("n % 1u"));
    tableSchema.Columns().emplace_back("m", EValueType::Uint64);
    tableSchema.Columns().emplace_back("n", EValueType::Uint64);
    tableSchema.Columns().emplace_back("a", EValueType::Uint64);

    TKeyColumns keyColumns{"k", "l", "m", "n"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where m >= 0u and m < 3u");
    auto result = Coordinate(query);

    EXPECT_EQ(4, result.size());

    EXPECT_EQ(BuildKey("0u;" _NULL_ ";0u"), result[0].first);
    EXPECT_EQ(BuildKey("0u;" _NULL_ ";2u"), result[0].second);
    EXPECT_EQ(BuildKey("0u;0u;0u"), result[1].first);
    EXPECT_EQ(BuildKey("0u;0u;2u"), result[1].second);
    EXPECT_EQ(BuildKey("1u;" _NULL_ ";2u"), result[2].first);
    EXPECT_EQ(BuildKey("1u;" _NULL_ ";3u"), result[2].second);
    EXPECT_EQ(BuildKey("1u;0u;2u"), result[3].first);
    EXPECT_EQ(BuildKey("1u;0u;3u"), result[3].second);
}

TEST_F(TComputedColumnTest, Divide4)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("l / -9223372036854775808"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where l >= -9223372036854775808 and l <= 9223372036854775807");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey("0;0;"), result[0].first);
    EXPECT_EQ(BuildKey("0;9223372036854775807;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("1;-9223372036854775808"), result[1].first);
    EXPECT_EQ(BuildKey("1;0;"), result[1].second);
}

TEST_F(TComputedColumnTest, FarDivide1)
{
    TTableSchema tableSchema;
    tableSchema.Columns().emplace_back("k", EValueType::Int64, Null, Stroka("m / 2"));
    tableSchema.Columns().emplace_back("l", EValueType::Int64);
    tableSchema.Columns().emplace_back("m", EValueType::Int64);
    tableSchema.Columns().emplace_back("a", EValueType::Int64);

    TKeyColumns keyColumns{"k", "l", "m"};

    SetSchema(tableSchema, keyColumns);

    auto query = Stroka("a from [//t] where m >= 3 and m < 5");
    auto result = Coordinate(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey("1"), result[0].first);
    EXPECT_EQ(BuildKey("1;" _MAX_), result[0].second);
    EXPECT_EQ(BuildKey("2"), result[1].first);
    EXPECT_EQ(BuildKey("2;" _MAX_), result[1].second);
}

TEST_P(TComputedColumnTest, Join)
{
    const auto& args = GetParam();
    const auto& schemaString1 = args[0];
    const auto& schemaString2 = args[2];
    const auto& keyString1 = args[1];
    const auto& keyString2 = args[3];

    TTableSchema tableSchema1;
    TTableSchema tableSchema2;
    Deserialize(tableSchema1, ConvertToNode(TYsonString(schemaString1)));
    Deserialize(tableSchema2, ConvertToNode(TYsonString(schemaString2)));

    TKeyColumns keyColumns1;
    TKeyColumns keyColumns2;
    Deserialize(keyColumns1, ConvertToNode(TYsonString(keyString1)));
    Deserialize(keyColumns2, ConvertToNode(TYsonString(keyString2)));

    SetSchema(tableSchema1, keyColumns1);
    SetSecondarySchema(tableSchema2, keyColumns2);

    auto query = Stroka("l from [//t] join [//t1] using l where l in (0, 1)");
    auto result = CoordinateForeign(query);

    EXPECT_EQ(2, result.size());

    EXPECT_EQ(BuildKey(args[4]), result[0].first);
    EXPECT_EQ(BuildKey(args[5]), result[0].second);
    EXPECT_EQ(BuildKey(args[6]), result[1].first);
    EXPECT_EQ(BuildKey(args[7]), result[1].second);
}

INSTANTIATE_TEST_CASE_P(
    TComputedColumnTest,
    TComputedColumnTest,
    ::testing::Values(
        std::vector<const char*>{
            "[{name=k;type=int64;expression=l}; {name=l;type=int64}; {name=a;type=int64}]",
            "[k;l]",
            "[{name=n;type=int64;expression=l}; {name=l;type=int64}; {name=b;type=int64}]",
            "[n;l]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64}; {name=a;type=int64}]",
            "[l]",
            "[{name=l;type=int64}; {name=b;type=int64}]",
            "[l]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64;expression=k}; {name=k;type=int64}; {name=a;type=int64}]",
            "[l;k]",
            "[{name=l;type=int64}; {name=b;type=int64}]",
            "[l]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64}; {name=a;type=int64}]",
            "[l]",
            "[{name=n;type=int64;expression=l}; {name=l;type=int64}; {name=b;type=int64}]",
            "[n;l]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64}; {name=a;type=int64}]",
            "[l]",
            "[{name=l;type=int64;expression=n}; {name=n;type=int64}; {name=b;type=int64}]",
            "[l;n]",
            "0;",
            "0;" _MAX_,
            "1;",
            "1;" _MAX_},
        std::vector<const char*>{
            "[{name=l;type=int64}; {name=a;type=int64}]",
            "[l]",
            "[{name=l;type=int64}; {name=n;type=int64;expression=l}; {name=b;type=int64}]",
            "[l;n]",
            "0;0;",
            "0;0;" _MAX_,
            "1;1;",
            "1;1;" _MAX_}
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT