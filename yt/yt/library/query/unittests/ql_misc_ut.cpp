#include <yt/yt/library/query/unittests/evaluate/ql_helpers.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine_api/top_collector.h>

#include <util/random/shuffle.h>

// Tests:
// TSelfifyEvaluatedExpressionTest
// TTopCollectorTest

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

char CompareI64(const TPIValue* first, const TPIValue* second)
{
    return first[0].Data.Int64 < second[0].Data.Int64;
}

char CompareString(const TPIValue* first, const TPIValue* second)
{
    return first[0].AsStringBuf() < second[0].AsStringBuf();
}

TEST(TTopCollectorTest, Simple)
{
    const int dataLength = 100;
    const int resultLength = 20;

    auto data = std::vector<TPIValue>(dataLength);
    for (int i = 0; i < dataLength; ++i) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = dataLength - i - 1;
    }

    auto topCollector = TTopCollector(
        resultLength,
        NWebAssembly::PrepareFunction(&CompareI64),
        /*rowSize*/ 1,
        GetDefaultMemoryChunkProvider());

    for (int i = 0; i < dataLength; ++i) {
        topCollector.AddRow(&data[i]);
    }

    auto sortResult = topCollector.GetRows();
    for (int i = 0; i < resultLength; ++i) {
        EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
        EXPECT_EQ(sortResult[i][0].Data.Int64, i);
    }
}

TEST(TTopCollectorTest, Shuffle)
{
    const int dataLength = 2000;
    const int resultLength = 100;

    auto data = std::vector<TValue>(dataLength);
    for (int i = 0; i < dataLength; i += 2) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = i;
        data[i+1].Type = EValueType::Int64;
        data[i+1].Data.Int64 = i;
    }

    for (int i = 0; i < 1000; ++i) {
        Shuffle(data.begin(), data.end());

        auto topCollector = TTopCollector(
            resultLength,
            NWebAssembly::PrepareFunction(&CompareI64),
            /*rowSize*/ 1,
            GetDefaultMemoryChunkProvider());

        for (int i = 0; i < dataLength; ++i) {
            topCollector.AddRow(std::bit_cast<TPIValue*>(&data[i]));
        }

        auto sortResult = topCollector.GetRows();
        for (int i = 0; i < resultLength; i += 2) {
            EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i][0].Data.Int64, i);
            EXPECT_EQ(sortResult[i+1][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i+1][0].Data.Int64, i);
        }
    }
}

TEST(TTopCollectorTest, LargeRow)
{
    i64 dataLength = 1;

    auto topCollector = TTopCollector(
        2,
        NWebAssembly::PrepareFunction(&CompareString),
        /*rowSize*/ 1,
        GetDefaultMemoryChunkProvider());

    auto largeString = std::string();
    largeString.resize(600_KB);

    auto data = std::vector<TPIValue>(dataLength);
    MakePositionIndependentStringLikeValue(&data[0], EValueType::String, largeString);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'c', 600_KB);
    topCollector.AddRow(&data[0]);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'b', 600_KB);
    topCollector.AddRow(&data[0]);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'a', 600_KB);
    topCollector.AddRow(&data[0]);

    auto sortResult = topCollector.GetRows();

    EXPECT_EQ(sortResult[0][0].AsStringBuf()[0], 'a');
    EXPECT_EQ(sortResult[1][0].AsStringBuf()[0], 'b');
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTypingTest, TypeInference)
{
    TTypingCtx typingCtx;

    // auto printSignature = [&] (std::vector<TTypeId> result) {
    //     Cout << Format("Signature: %v", MakeFormattableView(
    //         result,
    //         [&] (TStringBuilderBase* builder, const auto& typeId) {
    //             if (typeId >= 0) {
    //                 builder->AppendString(ToString(*typingCtx.GetLogicalType(typeId)));
    //             } else {
    //                 builder->AppendFormat("%v", typeId);
    //             }
    //         })) << Endl;
    // };

    {
        auto result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Null), typingCtx.GetTypeId(EValueType::Null)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Null));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Null));

        result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Uint64)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Uint64));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Uint64));

        result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Null)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Int64));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Int64));


        result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Any)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Any));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Any));

        result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Any), typingCtx.GetTypeId(EValueType::Null)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Any));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Any));

        result = typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Any), typingCtx.GetTypeId(EValueType::Any)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Any));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Any));

        EXPECT_THROW(
            typingCtx.InferFunctionType("=", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::String)}),
            NYT::TErrorException);
    }

    {
        auto result = typingCtx.InferFunctionType("+", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Int64)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Int64));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Int64));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Int64));
    }

    {
        auto result = typingCtx.InferFunctionType("+", {typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Double)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Double));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Double));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Double));
    }

    {
        {
            TTypingCtx::TFunctionSignatures signatures;
            signatures.push_back(TTypingCtx::TFunctionSignature({-1, typingCtx.GetTypeId(EValueType::Boolean), -1, -1}));
            typingCtx.RegisterFunction("if", std::move(signatures));
        }

        auto result = typingCtx.InferFunctionType("if", {typingCtx.GetTypeId(EValueType::Boolean), typingCtx.GetTypeId(EValueType::Int64), typingCtx.GetTypeId(EValueType::Double)});
        EXPECT_EQ(result[0], typingCtx.GetTypeId(EValueType::Double));
        EXPECT_EQ(result[1], typingCtx.GetTypeId(EValueType::Boolean));
        EXPECT_EQ(result[2], typingCtx.GetTypeId(EValueType::Double));
        EXPECT_EQ(result[3], typingCtx.GetTypeId(EValueType::Double));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFingerprintTest, GroupByReferencesAreResolved)
{
    StrictMock<TPrepareCallbacksMock> prepareMock;
    EXPECT_CALL(prepareMock, GetInitialSplit("//table"))
        .WillOnce(Return(MakeFuture(MakeSplit({
            {"a", EValueType::Int64},
            {"b", EValueType::Int64},
        }))));
    auto query = ParseAndPreparePlanFragment(
        &prepareMock,
        "SELECT x + 4 as k, b + 5 as l, sum(a % 3) as m FROM [//table] "
        "GROUP BY a / 2 as x, b + 3 as b HAVING k != 4 ORDER BY m + 5 LIMIT 1")->Query;

    auto fingerprint = InferName(query, TInferNameOptions{.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT a / ? + ? AS k, b + ? + ? AS l, sum(a % ?) AS m "
        "GROUP BY [common prefix: 0, disjoint: False, aggregates: [sum]] a / ?, b + ? "
        "HAVING a / ? + ? != ? ORDER BY sum(a % ?) + ? ASC LIMIT ?");
}

TEST(TFingerprintTest, NestedDataSubqueries)
{
    if (DefaultExpressionBuilderVersion != 2) {
        return;
    }

    StrictMock<TPrepareCallbacksMock> prepareMock;
    EXPECT_CALL(prepareMock, GetInitialSplit("//table"))
        .WillOnce(Return(MakeFuture(MakeSplit({
            {"a", EValueType::Int64},
            {"b", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
        }))));

    auto query = ParseAndPreparePlanFragment(
        &prepareMock,
        "SELECT first(try_get_int64((SELECT sum(i + 1) + 2 as x FROM (b as i) GROUP BY 3), '/0/x')) + 4 as nested FROM [//table] GROUP BY 5")->Query;

    auto fingerprint = InferName(query, TInferNameOptions{.OmitValues = true});

    EXPECT_EQ(fingerprint, "SELECT first(try_get_int64(("
        "SELECT sum(i + ?) + ? AS x FROM (b AS i) GROUP BY [common prefix: 0, aggregates: [sum]] ?"
        "), ?)) + ? AS nested GROUP BY [common prefix: 0, disjoint: False, aggregates: [first]] ?");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
