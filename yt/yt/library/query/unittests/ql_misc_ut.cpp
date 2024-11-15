#include "ql_helpers.h"

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

    auto largeString = TString();
    largeString.resize(600_KB);

    auto data = std::vector<TPIValue>(dataLength);
    MakePositionIndependentStringValue(&data[0], largeString);

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

} // namespace
} // namespace NYT::NQueryClient
