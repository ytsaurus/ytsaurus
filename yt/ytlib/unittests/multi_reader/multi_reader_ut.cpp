#include "mock/multi_reader_memory_manager.h"
#include "mock/multi_chunk_reader.h"
#include "mock/reader_factory.h"

#include <yt/core/test_framework/framework.h>

#include <util/random/shuffle.h>

namespace NYT::NChunkClient {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TMultiChunkReaderMockPtr CreateMultiReader(std::vector<IReaderBasePtr> readers)
{
    auto config = New<TMultiChunkReaderConfig>();
    auto options = New<TMultiChunkReaderOptions>();

    std::vector<IReaderFactoryPtr> factories;

    for (auto& reader : readers) {
        factories.push_back(New<TReaderFactoryMock>(std::move(reader)));
    }

    auto memoryManager = New<TMultiReaderMemoryManagerMock>();

    return New<TMultiChunkReaderMock>(
        std::move(config),
        std::move(options),
        std::move(factories),
        std::move(memoryManager));
}

std::vector<IReaderBasePtr> CreateMockReaders(int readerCount, int filledRowCount, int emptyRowCount = 0)
{
    std::vector<IReaderBasePtr> readers;

    int filledRowValue = 0;
    for (int readerIndex = 0; readerIndex < readerCount; ++readerIndex) {
        std::vector<std::vector<TUnversionedOwningRow>> readerData;
        for (int i = 0; i < filledRowCount; ++i) {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(filledRowValue++));
            readerData.push_back({builder.FinishRow()});
        }
        for (int i = 0; i < emptyRowCount; ++i) {
            readerData.push_back({});
        }
        Shuffle(readerData.begin(), readerData.end());
        readers.emplace_back(New<TChunkReaderMock>(std::move(readerData)));
    }
    return readers;
}

IReaderBasePtr CreateReaderWithError(int filledRowCount)
{
    std::vector<std::vector<TUnversionedOwningRow>> readerData;
    for (int rowIndex = 0; rowIndex < filledRowCount; ++rowIndex) {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(rowIndex + 100));
        readerData.push_back({builder.FinishRow()});
    }
    return New<TChunkReaderWithErrorMock>(std::move(readerData));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

class TMultiReaderTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, const char*>>
{
protected:
    virtual void SetUp() override
    { }
};

TEST(TMultiReaderTest, DataWithEmptyRows)
{
    auto multiReader = CreateMultiReader(CreateMockReaders(/*readerCount =*/ 10, /*filledRowCount =*/ 10, /*emptyRowCount =*/ 10));

    multiReader->Open();

    std::unordered_set<i64> values;
    for (int i = 0; i < 100; ++i) {
        values.insert(i);
    }

    std::vector<TUnversionedRow> readRows;
    while (multiReader->Read(&readRows)) {
        if (readRows.empty()) {
            WaitFor(multiReader->GetReadyEvent())
                .ThrowOnError();
                continue;
        }
        EXPECT_EQ(1, readRows.size());
        auto value = readRows.front()[0].Data.Int64;
        EXPECT_NE(values.end(), values.find(value));
        values.erase(value);
    }

    EXPECT_EQ(0, values.size());
}

TEST(TMultiReaderTest, ReaderWithError)
{
    auto readers = CreateMockReaders(/*readerCount =*/ 2, /*filledRowCount =*/ 5);
    readers.emplace_back(CreateReaderWithError(/*filledRowCount =*/ 5));

    auto multiReader = CreateMultiReader(std::move(readers));

    multiReader->Open();

    std::vector<TUnversionedRow> readRows;
    for (int i = 0; i < 15; ++i) {
        readRows.clear();
        while (readRows.empty()) {
            multiReader->Read(&readRows);
            if (readRows.empty()) {
                WaitFor(multiReader->GetReadyEvent())
                    .ThrowOnError();
                    continue;
            }
        }
    }
    while (multiReader->GetFailedChunkIds().empty() && multiReader->Read(&readRows)) {
        if (readRows.empty()) {
            auto error = WaitFor(multiReader->GetReadyEvent());
            if (error.IsOK()) {
                EXPECT_EQ(multiReader->GetFailedChunkIds().size(), 0);
            }
        }
    }
    EXPECT_EQ(multiReader->GetFailedChunkIds().size(), 1);
    EXPECT_FALSE(WaitFor(multiReader->GetReadyEvent()).IsOK());
}

TEST(TMultiReaderTest, Interrupt)
{
    auto readers = CreateMockReaders(/*readerCount =*/ 5, /*filledRowCount =*/ 10);

    auto multiReader = CreateMultiReader(std::move(readers));

    multiReader->Open();

    std::vector<TUnversionedRow> readRows;
    for (int i = 0; i < 15; ++i) {
        readRows.clear();
        while (multiReader->Read(&readRows)) {
            if (readRows.empty()) {
                WaitFor(multiReader->GetReadyEvent())
                    .ThrowOnError();
                    continue;
            } else {
                break;
            }
        }
    }

    multiReader->Interrupt();

    WaitFor(multiReader->GetReadyEvent())
        .ThrowOnError();
    int remainingRowCount = 0;
    while (multiReader->Read(&readRows)) {
        if (readRows.empty()) {
            WaitFor(multiReader->GetReadyEvent())
                .ThrowOnError();
                continue;
        }
        ++remainingRowCount;
    }
    EXPECT_EQ(5, remainingRowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
