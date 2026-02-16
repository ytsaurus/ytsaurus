#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/partition_chunk_reader.h>
#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/misc/range_helpers.h>

namespace NYT::NCppTests {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;

using namespace testing;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionReaderTestParams
{
    int PartitionCount{};
    TPartitionTags PartitionTags;
    std::vector<std::vector<i64>> ChunksRowsData;
    std::vector<i64> ExpectedValues;
};

class TPartitionMultiChunkReaderTest
    : public TApiTestBase
    , public WithParamInterface<TPartitionReaderTestParams>
{
protected:
    TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector{TColumnSchema("partition", ESimpleLogicalValueType::Int64)});
    TNameTablePtr NameTable_ = TNameTable::FromSchema(*Schema_);

    NApi::ITransactionPtr Transaction_ = WaitFor(Client_->StartTransaction(ETransactionType::Master, {}))
        .ValueOrThrow();

    NApi::NNative::IClientPtr NativeClient_ = DynamicPointerCast<NApi::NNative::IClient>(Client_);

    void TearDown() override
    {
        WaitFor(Transaction_->Commit())
            .ThrowOnError();
    }
};

TEST_P(TPartitionMultiChunkReaderTest, WithManyPartitionTags)
{
    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;

    for (const auto& rowsData : GetParam().ChunksRowsData) {
        auto options = New<TTableWriterOptions>();
        options->Account = "tmp";

        ISchemalessMultiChunkWriterPtr writer = CreatePartitionMultiChunkWriter(
            New<TTableWriterConfig>(),
            options,
            NameTable_,
            Schema_,
            NativeClient_,
            /*localHostName*/ {},
            CellTagFromId(Transaction_->GetId()),
            Transaction_->GetId(),
            NullTableSchemaId,
            NullChunkListId,
            CreateColumnBasedPartitioner(GetParam().PartitionCount, NameTable_->GetId("partition")),
            /*dataSink*/ {},
            /*writeBlocksOptions*/ {});

        auto makeRow = [&] (i64 partition) {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(partition, NameTable_->GetId("partition")));
            return builder.FinishRow();
        };

        auto owningRows = TransformRangeTo<std::vector<TUnversionedOwningRow>>(rowsData, makeRow);
        auto rows = TransformRangeTo<std::vector<TUnversionedRow>>(owningRows, &TUnversionedOwningRow::Get);

        ASSERT_TRUE(writer->Write(TRange(rows)));

        WaitFor(writer->Close())
            .ThrowOnError();

        ASSERT_THAT(writer->GetWrittenChunkSpecs(), SizeIs(1));
        chunkSpecs.push_back(writer->GetWrittenChunkSpecs().front());
    }

    auto sourceDirectory = New<TDataSourceDirectory>();
    auto& source = sourceDirectory->DataSources().emplace_back(New<TDataSource>());
    source->Schema() = Schema_;

    TPartitionMultiChunkReaderPtr reader = CreatePartitionMultiChunkReader(
        New<TMultiChunkReaderConfig>(),
        New<TMultiChunkReaderOptions>(),
        New<TChunkReaderHost>(NativeClient_),
        sourceDirectory,
        TransformRangeTo<std::vector<TDataSliceDescriptor>>(
            chunkSpecs,
            [] (const auto& chunkSpec) { return TDataSliceDescriptor(chunkSpec); }),
        NameTable_,
        GetParam().PartitionTags,
        /*chunkReadOptions*/ {},
        /*multiReaderMemoryManager*/ {});

    std::vector<TRowDescriptor> descriptors;

    auto getDescriptors = [&] {
        i64 readCount = 0;
        std::vector<TUnversionedOwningValue> values;
        auto valuesInserter = std::back_inserter(values);

        descriptors.clear();
        auto descriptorsInserter = std::back_inserter(descriptors);

        WaitFor(reader->GetReadyEvent())
            .ThrowOnError();

        return reader->Read(valuesInserter, descriptorsInserter, &readCount);
    };

    std::vector<i64> readRowsValues;
    TChunkedMemoryPool pool;

    auto readRows = [&] {
        if (descriptors.empty()) {
            return;
        }

        auto blockReader = descriptors.front().BlockReader;

        ASSERT_THAT(
            descriptors,
            Each(Field(&TRowDescriptor::BlockReader, blockReader)));

        if (!blockReader->JumpToRowIndex(0)) {
            return;
        }

        do {
            auto row = blockReader->GetRow(&pool, false);
            ASSERT_THAT(row.GetCount(), 1);
            if (row[0].Type == EValueType::Int64) {
                readRowsValues.push_back(row[0].Data.Int64);
            }
        } while (blockReader->NextRow());
    };

    while (getDescriptors()) {
        readRows();
    }
    EXPECT_THAT(descriptors, IsEmpty());
    EXPECT_THAT(readRowsValues, UnorderedElementsAreArray(GetParam().ExpectedValues));
}

INSTANTIATE_TEST_SUITE_P(
    NTestPartitionMultiReader,
    TPartitionMultiChunkReaderTest,
    Values(
        // One chunk.
        TPartitionReaderTestParams{
            .PartitionCount = 3,
            .PartitionTags = {0},
            .ChunksRowsData = {{0}},
            .ExpectedValues = {0}},
        TPartitionReaderTestParams{
            .PartitionCount = 3,
            .PartitionTags = {0, 2},
            .ChunksRowsData = {{0, 1, 2}},
            .ExpectedValues = {0, 2}},
        TPartitionReaderTestParams{
            .PartitionCount = 3,
            .PartitionTags = {1},
            .ChunksRowsData = {{0, 2}},
            .ExpectedValues = {}},
        TPartitionReaderTestParams{
            .PartitionCount = 3,
            .PartitionTags = {1},
            .ChunksRowsData = {{0, 0, 0, 1, 1, 1, 2, 2}},
            .ExpectedValues = {1, 1, 1}},
        // Two chunks.
        TPartitionReaderTestParams{
            .PartitionCount = 3,
            .PartitionTags = {2, 0},
            .ChunksRowsData = {{0, 0, 1}, {2, 2, 1}, {0, 1, 2}},
            .ExpectedValues = {0, 0, 0, 2, 2, 2}}));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
