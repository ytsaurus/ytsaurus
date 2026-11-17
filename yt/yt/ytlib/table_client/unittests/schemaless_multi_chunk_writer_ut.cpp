#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/memory_reader.h>
#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/chunk_state.h>
#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>
#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkWriterTest
    : public ::testing::Test
{
protected:
    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TTableWriterConfigPtr Config_ = New<TTableWriterConfig>();
    const TTableWriterOptionsPtr Options_ = New<TTableWriterOptions>();

    TTableSchemaPtr Schema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("value", EValueType::Int64),
        TColumnSchema("text", EValueType::String),
    });
    std::vector<TMemoryWriterPtr> Sinks_;

    void SetUp() override
    {
        NameTable_->RegisterName("text");
        NameTable_->RegisterName("value");
        Config_->DesiredChunkWeight = 1;
        Options_->EvaluateComputedColumns = false;
        Options_->ComputeDigest = false;
        Options_->OptimizeFor = EOptimizeFor::Scan;
        Options_->CompressionCodec = NCompression::ECodec::Lz4;
        Options_->TableIndex = 42;
    }

    ISchemalessMultiChunkWriterPtr CreateWriter(TUnderlyingChunkWriterFactory factory = {})
    {
        auto options = Options_;
        options->TableSchema = Schema_;
        if (!factory) {
            factory = BIND([this] () -> IChunkWriterPtr {
                auto sink = New<TMemoryWriter>(
                    MakeRandomId(EObjectType::Chunk, TCellTag(1)));
                Sinks_.push_back(sink);
                return sink;
            });
        }
        return CreateSchemalessMultiChunkWriter(
            Config_,
            options,
            NameTable_,
            Schema_,
            /*lastKey*/ {},
            /*client*/ nullptr,
            /*localHostName*/ {},
            TCellTag(1),
            /*transactionId*/ {},
            /*schemaId*/ {},
            /*dataSink*/ {},
            /*writeBlocksOptions*/ {},
            NullChunkListId,
            TChunkTimestamps(),
            /*trafficMeter*/ nullptr,
            GetUnlimitedThrottler(),
            GetNullBlockCache(),
            /*boundaryKeysProcessor*/ {},
            std::move(factory));
    }

    void WriteRow(const ISchemalessMultiChunkWriterPtr& writer, i64 value)
    {
        auto cell = MakeUnversionedInt64Value(value, NameTable_->GetId("value"));
        TUnversionedOwningRow owningRow(TRange(&cell, 1));
        TUnversionedRow row = owningRow;
        if (!writer->Write(TRange(&row, 1))) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    ISchemalessChunkReaderPtr CreateReader(const TMemoryWriterPtr& sink)
    {
        auto state = New<TChunkState>(TChunkState{
            .BlockCache = GetNullBlockCache(),
            .TableSchema = Schema_,
        });
        return CreateSchemalessRangeChunkReader(
            CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>()),
            state,
            New<TColumnarChunkMeta>(*sink->GetChunkMeta()),
            TChunkReaderConfig::GetDefault(),
            TChunkReaderOptions::GetDefault(),
            CreateMemoryReader(sink->GetChunkMeta(), sink->GetBlocks()),
            TNameTable::FromSchema(*Schema_),
            /*chunkReadOptions*/ {},
            /*sortColumns*/ {},
            /*omittedInaccessibleColumns*/ {},
            TColumnFilter(),
            TReadRange(),
            /*partitionTags*/ {},
            /*memoryManagerHolder*/ nullptr,
            /*virtualRowIndex*/ std::nullopt,
            /*interruptDescriptorKeyLength*/ 0,
            TDispatcher::Get()->GetReaderInvoker());
    }
};

TEST_F(TSchemalessMultiChunkWriterTest, EmptyInput)
{
    auto writer = CreateWriter();
    WaitFor(writer->Close())
        .ThrowOnError();
    EXPECT_EQ(1u, Sinks_.size());
    EXPECT_TRUE(writer->GetWrittenChunkSpecs().empty());
    EXPECT_EQ(0, writer->GetDataStatistics().row_count());
}

TEST_F(TSchemalessMultiChunkWriterTest, RotationAndReadback)
{
    Schema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("value", EValueType::Int64)
            .SetStableName(TColumnStableName("original_value")).SetGroup("numbers"),
        TColumnSchema("text", EValueType::String).SetGroup("strings"),
    });
    auto writer = CreateWriter();
    for (int index = 0; index < 3; ++index) {
        WriteRow(writer, index);
        EXPECT_EQ(index + 1, writer->GetDataStatistics().row_count());
    }
    WaitFor(writer->Close())
        .ThrowOnError();
    ASSERT_EQ(3u, writer->GetWrittenChunkSpecs().size());
    ASSERT_EQ(4u, Sinks_.size());
    ASSERT_EQ(3u, writer->GetWrittenChunkReplicasInfos().size());
    i64 expectedValue = 0;
    i64 diskSpace = 0;
    for (int index = 0; index < 3; ++index) {
        const auto& sink = Sinks_[index];
        const auto& spec = writer->GetWrittenChunkSpecs()[index];
        EXPECT_EQ(sink->GetChunkId(), FromProto<TChunkId>(spec.chunk_id()));
        EXPECT_EQ(42, spec.table_index());
        EXPECT_EQ(0, spec.replicas_size());
        diskSpace += sink->GetChunkInfo().disk_space();
        for (const auto& block : sink->GetBlocks()) {
            block.CheckChecksum()
                .ThrowOnError();
        }
        auto reader = CreateReader(sink);
        while (auto batch = reader->Read()) {
            for (auto row : batch->MaterializeRows()) {
                ASSERT_EQ(2u, row.GetCount());
                EXPECT_EQ(expectedValue++, row[0].Data.Int64);
                EXPECT_EQ(reader->GetNameTable()->GetId("value"), row[0].Id);
                EXPECT_EQ(EValueType::Null, row[1].Type);
            }
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
        }
    }
    EXPECT_EQ(3, expectedValue);
    auto statistics = writer->GetDataStatistics();
    EXPECT_EQ(3, statistics.row_count());
    EXPECT_EQ(3, statistics.chunk_count());
    EXPECT_EQ(diskSpace, statistics.regular_disk_space());
    EXPECT_EQ(diskSpace, statistics.compressed_data_size());
}

TEST_F(TSchemalessMultiChunkWriterTest, RotationFactoryError)
{
    int invocationCount = 0;
    auto writer = CreateWriter(BIND([&] () -> IChunkWriterPtr {
        if (++invocationCount == 2) {
            THROW_ERROR_EXCEPTION("rotation factory failure");
        }
        return New<TMemoryWriter>(MakeRandomId(EObjectType::Chunk, TCellTag(1)));
    }));
    // DesiredChunkWeight is 1, so the first row triggers rotation and the second factory call.
    EXPECT_THROW_WITH_SUBSTRING(WriteRow(writer, 1), "rotation factory failure");
    EXPECT_EQ(1u, writer->GetWrittenChunkSpecs().size());
}

TEST_F(TSchemalessMultiChunkWriterTest, FinalPartialBlock)
{
    Config_->DesiredChunkWeight = 1_GB;
    auto writer = CreateWriter();
    for (int index = 0; index < 10; ++index) {
        WriteRow(writer, index);
    }
    EXPECT_EQ(10, writer->GetDataStatistics().row_count());
    WaitFor(writer->Close())
        .ThrowOnError();
    EXPECT_EQ(1u, Sinks_.size());
    EXPECT_EQ(1u, writer->GetWrittenChunkSpecs().size());
    EXPECT_EQ(10, writer->GetDataStatistics().row_count());
    EXPECT_EQ(10, Sinks_.front()->GetDataStatistics().row_count());
}

TEST_F(TSchemalessMultiChunkWriterTest, ZeroColumnSchema)
{
    Schema_ = New<TTableSchema>();
    Config_->DesiredChunkWeight = 1_GB;
    auto writer = CreateWriter();
    auto owningRow = TUnversionedOwningRow(TRange<TUnversionedValue>());
    TUnversionedRow row = owningRow;
    ASSERT_TRUE(writer->Write(TRange(&row, 1)));
    WaitFor(writer->Close())
        .ThrowOnError();
    ASSERT_EQ(1u, writer->GetWrittenChunkSpecs().size());
    EXPECT_EQ(1, writer->GetDataStatistics().row_count());
}

TEST_PI(
    TSchemalessMultiChunkWriterTest,
    RequiredAndTypeErrors,
    ::testing::Values(EValueType::Null, EValueType::Boolean),
    EValueType)
{
    Schema_ = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("value", EValueType::Int64).SetRequired(true),
    });
    auto columnId = NameTable_->GetId("value");
    auto cell = GetParam() == EValueType::Null
        ? MakeUnversionedNullValue(columnId)
        : MakeUnversionedBooleanValue(true, columnId);
    auto writer = CreateWriter();
    auto owningRow = TUnversionedOwningRow(TRange(&cell, 1));
    TUnversionedRow row = owningRow;
    EXPECT_FALSE(writer->Write(TRange(&row, 1)));
    EXPECT_FALSE(WaitFor(writer->GetReadyEvent()).IsOK());
    EXPECT_TRUE(writer->GetWrittenChunkSpecs().empty());
}

TEST_F(TSchemalessMultiChunkWriterTest, DenseLargeRow)
{
    Config_->DesiredChunkWeight = 1_GB;
    Config_->BlockSize = 1_KB;
    auto writer = CreateWriter();
    std::string text(1_MB, 'x');
    std::vector<TUnversionedValue> values{
        MakeUnversionedStringValue(text, NameTable_->GetId("text")),
        MakeUnversionedInt64Value(17, NameTable_->GetId("value")),
    };
    auto owningRow = TUnversionedOwningRow(values);
    TUnversionedRow row = owningRow;
    if (!writer->Write(TRange(&row, 1))) {
        WaitFor(writer->GetReadyEvent())
            .ThrowOnError();
    }
    WaitFor(writer->Close())
        .ThrowOnError();
    ASSERT_EQ(1u, writer->GetWrittenChunkSpecs().size());
    auto reader = CreateReader(Sinks_.front());
    int rowCount = 0;
    while (auto batch = reader->Read()) {
        for (auto actual : batch->MaterializeRows()) {
            ++rowCount;
            ASSERT_EQ(2u, actual.GetCount());
            EXPECT_EQ(17, actual[0].Data.Int64);
            EXPECT_EQ(text, actual[1].AsStringBuf());
        }
        WaitFor(reader->GetReadyEvent())
            .ThrowOnError();
    }
    EXPECT_EQ(1, rowCount);
    EXPECT_GT(
        writer->GetDataStatistics().uncompressed_data_size(),
        writer->GetDataStatistics().compressed_data_size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
