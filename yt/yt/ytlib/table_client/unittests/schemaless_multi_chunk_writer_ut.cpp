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
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <util/generic/cast.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMockColumnarRowBatch)

class TMockColumnarRowBatch
    : public IUnversionedColumnarRowBatch
{
public:
    TMockColumnarRowBatch(TSharedRange<TUnversionedRow> rows, bool failMaterialization)
        : Rows_(std::move(rows))
        , FailMaterialization_(failMaterialization)
    { }

    int GetRowCount() const override
    {
        return Rows_.Size();
    }

    TSharedRange<TUnversionedRow> MaterializeRows() override
    {
        ++MaterializeRowsCallCount_;
        if (FailMaterialization_) {
            THROW_ERROR_EXCEPTION("Batch materialization failure");
        }
        return Rows_;
    }

    TRange<const TColumn*> MaterializeColumns() override
    {
        THROW_ERROR_EXCEPTION("Unexpected column materialization");
    }

    TRange<TDictionaryId> GetRetiringDictionaryIds() const override
    {
        return {};
    }

    int GetMaterializeRowsCallCount() const
    {
        return MaterializeRowsCallCount_;
    }

private:
    const TSharedRange<TUnversionedRow> Rows_;
    const bool FailMaterialization_;
    int MaterializeRowsCallCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TMockColumnarRowBatch)

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

    TMockColumnarRowBatchPtr CreateBatch(
        const std::vector<i64>& values,
        bool failMaterialization = false)
    {
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TUnversionedRow> rows;
        for (auto value : values) {
            auto row = rowBuffer->AllocateUnversioned(1);
            row[0] = MakeUnversionedInt64Value(value, NameTable_->GetId("value"));
            rows.push_back(row);
        }
        return New<TMockColumnarRowBatch>(
            MakeSharedRange(std::move(rows), std::move(rowBuffer)),
            failMaterialization);
    }

    TTypeConversionConfigPtr CreateDisabledTypeConversionConfig()
    {
        auto config = New<TTypeConversionConfig>();
        config->EnableIntegralTypeConversion = false;
        return config;
    }

    std::vector<i64> ReadValues(TRange<TMemoryWriterPtr> sinks)
    {
        std::vector<i64> values;
        for (const auto& sink : sinks) {
            auto reader = CreateReader(sink);
            while (auto batch = reader->Read()) {
                for (auto row : batch->MaterializeRows()) {
                    values.push_back(row[0].Data.Int64);
                }
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            }
        }
        return values;
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

TEST_PI(
    TSchemalessMultiChunkWriterTest,
    MixedScalarAndColumnarReadback,
    ::testing::Bool())
{
    auto queue = New<TActionQueue>("ColumnarReadbackTest");
    WaitFor(BIND([this] {
        auto writer = CreateWriter();
        auto batch = CreateBatch({1, 2});
        if (GetParam()) {
            TWritingValueConsumer consumer(writer, CreateDisabledTypeConversionConfig());
            consumer.OnBeginRow();
            consumer.OnValue(MakeUnversionedInt64Value(0, NameTable_->GetId("value")));
            consumer.OnEndRow();
            consumer.Write(batch);
            consumer.OnBeginRow();
            consumer.OnValue(MakeUnversionedInt64Value(3, NameTable_->GetId("value")));
            consumer.OnEndRow();
            WaitFor(consumer.Flush())
                .ThrowOnError();
        } else {
            WriteRow(writer, 0);
            auto* columnarWriter = VerifyDynamicCast<IColumnarUnversionedWriter*>(writer.Get());
            if (!columnarWriter->Write(batch)) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
            WriteRow(writer, 3);
        }
        EXPECT_EQ(1, batch->GetMaterializeRowsCallCount());
        WaitFor(writer->Close())
            .ThrowOnError();
        EXPECT_GT(writer->GetWrittenChunkSpecs().size(), 1u);
        auto sinks = TRange(Sinks_.data(), writer->GetWrittenChunkSpecs().size());
        EXPECT_EQ((std::vector<i64>{0, 1, 2, 3}), ReadValues(sinks));
        EXPECT_EQ(4, writer->GetDataStatistics().row_count());
    }).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
}

TEST_F(TSchemalessMultiChunkWriterTest, ConsumerFlushWaitsForColumnarRotation)
{
    auto queue = New<TActionQueue>("ColumnarFlushTest");
    WaitFor(BIND([this] {
        auto allowRotation = NewPromise<void>();
        auto createSink = BIND([allowRotation, writerCount = 0] () mutable -> IChunkWriterPtr {
            if (++writerCount > 1) {
                // InitSession holds a spinlock, so WaitFor must not suspend this fiber.
                // The separate test action-queue thread releases the gate after checking Flush.
                auto rotationGate = allowRotation.ToFuture();
                while (!rotationGate.IsSet()) {
                }
            }
            return New<TMemoryWriter>(MakeRandomId(EObjectType::Chunk, TCellTag(1)));
        });
        auto writer = CreateWriter(std::move(createSink));
        TWritingValueConsumer consumer(writer, CreateDisabledTypeConversionConfig());
        consumer.Write(CreateBatch({1, 2}));

        auto flush = consumer.Flush();
        bool flushWasPending = !flush.IsSet();
        allowRotation.Set();
        ASSERT_TRUE(flushWasPending);
        WaitFor(flush)
            .ThrowOnError();
        WaitFor(writer->Close())
            .ThrowOnError();
        EXPECT_EQ(1u, writer->GetWrittenChunkSpecs().size());
        EXPECT_EQ(2, writer->GetDataStatistics().row_count());
    }).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
}

TEST_PI(
    TSchemalessMultiChunkWriterTest,
    ColumnarMaterializationFailure,
    ::testing::Bool())
{
    auto queue = New<TActionQueue>("ColumnarFailureTest");
    WaitFor(BIND([this] {
        auto writer = CreateWriter();
        WriteRow(writer, 0);
        auto batch = CreateBatch({1, 2}, /*failMaterialization*/ true);
        auto* columnarWriter = VerifyDynamicCast<IColumnarUnversionedWriter*>(writer.Get());
        if (GetParam()) {
            TWritingValueConsumer consumer(writer, CreateDisabledTypeConversionConfig());
            consumer.Write(batch);
            EXPECT_THROW_WITH_SUBSTRING(
                WaitFor(consumer.Flush())
                    .ThrowOnError(),
                "Batch materialization failure");
        } else {
            EXPECT_FALSE(columnarWriter->Write(batch));
        }
        EXPECT_EQ(1, batch->GetMaterializeRowsCallCount());
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError(),
            "Batch materialization failure");
        auto nextBatch = CreateBatch({3});
        EXPECT_FALSE(columnarWriter->Write(nextBatch));
        EXPECT_EQ(0, nextBatch->GetMaterializeRowsCallCount());
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(writer->Close())
                .ThrowOnError(),
            "Batch materialization failure");
        ASSERT_EQ(1u, writer->GetWrittenChunkSpecs().size());
        EXPECT_EQ((std::vector<i64>{0}), ReadValues(TRange(Sinks_.data(), 1)));
    }).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
}

TEST_F(TSchemalessMultiChunkWriterTest, ConsumerRejectsUnsupportedWriter)
{
    auto queue = New<TActionQueue>("UnsupportedWriterTest");
    WaitFor(BIND([this] {
        Schema_ = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("value", EValueType::Int64),
        });
        auto sink = New<TMemoryWriter>();
        auto writer = CreateSchemalessChunkWriter(
            Config_,
            Options_,
            Schema_,
            TNameTable::FromSchema(*Schema_),
            sink,
            /*writeBlocksOptions*/ {});
        TWritingValueConsumer consumer(writer, CreateDisabledTypeConversionConfig());
        consumer.OnBeginRow();
        consumer.OnValue(MakeUnversionedInt64Value(7, consumer.GetNameTable()->GetId("value")));
        consumer.OnEndRow();
        auto batch = CreateBatch({1, 2});
        EXPECT_THROW_WITH_SUBSTRING(
            consumer.Write(batch),
            "Underlying writer does not support columnar writes");
        EXPECT_EQ(0, batch->GetMaterializeRowsCallCount());
        EXPECT_EQ(0, writer->GetDataStatistics().row_count());
        WaitFor(consumer.Flush())
            .ThrowOnError();
        WaitFor(writer->Close())
            .ThrowOnError();
        EXPECT_EQ((std::vector<i64>{7}), ReadValues(TRange(&sink, 1)));
    }).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
}

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
