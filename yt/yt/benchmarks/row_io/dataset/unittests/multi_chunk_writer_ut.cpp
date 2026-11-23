#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/benchmarks/row_io/dataset/dataset.h>
#include <yt/yt/benchmarks/row_io/dataset/multi_chunk_writer.h>
#include <yt/yt/benchmarks/row_io/dataset/writer.h>

#include <yt/yt/ytlib/chunk_client/memory_writer.h>

#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NRowIOBenchmark {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TMemoryChunkCollectorTest, OrderIsPreserved)
{
    auto queue = New<TActionQueue>("ReadbackTest");
    WaitFor(BIND([] {
        auto schema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("value", EValueType::Int64),
        });
        auto source = New<TMemoryWriter>();
        auto sourceWriter = CreateWriter(schema, source, EOptimizeFor::Scan);
        auto collector = New<TMemoryChunkCollector>();
        auto writer = CreateMemoryMultiChunkWriter(
            schema,
            TNameTable::FromSchema(*schema),
            collector,
            /*desiredChunkSize*/ 1);
        for (int index = 0; index < 5; ++index) {
            auto cell = MakeUnversionedInt64Value(index);
            auto owningRow = TUnversionedOwningRow(TRange(&cell, 1));
            TUnversionedRow row = owningRow;
            if (!sourceWriter->Write(TRange(&row, 1))) {
                WaitFor(sourceWriter->GetReadyEvent())
                    .ThrowOnError();
            }
            if (!writer->Write(TRange(&row, 1))) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
        }
        WaitFor(sourceWriter->Close())
            .ThrowOnError();
        auto chunks = collector->Finish(writer);
        EXPECT_GT(chunks.size(), 1u);
        ValidateOutput(schema, source, chunks, /*expectedRowCount*/ 5);
    }).AsyncVia(queue->GetInvoker()).Run())
        .ThrowOnError();
}

TEST(TMemoryChunkCollectorTest, EmptyInput)
{
    auto schema = New<TTableSchema>();
    auto collector = New<TMemoryChunkCollector>();
    auto writer = CreateMemoryMultiChunkWriter(schema, New<TNameTable>(), collector);
    EXPECT_TRUE(collector->Finish(writer).empty());
}

////////////////////////////////////////////////////////////////////////////////

class TColumnarValueConsumerTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_PI(
    TColumnarValueConsumerTest,
    RejectsTypeConversions,
    ::testing::Values(
        &TTypeConversionConfig::EnableTypeConversion,
        &TTypeConversionConfig::EnableStringToAllConversion,
        &TTypeConversionConfig::EnableAllToStringConversion,
        &TTypeConversionConfig::EnableIntegralTypeConversion,
        &TTypeConversionConfig::EnableIntegralToDoubleConversion),
    bool TTypeConversionConfig::*)
{
    auto config = New<TTypeConversionConfig>();
    config->EnableIntegralTypeConversion = false;
    config.Get()->*GetParam() = true;

    auto schema = New<TTableSchema>();
    auto collector = New<TMemoryChunkCollector>();
    auto writer = CreateMemoryMultiChunkWriter(schema, New<TNameTable>(), collector);
    TWritingValueConsumer consumer(writer, config);
    EXPECT_THROW_WITH_SUBSTRING(
        consumer.Write(/*batch*/ nullptr),
        "Type conversion is not supported for columnar writes");
    EXPECT_TRUE(collector->Finish(writer).empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRowIOBenchmark
