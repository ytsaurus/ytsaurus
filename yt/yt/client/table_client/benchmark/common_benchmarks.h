#pragma once

#include <benchmark/benchmark.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTableClientBenchmark {

////////////////////////////////////////////////////////////////////////////////

#define YT_BENCHMARK_FORMAT(func, benchmarkFormat, messageName) \
    BENCHMARK_CAPTURE(func, benchmarkFormat/messageName, benchmarkFormat, Get##messageName##Schema())

#define YT_BENCHMARK_FORMAT_ALL_MESSAGES(func, benchmarkFormat) \
    YT_BENCHMARK_FORMAT(func, benchmarkFormat, TIntermediateSemidupsData); \
    YT_BENCHMARK_FORMAT(func, benchmarkFormat, TCanonizedUrl); \
    YT_BENCHMARK_FORMAT(func, benchmarkFormat, TUrlData)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBenchmarkedFormat,
    (Skiff)
    (Protobuf)
    (Yson)
);

////////////////////////////////////////////////////////////////////////////////

class IFormatIOFactory
{
public:
    virtual ~IFormatIOFactory() = default;

    virtual NFormats::ISchemalessFormatWriterPtr CreateWriter(
        const NConcurrency::IAsyncOutputStreamPtr& writerStream) = 0;
    virtual std::unique_ptr<NFormats::IParser> CreateParser(
        NTableClient::IValueConsumer* valueConsumer) = 0;
};

std::unique_ptr<IFormatIOFactory> CreateFormatIOFactory(
    EBenchmarkedFormat format,
    const NTableClient::TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

void FormatWriterBenchmark(
    benchmark::State& state,
    EBenchmarkedFormat format,
    const NYT::NTableClient::TTableSchemaPtr& tableSchema);

void FormatReaderBenchmark(
    benchmark::State& state,
    EBenchmarkedFormat format,
    const NYT::NTableClient::TTableSchemaPtr& tableSchema);

////////////////////////////////////////////////////////////////////////////////

static constexpr int DatasetRowCount = 1000;
static constexpr int WriterRowRangeSize = 1000;
static constexpr int ParserReadBufferSize = 4 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // NYT::NTableClientBenchmark
