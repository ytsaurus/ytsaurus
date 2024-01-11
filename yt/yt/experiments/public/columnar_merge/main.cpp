#include "routines.h"

#include <yt/yt/core/misc/signal_registry.h>
#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/chunk_client/memory_writer.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_writer.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/range_formatters.h>

#include <util/string/cast.h>

#include <library/cpp/getopt/last_getopt.h>


#include <yt/yt/ytlib/table_client/chunk_state.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/query/engine_api/config.h>

namespace NYT {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

namespace NNewTableClient {

TString ToString(const TReaderStatistics& statistics);

} // namespace TReaderStatistics

////////////////////////////////////////////////////////////////////////////////

struct TOptions
    : public TReaderOptions
{
    int BatchSize = 128;
    bool Dump = false;
    int Repeat = 1;
    bool SetOneGroup = false;
    bool CalculateChecksum = false;
    bool LookupHashTable = false;
    i64 BlockSize = 256_KB;
};

TChecksum GetChecksum(const TUnversionedValue& value, TChecksum checksum)
{
    if (IsStringLikeType(value.Type)) {
        checksum = GetChecksum(TRef(&value, sizeof(TUnversionedValue) - sizeof(TUnversionedValueData)), checksum);
        checksum = GetChecksum(TRef(value.Data.String, value.Length), checksum);
    } else {
        checksum = GetChecksum(TRef(&value, sizeof(TUnversionedValue)), checksum);
    }
    return checksum;
}

TChecksum GetChecksum(const TVersionedValue& value, TChecksum checksum)
{
    checksum = GetChecksum(static_cast<const TUnversionedValue&>(value), checksum);
    checksum = GetChecksum(TRef(&value.Timestamp, sizeof(TTimestamp)), checksum);
    return checksum;
}

struct TStat
{
    size_t RowCount = 0;
    size_t KeysWeight = 0;
    size_t ValueWeight = 0;
    size_t TsWeight = 0;
    size_t MaxTimestamps = 0;
    size_t Checksum = 0;
    bool CalculateChecksum = false;

    void OnRow(TVersionedRow row)
    {
        ++RowCount;

        if (!row) {
            return;
        }

        KeysWeight += row.GetKeyCount() * sizeof(TUnversionedValue);
        ValueWeight += row.GetValueCount() * sizeof(TVersionedValue);

        TsWeight += row.GetWriteTimestampCount() * sizeof(TTimestamp);
        TsWeight += row.GetDeleteTimestampCount() * sizeof(TTimestamp);

        MaxTimestamps = std::max(MaxTimestamps, size_t(row.GetWriteTimestampCount()));

        if (!CalculateChecksum) {
            return;
        }

        auto checksum = Checksum;

        checksum = GetChecksum(TRef(row.BeginWriteTimestamps(), row.EndWriteTimestamps()), checksum);
        checksum = GetChecksum(TRef(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps()), checksum);

        for (const auto& value : row.Keys()) {
            checksum += GetChecksum(value, checksum);
        }

        for (const auto& value : row.Values()) {
            checksum += GetChecksum(value, checksum);
        }

        Checksum = checksum;
    }
};

TString ToString(const TStat& stat)
{
    return Format("RowCount: %v, MaxTimestamps: %v, Key/Value/Timestamp Weight: %v / %v / %v, Checksum: %x",
        stat.RowCount,
        stat.MaxTimestamps,
        stat.KeysWeight,
        stat.ValueWeight,
        stat.TsWeight,
        stat.Checksum);
}

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TRow>
bool ReadRows(const TReader& reader, std::vector<TRow>* rows)
{
    TRowBatchReadOptions readOptions{
        .MaxRowsPerRead = i64(rows->capacity())
    };

    auto batch = reader->Read(readOptions);

    if (!batch) {
        return false;
    }

    // Materialize rows here.
    // Drop null rows.
    auto batchRows = batch->MaterializeRows();
    rows->reserve(batchRows.size());
    rows->clear();
    for (auto row : batchRows) {
        rows->push_back(row);
    }

    return true;
}

void TestReadWithMerge(const std::vector<TString>& chunkNames, TOptions options)
{
    auto ioEngine = CreateIOEngine();
    auto tableSchema = GetTableSchema();

    auto readItems = MakeSingletonRowRange(MinKey(), MaxKey());

    auto reader = CreateMergingReader(
        ioEngine,
        tableSchema,
        readItems,
        chunkNames,
        options);

    Cout << "Started..." << Endl;

    TDuration readTime;

    {
        std::vector<TUnversionedRow> rows;
        rows.reserve(options.BatchSize);

        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&readTime);

        size_t rowCount = 0;

        while (ReadRows(reader, &rows)) {
            rowCount += rows.size();

            if (rows.empty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            } else if (options.Dump) {
                for (const auto& row : rows) {
                    Cout << ToString(row) << Endl;
                }
            }
        }
        Cout << "Row count: " << rowCount << Endl;
    }
    Cout << Format("Times (Read: %v)", readTime) << Endl;
}

// TODO: Compaction is Merge with --all-committed
void TestCompaction(const std::vector<TString>& chunkNames, bool dump, int batchSize, bool write)
{
    auto ioEngine = CreateIOEngine();
    auto tableSchema = GetTableSchema();
    auto reader = CreateCompactionReader(ioEngine, tableSchema, chunkNames, {.NewReader = false});

    auto memoryWriter = New<NChunkClient::TMemoryWriter>();

    IVersionedChunkWriterPtr writer;

    {
        auto config = New<TChunkWriterConfig>();
        config->BlockSize = 256_KB;
        config->Postprocess();

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = EOptimizeFor::Scan;
        options->CompressionCodec = NCompression::ECodec::None;
        options->Postprocess();

        writer = NTableClient::CreateVersionedChunkWriter(
            config,
            options,
            tableSchema, // TTableSchema(cols),
            memoryWriter,
            /*dataSink*/ std::nullopt);
    }

    TDuration writeTime;
    TDuration writeAsyncTime;

    Cout << "Started..." << Endl;

    TDuration readTime;
    {
        std::vector<TVersionedRow> rows;
        rows.reserve(batchSize);

        size_t rowCount = 0;

        bool hasMoreData;

        do {
            {
                TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readTime);
                hasMoreData = ReadRows(reader, &rows);
            }
            rowCount += rows.size();

            if (rows.empty()) {
                if (!hasMoreData) {
                    break;
                }

                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            if (write) {
                bool writerShouldWait = false;
                {
                    TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&writeTime);
                    writerShouldWait = !writer->Write(rows);
                }

                if (writerShouldWait) {
                    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&writeAsyncTime);
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            }

            if (dump) {
                for (const auto& row : rows) {
                    Cout << ToString(row) << Endl;
                }
            }
        } while (hasMoreData);
        Cout << "Row count: " << rowCount << Endl;
    }

    size_t writtenSize = 0;
    if (write) {
        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&writeAsyncTime);
            WaitFor(writer->Close())
                .ThrowOnError();
        }

        for (auto& block : memoryWriter->GetBlocks()) {
            writtenSize += block.Size();
        }
    }

    Cout << Format("Times (Read: %v, Write: %v, WriteAsync: %v, Size: %v)",
        readTime,
        writeTime,
        writeAsyncTime,
        writtenSize) << Endl;
}

template <class TOnRows>
size_t DoReadImpl(IVersionedReaderPtr versionedReader, int batchSize, TOnRows OnRows)
{
    std::vector<TVersionedRow> rows;
    rows.reserve(batchSize);

    size_t getReadyEventCount = 0;

    while (ReadRows(versionedReader, &rows)) {
        if (rows.empty()) {
            ++getReadyEventCount;
            auto future = versionedReader->GetReadyEvent();

            if (future.IsSet()) {
                continue;
            }

            WaitFor(std::move(future))
                .ThrowOnError();
        } else {
            OnRows(rows);
            rows.clear();
        }
    }
    YT_VERIFY(rows.empty());

    return getReadyEventCount;
}

void DoRead(
    IVersionedReaderPtr versionedReader,
    TString outputName,
    int batchSize,
    bool calculateChecksum)
{
    TDuration readTime;
    TStat stat;
    stat.CalculateChecksum = calculateChecksum;

    std::unique_ptr<TFileOutput> textFile;
    if (outputName) {
        textFile = std::make_unique<TFileOutput>(outputName);
    }

    size_t getReadyEventCount;
    {
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&readTime);
        getReadyEventCount = DoReadImpl(versionedReader, batchSize, [&] (TRange<TVersionedRow> rows) {
            for (auto row : rows) {
                stat.OnRow(row);
            }

            if (textFile) {
                for (auto row : rows) {
                    textFile->Write(Format("%v\n", row));
                }
            }
        });
    }

    if (textFile) {
        textFile->Finish();
    }

    Cout << Format("ReadTime: %v, Stat: (%v), GetReadyEvent: %v", readTime, stat, getReadyEventCount) << Endl;
}

void DoRead(IVersionedReaderPtr versionedReader, int batchSize)
{
    TDuration readTime;
    TStat stat;

    size_t getReadyEventCount;
    {
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&readTime);
        getReadyEventCount = DoReadImpl(versionedReader, batchSize, [&] (TRange<TVersionedRow> rows) {
            for (auto row : rows) {
                stat.OnRow(row);
            }
        });
    }

    Cout << Format("ReadTime: %v, Stat: (%v), GetReadyEvent: %v", readTime, stat, getReadyEventCount) << Endl;
}

void ConvertChunkFormat(
    const TReaderData& readerData,
    EOptimizeFor optimizeFor,
    i64 blockSize,
    const TTableSchemaPtr tableSchema,
    NChunkClient::TMemoryWriterPtr memoryWriter)
{
    IVersionedChunkWriterPtr writer;

    {
        auto config = New<TChunkWriterConfig>();
        config->BlockSize = blockSize;
        config->Postprocess();

        auto options = New<TChunkWriterOptions>();
        options->OptimizeFor = optimizeFor;
        options->CompressionCodec = NCompression::ECodec::None;
        options->Postprocess();

        writer = NTableClient::CreateVersionedChunkWriter(
            config,
            options,
            tableSchema,
            memoryWriter,
            /*dataSink*/ std::nullopt);
    }

    auto rowBuffer = New<TRowBuffer>();

    TDuration convertTime;
    {
        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&convertTime);
        auto versionedReader = CreateChunkReader(
            readerData,
            tableSchema,
            MakeSingletonRowRange(MinKey(), MaxKey()),
            TReaderOptions{.AllCommitted = true, .ProduceAllVersions = true, .NewReader = false});

        DoReadImpl(versionedReader, 128, [&] (TRange<TVersionedRow> rows) {
            bool writerShouldWait = false;
            {
                writerShouldWait = !writer->Write(rows);
            }

            if (writerShouldWait) {
                WaitFor(writer->GetReadyEvent())
                    .ThrowOnError();
            }
        });

        WaitFor(writer->Close())
            .ThrowOnError();
    }

    Cout << Format("Format: %v, ConvertTime: %v", optimizeFor, convertTime) << Endl;
}

void TestVersionedScanRead(TString chunkName, TOptions options)
{
    auto ioEngine = CreateIOEngine();
    auto tableSchema = GetTableSchema();


#if 0
    // From file.
    TReaderData readerData(ioEngine, tableSchema, chunkName);

#else
    TReaderData readerDataInitial(ioEngine, tableSchema, chunkName);

    auto columns = tableSchema->Columns();
    for (auto& column : columns) {
        if (options.SetOneGroup) {
            column.SetGroup("default");
        } else {
            column.SetGroup(std::nullopt);
        }
    }
    tableSchema = New<TTableSchema>(columns);

    auto scanMemoryWriter = New<NChunkClient::TMemoryWriter>();
    ConvertChunkFormat(readerDataInitial, EOptimizeFor::Scan, options.BlockSize, tableSchema, scanMemoryWriter);
    TReaderData readerData(tableSchema, scanMemoryWriter->GetChunkMeta(), scanMemoryWriter->GetBlocks());
#endif

    auto readItems = MakeSingletonRowRange(MinKey(), MaxKey());

    for (int index = 0; index < options.Repeat; ++index) {
        auto timeStatistics = New<NNewTableClient::TReaderStatistics>();
        auto versionedReader = CreateChunkReader(
            readerData,
            tableSchema,
            readItems,
            options,
            timeStatistics.Get());

        WaitFor(versionedReader->Open())
            .ThrowOnError();

        if (index == 0) {
            auto fileNamePattern =  options.NewReader ? "%v.new.txt" : "%v.txt";

            DoRead(
                versionedReader,
                options.Dump ? Format(fileNamePattern, chunkName) : TString{},
                options.BatchSize,
                options.CalculateChecksum);

            Cout << Format("Statistics (%v)", *timeStatistics) << Endl;
        } else {
            TStat stat;
            DoReadImpl(versionedReader, options.BatchSize, [&] (TRange<TVersionedRow> rows) {
                for (auto row : rows) {
                    stat.OnRow(row);
                }
            });
        }
    }

#if 1
    if (options.Repeat == 1) {
        auto lookupMemoryWriter = New<NChunkClient::TMemoryWriter>();
        ConvertChunkFormat(readerDataInitial, EOptimizeFor::Lookup, options.BlockSize, tableSchema, lookupMemoryWriter);
        TReaderData readerDataLookup(tableSchema, lookupMemoryWriter->GetChunkMeta(), lookupMemoryWriter->GetBlocks());

        TReaderOptions modifiedOptions = options;
        modifiedOptions.NewReader = false;

        // Benchmark lookup.
        auto versionedReader = CreateChunkReader(
            readerDataLookup,
            tableSchema,
            readItems,
            modifiedOptions,
            nullptr);

        DoRead(
            versionedReader,
            options.Dump ? Format("%v.lookup.txt", chunkName) : TString{},
            options.BatchSize,
            options.CalculateChecksum);
    }
#endif
}

TSharedRange<TUnversionedRow> CollectKeys(const TReaderData& readerData, const TTableSchemaPtr schema, int nth)
{
    auto versionedReader = CreateChunkReader(
        readerData,
        schema,
        MakeSingletonRowRange(MinKey(), MaxKey()),
        TReaderOptions{.AllCommitted = true});

    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> keys;

    size_t keyDataWeight = 0;

    size_t index = 0;
    DoReadImpl(versionedReader, 128, [&] (TRange<TVersionedRow> rows) {
        for (auto row : rows) {
            if (index++ % nth == 0) {
                keys.push_back(rowBuffer->CaptureRow(row.Keys(), true));

                keyDataWeight += GetDataWeight(row);
            }
        }
    });

    Cout << Format("KeyCount: %v, KeyDataWeight: %v",
        keys.size(),
        keyDataWeight) << Endl;

    return MakeSharedRange(std::move(keys), std::move(rowBuffer));
}

void TestVersionedLookupRead(TString chunkName, TOptions options, int nth)
{
    auto ioEngine = CreateIOEngine();
    auto tableSchema = GetTableSchema();

#if 0
    // From file.
    TReaderData readerData(ioEngine, tableSchema, chunkName);

#else
    TReaderData readerDataInitial(ioEngine, tableSchema, chunkName);

    auto columns = tableSchema->Columns();
    for (auto& column : columns) {
        if (options.SetOneGroup) {
            column.SetGroup("default");
        } else {
            column.SetGroup(std::nullopt);
        }
    }
    tableSchema = New<TTableSchema>(columns);

    auto scanMemoryWriter = New<NChunkClient::TMemoryWriter>();
    ConvertChunkFormat(readerDataInitial, EOptimizeFor::Scan, options.BlockSize, tableSchema, scanMemoryWriter);
    TReaderData readerData(tableSchema, scanMemoryWriter->GetChunkMeta(), scanMemoryWriter->GetBlocks());
    readerData.PrepareMeta();
    readerData.PrepareColumnMapping(tableSchema);

    if (options.LookupHashTable) {
        readerData.PrepareLookupHashTable();
    }
#endif

    auto sharedKeys = CollectKeys(readerData, tableSchema, nth);

    // Prefetch code.
    {
        auto timeStatistics = New<NNewTableClient::TReaderStatistics>();
        auto versionedReader = CreateChunkReader(
            readerData,
            tableSchema,
            sharedKeys.Slice(0, 1),
            options,
            timeStatistics);

        TStat stat;
        DoReadImpl(versionedReader, options.BatchSize, [&] (TRange<TVersionedRow> rows) {
            for (auto row : rows) {
                stat.OnRow(row);
            }
        });
    }

    for (int index = 0; index < options.Repeat; ++index) {
        TFiberWallTimer createReaderTimer;

        auto timeStatistics = New<NNewTableClient::TReaderStatistics>();
        auto versionedReader = CreateChunkReader(
            readerData,
            tableSchema,
            sharedKeys,
            options,
            timeStatistics);

        // auto openFuture = versionedReader->Open();
        // if (openFuture.IsSet()) {
        //     openFuture.Get().ThrowOnError();
        // } else {
        //     WaitFor(openFuture)
        //     .ThrowOnError();
        // }

        if (index < 1) {
            Cout << Format("ReaderCreationTime: %v", createReaderTimer.GetElapsedTime()) << Endl;

            auto fileNamePattern = options.NewReader ? "%v.lr.lookup.txt" : "%v.lookup.txt";

            DoRead(
                versionedReader,
                options.Dump ? Format(fileNamePattern, chunkName) : TString{},
                options.BatchSize,
                options.CalculateChecksum);

            Cout << Format("Statistics (%v)", *timeStatistics) << Endl;
        } else {
            TStat stat;
            DoReadImpl(versionedReader, options.BatchSize, [&] (TRange<TVersionedRow> rows) {
                for (auto row : rows) {
                    stat.OnRow(row);
                }
            });
        }
    }

    if (options.Repeat == 1) {
        auto lookupMemoryWriter = New<NChunkClient::TMemoryWriter>();
        ConvertChunkFormat(readerDataInitial, EOptimizeFor::Lookup, options.BlockSize, tableSchema, lookupMemoryWriter);
        TReaderData readerDataLookup(tableSchema, lookupMemoryWriter->GetChunkMeta(), lookupMemoryWriter->GetBlocks());
        readerDataLookup.PrepareColumnMapping(tableSchema);

        if (options.LookupHashTable) {
            readerDataLookup.PrepareLookupHashTable();
        }

        // Prefetch code.
        {
            TReaderOptions modifiedOptions = options;
            modifiedOptions.NewReader = false;

            // Benchmark lookup.
            auto versionedReader = CreateChunkReader(
                readerDataLookup,
                tableSchema,
                sharedKeys.Slice(0, 1),
                modifiedOptions);

            TStat stat;
            DoReadImpl(versionedReader, options.BatchSize, [&] (TRange<TVersionedRow> rows) {
                for (auto row : rows) {
                    stat.OnRow(row);
                }
            });
        }

        for (int index = 0; index < options.Repeat; ++index) {
            TFiberWallTimer createReaderTimer;

            TReaderOptions modifiedOptions = options;
            modifiedOptions.NewReader = false;

            // Benchmark lookup.
            auto versionedReader = CreateChunkReader(
                readerDataLookup,
                tableSchema,
                sharedKeys,
                modifiedOptions);

            Cout << Format("ReaderCreationTime: %v", createReaderTimer.GetElapsedTime()) << Endl;

            DoRead(
                versionedReader,
                options.Dump ? Format("%v.lookup.txt", chunkName) : TString{},
                options.BatchSize,
                options.CalculateChecksum);
        }
    }
}


template <class TReadItems>
void DoBenchmark(
    TReaderData& readerData,
    TTableSchemaPtr schema,
    TReadItems readItems,
    TReaderOptions options)
{
    auto timeStatistics = New<NNewTableClient::TReaderStatistics>();

    auto versionedReader = CreateChunkReader(
        readerData,
        schema,
        readItems,
        options,
        timeStatistics);

    WaitFor(versionedReader->Open())
        .ThrowOnError();

    DoRead(versionedReader, 128);
}

void Benchmark(TString chunkName, TOptions options)
{
    auto ioEngine = CreateIOEngine();
    auto tableSchema = GetTableSchema();

    TReaderData initialReaderData(ioEngine, tableSchema, chunkName);

    auto columns = tableSchema->Columns();
    for (auto& column : columns) {
        if (options.SetOneGroup) {
            column.SetGroup("default");
        } else {
            column.SetGroup(std::nullopt);
        }
    }
    tableSchema = New<TTableSchema>(columns);

    auto scanMemoryWriter = New<NChunkClient::TMemoryWriter>();
    ConvertChunkFormat(initialReaderData, EOptimizeFor::Scan, options.BlockSize, tableSchema, scanMemoryWriter);
    TReaderData scanReaderData(tableSchema, scanMemoryWriter->GetChunkMeta(), scanMemoryWriter->GetBlocks());

    auto lookupMemoryWriter = New<NChunkClient::TMemoryWriter>();
    ConvertChunkFormat(initialReaderData, EOptimizeFor::Lookup, options.BlockSize, tableSchema, lookupMemoryWriter);
    TReaderData lookupReaderData(tableSchema, lookupMemoryWriter->GetChunkMeta(), lookupMemoryWriter->GetBlocks());

    /////////////////////////////

    // 1. Read
    {
        Cout << Format("Read all") << Endl;

        auto readItems = MakeSingletonRowRange(MinKey(), MaxKey());

        TReaderOptions modifiedOptions = options;
        modifiedOptions.NewReader = false;
        DoBenchmark(scanReaderData, tableSchema, readItems, modifiedOptions);
        modifiedOptions.NewReader = true;
        DoBenchmark(scanReaderData, tableSchema, readItems, modifiedOptions);
        modifiedOptions.NewReader = false;
        DoBenchmark(lookupReaderData, tableSchema, readItems, modifiedOptions);
    }

    auto allKeys = CollectKeys(lookupReaderData, tableSchema, 1);

    // 2. Lookup
    for (auto nth : {1, 5, 10, 20, 50, 100})
    {
        Cout << Format("Lookup %vth", nth) << Endl;

        std::vector<TUnversionedRow> keys;
        for (size_t index = 0; index < allKeys.size(); index += nth) {
            keys.push_back(allKeys[index]);
        }

        auto sharedKeys = MakeSharedRange(keys, allKeys.GetHolder());

        TReaderOptions modifiedOptions = options;
        modifiedOptions.NewReader = false;
        DoBenchmark(scanReaderData, tableSchema, sharedKeys, options);
        modifiedOptions.NewReader = true;
        DoBenchmark(scanReaderData, tableSchema, sharedKeys, options);
        modifiedOptions.NewReader = false;
        DoBenchmark(lookupReaderData, tableSchema, sharedKeys, options);
    }
}

#if 0

std::vector<TString> DirectPhraseStatV2 = {

#if 0
    //Partition 1
    "a836-63cdb-4090064-90d7cf03",
    "a857-b347b-4090064-1bb18454",
    "a86c-cab0-4090064-74f177c6",
    "a878-9b6cc-4090064-a2909529",
    "a88d-1ba69-4090064-5dc08cdd",
    "a890-e6578-4090064-91b0405a",
    "a891-34967-4090064-f0527c50"
#endif

    // Largest chunk a7df-b7ed6-4090064-f1138bd4

    //Partition 2
    "a7c3-c09ea-4090064-40ff95e6",
    "a7df-b7ed6-4090064-f1138bd4",
    "a843-5bd6d-4090064-b9df0598",
    "a860-3e09a-4090064-277942b2",
    "a883-25ebd-4090064-99aaa3bb",
    "a891-7998d-4090064-a512e374",
    "a88f-bbec1-4090064-a95d1ddd",
    "a891-8a5a0-4090064-9a79833c",
    "a891-c1124-4090064-55d4bed9"
};
#endif

void GuardedMain(int argc, char** argv)
{
    TOptions options;
    int nthKey = 10;
    bool allChunks = false;
    bool columnar = false;
    TString command;

    std::vector<TString> chunkIds;
    {
        auto opts = NLastGetopt::TOpts::Default();

        opts.SetTitle("Columnar merge experiment");
        opts.AddLongOption("new", "Use new reader")
            .StoreTrue(&options.NewReader);
        opts.AddLongOption("dump", "Dump rows into text file")
            .StoreTrue(&options.Dump);
        opts.AddLongOption("all-chunks", "All chunks")
            .StoreTrue(&allChunks);
        opts.AddLongOption("columnar", "Columnar merge")
            .StoreTrue(&columnar);
        opts.AddLongOption("value-column-count", "Value columns to read")
            .StoreResult(&options.ValueColumnCount);
        opts.AddLongOption("batch-size", "Batch size")
            .StoreResult(&options.BatchSize);
        opts.AddLongOption("nth-key", "Nth key for lookup")
            .StoreResult(&nthKey);
        opts.AddLongOption("all-committed", "All committed timestamp")
            .StoreTrue(&options.AllCommitted);
        opts.AddLongOption("repeat", "Repeat")
            .StoreResult(&options.Repeat);
        opts.AddLongOption("set-one-group", "Set one column group")
            .StoreTrue(&options.SetOneGroup);
        opts.AddLongOption("checksum", "Calculate checksum")
            .StoreTrue(&options.CalculateChecksum);
        opts.AddLongOption("no-block-fetcher", "No block fetcher")
            .StoreTrue(&options.NoBlockFetcher);
        opts.AddLongOption("lookup-hash-table", "Lookup hash table")
            .StoreTrue(&options.LookupHashTable);
        opts.AddLongOption("block-size", "Block size")
            .StoreResult(&options.BlockSize);

        //opts.SetFreeArgsNum(1, 1);

        opts.SetFreeArgsMin(1);
        opts.SetFreeArgsMax(NLastGetopt::TOpts::UNLIMITED_ARGS);

        opts.SetFreeArgTitle(0, "<action>",
            "read, lookup, merge");

        NLastGetopt::TOptsParseResult args(&opts, argc, argv);

        auto freeArgs = args.GetFreeArgs();

        command = freeArgs[0];

        for (size_t index = 1; index < freeArgs.size(); ++index) {
            chunkIds.push_back(freeArgs[index]);
        }

        if (allChunks) {

            TFileInput file("index.txt");
            TString line;
            while (file.ReadLine(line)) {
                chunkIds.push_back(line);
            }
        }

        // chunk ids
    }

    if (command == "read") {
        for (auto& chunkId : chunkIds) {
            TestVersionedScanRead(chunkId, options);
        }
    } else if (command == "lookup") {
        for (auto& chunkId : chunkIds) {
            TestVersionedLookupRead(chunkId, options, nthKey);
        }
    } else if (command == "compaction") {
        TestCompaction(chunkIds, options.Dump, options.BatchSize, false);
    } else if (command == "benchmark") {
        for (auto& chunkId : chunkIds) {
            Benchmark(chunkId, options);
        }
    }
}

} // namespace NYT


int main(int argc, char** argv)
{
    using namespace NYT;

    auto shutdown = Finally(Shutdown);

    try {
        TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
        TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }

    return 0;
}
