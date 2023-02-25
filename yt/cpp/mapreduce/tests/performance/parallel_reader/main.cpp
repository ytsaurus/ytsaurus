#include <yt/cpp/mapreduce/tests/performance/parallel_reader/jupiter_log.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/library/parallel_io/parallel_reader.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/system/env.h>

using namespace NYT;

template <typename T>
TTableReaderPtr<T> CreateReader(
    const IClientBasePtr& client,
    const TRichYPath& path,
    size_t threadCount,
    bool ordered,
    size_t memoryLimit,
    size_t batchSize)
{
    if (threadCount == 1) {
        return client->CreateTableReader<T>(path);
    } else {
        return CreateParallelTableReader<T>(client, path,
            TParallelTableReaderOptions()
                .Ordered(ordered)
                .MemoryLimit(memoryLimit)
                .ThreadCount(threadCount)
                .BatchSizeBytes(batchSize));
    }
}

template <typename T>
void RunReader(const TTableReaderPtr<T>& reader);

template <>
void RunReader(const TTableReaderPtr<TNode>& reader)
{
    size_t count = 0;
    for (; reader->IsValid(); reader->Next()) {
        count += reader->GetRow().Size();
    }
    TOFStream os("/dev/null");
    os << count << Endl;
}

template <>
void RunReader(const TTableReaderPtr<NTesting::TLogRow>& reader)
{
    size_t sum = 0;
    for (; reader->IsValid(); reader->Next()) {
        sum += reader->GetRow().GetTimestamp();
    }
    TOFStream os("/dev/null");
    os << sum << Endl;
}

int main(int argc, const char** argv) {
    Initialize(argc, argv);

    NLastGetopt::TOpts options;

    TString path;
    options
        .AddLongOption("path")
        .Required()
        .StoreResult(&path);

    i64 rowCount;
    options
        .AddLongOption("row-count")
        .Required()
        .StoreResult(&rowCount);

    int threadCount;
    options
        .AddLongOption("thread-count")
        .Required()
        .StoreResult(&threadCount);

    i64 memoryLimit;
    options
        .AddLongOption("memory-limit")
        .DefaultValue(100'000'000)
        .StoreResult(&memoryLimit);

    i64 batchSize;
    options
        .AddLongOption("batch-size")
        .DefaultValue(2'000'000)
        .StoreResult(&batchSize);

    bool ordered;
    options
        .AddLongOption("ordered")
        .Required()
        .StoreResult(&ordered);

    TString rowType;
    options
        .AddLongOption("row-type")
        .Required()
        .StoreResult(&rowType);
    NLastGetopt::TOptsParseResult parsedOpts(&options, argc, argv);

    const TString ytProxy = "hahn";

    Y_ENSURE(threadCount >= 1);

    auto richPath = TRichYPath(path).AddRange(TReadRange::FromRowIndices(0, rowCount));
    auto client = CreateClient(ytProxy);

    if (rowType == "node") {
        RunReader(CreateReader<TNode>(
            client,
            richPath,
            threadCount,
            ordered,
            memoryLimit,
            batchSize));
    } else if (rowType == "proto") {
        RunReader(CreateReader<NTesting::TLogRow>(
            client,
            richPath,
            threadCount,
            ordered,
            memoryLimit,
            batchSize));
    } else {
        Cerr << "Expected row-type to be one of {\"node\", \"proto\"}" << Endl;
        return 1;
    }

    return 0;
}
