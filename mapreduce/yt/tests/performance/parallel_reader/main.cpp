#include <mapreduce/yt/tests/performance/parallel_reader/jupiter_log.pb.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/library/parallel_io/parallel_reader.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/system/env.h>

using namespace NYT;

template <typename T>
TTableReaderPtr<T> CreateReader(const IClientBasePtr& client, const TRichYPath& path, size_t threadCount, bool ordered, size_t bufferedRowCount)
{
    if (threadCount == 1) {
        return client->CreateTableReader<T>(path);
    } else {
        return CreateParallelTableReader<T>(client, path,
            TParallelTableReaderOptions()
                .Ordered(ordered)
                .BufferedRowCountLimit(bufferedRowCount)
                .ThreadCount(threadCount));
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
    if (argc < 6) {
        Cerr << "Usage: " << argv[0] << " <path> <num-rows> <num-threads> <ordered> <format>" << Endl;
        return 1;
    }

    Initialize(argc, argv);
    const TString ytProxy = "freud";
    const TString path = argv[1];
    const ui64 rowCount = FromString<ui64>(argv[2]);
    const size_t threadCount = FromString<size_t>(argv[3]);
    const bool ordered = FromString<bool>(argv[4]);
    const TString format = argv[5];

    Y_ENSURE(threadCount >= 1);

    auto richPath = TRichYPath(path).AddRange(TReadRange::FromRowIndices(0, rowCount));
    auto client = CreateClient(ytProxy);

    if (format == "node") {
        RunReader(CreateReader<TNode>(client, richPath, threadCount, ordered, 10000000));
    } else {
        Y_ENSURE(format == "proto");
        RunReader(CreateReader<NTesting::TLogRow>(client, richPath, threadCount, ordered, 20000000));
    }

    return 0;
}
