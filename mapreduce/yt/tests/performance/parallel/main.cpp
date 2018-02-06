#include <mapreduce/yt/interface/client.h>

#include <util/system/env.h>

using namespace NYT;

int main(int argc, const char** argv) {
    if (argc < 3) {
        Cout << "Usage: " << argv[0] << " <path> <num-rows>" << Endl;
        return 1;
    }

    Initialize(argc, argv);
    const TString ytProxy = "freud";
    const TString path = argv[1];
    const ui64 rowCount = FromString<ui64>(argv[2]);

    auto client = CreateClient(ytProxy);
    auto reader = client->CreateTableReader<TNode>(TRichYPath(path)
        .AddRange(TReadRange::FromRowIndexes(0, rowCount)));
    size_t count = 0;
    for ( ; reader->IsValid(); reader->Next()) {
        count += reader->GetRow().Size();
    }
    TOFStream os("/dev/null");
    os << count;

    return 0;
}
