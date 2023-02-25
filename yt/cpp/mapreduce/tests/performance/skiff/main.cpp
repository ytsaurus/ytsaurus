#include <yt/cpp/mapreduce/client/skiff.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/io/skiff_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/skiff/skiff_schema.h>

#include <util/system/env.h>
#include <util/string/type.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NSkiff;

TTableReaderPtr<TNode> CreateSkiffTableReader(IInputStream* stream, const TSkiffSchemaPtr& schema)
{
    auto impl = ::MakeIntrusive<TSkiffTableReader>(::MakeIntrusive<TInputStreamProxy>(stream), schema);
    return new TTableReader<TNode>(impl);
}

int main(int argc, const char** argv) {
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " <path> [ skiff | yson ] [ <schema-path> ]" << Endl;
        return 1;
    }

    Initialize(argc, argv);
    const TString ytProxy = "freud";
    const TString path = argv[1];
    const TString format = argv[2];

    TIFStream input(path);
    TTableReaderPtr<TNode> reader;
    if (format == "skiff") {
        if (argc < 4) {
            Cerr << "Need <schema-path> argument when skiff is chosen" << Endl;
            return 1;
        }
        TSkiffSchemaPtr schema;
        Deserialize(schema, NodeFromYsonString(TIFStream(argv[3]).ReadAll()));
        reader = CreateSkiffTableReader(&input, schema);
    } else if (format == "yson") {
        reader = CreateTableReader<TNode>(&input);
    } else {
        Cerr << "Unknown format " << format << Endl;
        return 1;
    }

    size_t count = 0;
    for ( ; reader->IsValid(); reader->Next()) {
        count += reader->GetRow().Size();
    }
    TOFStream os("/dev/null");
    os << count;

    return 0;
}
