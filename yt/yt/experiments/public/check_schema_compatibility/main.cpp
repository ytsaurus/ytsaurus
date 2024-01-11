#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/check_schema_compatibility.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/stream/file.h>
#include <util/generic/set.h>

#include <set>

using namespace NYT;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

void PrintUsage(const char* argv0, IOutputStream& out)
{
    out << "Usage: " << argv0 << " <old-schema-file> <new-schema-file>" << Endl;
}

int main(int argc, const char** argv)
{
    static const std::set<TString> helpFlags = {"-h", "--help"};

    for (int i = 1; i < argc; ++i) {
        if (helpFlags.count(argv[i])) {
            PrintUsage(argv[0], Cout);
            return 0;
        }
    }
    if (argc != 3) {
        PrintUsage(argv[0], Cerr);
        return 1;
    }

    TString oldSchemaText = TFileInput(argv[1]).ReadAll();
    TString newSchemaText = TFileInput(argv[2]).ReadAll();

    auto oldSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(oldSchemaText));
    auto newSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(newSchemaText));

    const auto& [result, error] = CheckTableSchemaCompatibility(*oldSchema, *newSchema, false);
    Cout << ToString(result) << Endl;
    if (!error.IsOK()) {
        Cout << ToString(error) << Endl;
    }
    return 0;
}
