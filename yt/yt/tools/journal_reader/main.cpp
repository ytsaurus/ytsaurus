#include "private.h"
#include "mutation_dumper.h"
#include "journal_dumper.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/getopt/last_getopt.h>

using namespace NYT;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYT::NYson;

using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

auto ReadRecords(const TString& filename)
{
    // TODO: Use pull parser instead of materializing everything at once.
    auto str = TIFStream(filename).ReadAll();
    return ConvertTo<std::vector<IMapNodePtr>>(TYsonString(str, EYsonType::ListFragment));
}


TTableSchemaPtr LoadSchema(const TString& filename)
{
    TIFStream stream(filename);
    return ConvertTo<TTableSchemaPtr>(TYsonString(stream.ReadAll()));
}

////////////////////////////////////////////////////////////////////////////////

void GuardedMain(int argc, char* argv[])
{
    TString schemaFile;
    TOpts opts;
    opts.SetFreeArgsNum(1);
    opts.SetFreeArgTitle(0, "journal", "Journal dump (yson list fragment)");

    TString tabletIdStr;
    TString schemaPath;
    opts.AddLongOption("tablet-id")
        .StoreResult(&tabletIdStr)
        .Required();
    opts.AddLongOption("schema-path")
        .StoreResult(&schemaPath)
        .Required();

    TOptsParseResult result(&opts, argc, argv);

    auto journalPath = result.GetFreeArgs()[0];
    auto records = ReadRecords(journalPath);

    auto schema = LoadSchema(schemaPath);
    auto tabletId = FromProto<NTabletClient::TTabletId>(tabletIdStr);

    TJournalDumper dumper;
    dumper.AddMutationDumper(CreateWriteRowsDumper(tabletId, schema));

    dumper.Dump(records);
}

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }

    Shutdown();
}

////////////////////////////////////////////////////////////////////////////////
