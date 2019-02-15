#include <util/system/env.h>
#include <util/stream/file.h>
#include <library/unittest/registar.h>
#include <mapreduce/yt/interface/client.h>

using namespace NYT;

namespace {

    void AwaitTabletState(
        NYT::IClientPtr ytc,
        const TString& tableName,
        const TString& targetState)
    {
        const TString tablets_path = tableName + "/@tablets";

        while (true) {
            const TNode& tablets = ytc->Get(tablets_path);
            bool ok = true;
            for (const TNode& n: tablets.AsList()) {
                if (n["state"].AsString() != targetState) {
                    ok = false;
                    break;
                }
            }
            if (ok) return;

            Sleep(TDuration::Seconds(1));
        }
    }
}

Y_UNIT_TEST_SUITE(Suite)
{
    Y_UNIT_TEST(TestCreateTable)
    {
        TString ytProxy = GetEnv("YT_PROXY");

        auto client = NYT::CreateClient(ytProxy);

        UNIT_ASSERT(!client->Exists("//tmp/table"));
        client->Create("//tmp/table", NYT::NT_TABLE);
        UNIT_ASSERT(client->Exists("//tmp/table"));
    }

    Y_UNIT_TEST(TestDynamicTable)
    {
        TString ytProxy = GetEnv("YT_PROXY");

        auto client = NYT::CreateClient(ytProxy);

        TTableSchema schema;
        schema.AddColumn("key", VT_STRING, SO_ASCENDING);
        schema.AddColumn("value", VT_STRING);

        TNode attrs;
        attrs("dynamic", true);
        attrs("schema", schema.ToNode());

        client->Create("//tmp/dynamic_table", NYT::NT_TABLE, TCreateOptions().Attributes(attrs));
        client->MountTable("//tmp/dynamic_table");
        AwaitTabletState(client, "//tmp/dynamic_table", "mounted");

        TNode row;
        row("key", "answer");
        row("value", "42");

        client->InsertRows("//tmp/dynamic_table", {row});

        auto resultRows = client->SelectRows("* from [//tmp/dynamic_table]");
        UNIT_ASSERT_VALUES_EQUAL(resultRows.size(), 1);
        UNIT_ASSERT_EQUAL(resultRows[0], row);
    }
};

