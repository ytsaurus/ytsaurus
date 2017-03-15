#include <mapreduce/yt/tests/lib/lib.h>
#include <mapreduce/yt/tests/lib/owning_yamr_row.h>

#include <mapreduce/interface/all.h>

#include <mapreduce/yt/interface/client.h>

#include <util/string/builder.h>
#include <util/stream/null.h>
#include <util/system/env.h>

namespace NYT {
namespace NCommonTest {

using namespace NTest;

////////////////////////////////////////////////////////////////////////////////

class TFormatAttribute
    : public NTest::TTest
{
    public:
        void SetUp() override
        {
            TTest::SetUp();
            Server.Reset(new NMR::TServer(ServerName()));

            auto client = CreateClient(ServerName());
            client->Remove("//home/testing", TRemoveOptions().Recursive(true).Force(true));
            client->Create("//home/testing", NYT::ENodeType::NT_MAP);

            {
                auto writer = client->CreateTableWriter<NYT::TNode>("//home/testing/yamr_table");
                writer->AddRow(NYT::TNode()("key", "gg")("value", "lol"));
                writer->AddRow(NYT::TNode()("key", "foo")("value", "bar"));
                writer->Finish();
                client->Set("//home/testing/yamr_table/@_format", "yamr");
            }
            {
                auto writer = client->CreateTableWriter<NYT::TNode>("//home/testing/yamred_dsv_table");
                writer->AddRow(NYT::TNode()("a", "gg")("b", "lol"));
                writer->AddRow(NYT::TNode()("a", "foo")("b", "bar"));
                writer->Finish();

                TNode format("yamred_dsv");
                format.Attributes()
                    ("key_column_names", TNode().Add("b"))
                    ("has_subkey", false);
                client->Set("//home/testing/yamred_dsv_table/@_format", format);
            }
        }

        void CreatePath(const Stroka& path)
        {
            Y_ENSURE(path.StartsWith("home/testing"),
                "Bug in test code: all paths must be in home/testing directory. " << path);

            NMR::TClient client(GetServer());
            NMR::TUpdate update(client, path);
            update.Add("foo", "bar");
        }

        void CreateBrokenSymlink(const Stroka& path)
        {
            Y_ENSURE(path.StartsWith("home/testing"),
                "Bug in test code: all pathes must be in home/testing directory. " << path);
        }

        NMR::TServer& GetServer()
        {
            return *Server;
        }

    private:
        THolder<NMR::TServer> Server;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFormatAttribute, Read_YamredDsv)
{
    NMR::TClient client(GetServer());
    NMR::TTable table(client, "home/testing/yamred_dsv_table");

    yvector<TOwningYaMRRow> actualRowList;
    for (auto it = table.Begin(); it.IsValid(); ++it) {
        actualRowList.emplace_back(it.GetKey().AsString(), it.GetSubKey().AsString(), it.GetValue().AsString());
    }

    const yvector<TOwningYaMRRow> expectedRowList = {
        {"lol", "", "a=gg"},
        {"bar", "", "a=foo"},
    };

    EXPECT_EQ(actualRowList, expectedRowList);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFormatAttribute, Read_Yamr)
{
    NMR::TClient client(GetServer());
    NMR::TTable table(client, "home/testing/yamr_table");

    yvector<TOwningYaMRRow> actualRowList;
    for (auto it = table.Begin(); it.IsValid(); ++it) {
        actualRowList.emplace_back(it.GetKey().AsString(), it.GetSubKey().AsString(), it.GetValue().AsString());
    }

    const yvector<TOwningYaMRRow> expectedRowList = {
        {"gg", "", "lol"},
        {"foo", "", "bar"},
    };

    EXPECT_EQ(actualRowList, expectedRowList);
}

////////////////////////////////////////////////////////////////////////////////


class TSwapKVMap
    : public NMR::IMap
{
    OBJECT_METHODS(TSwapKVMap);
public:
    void DoSub(NMR::TValue k, NMR::TValue s, NMR::TValue v, NMR::TUpdate& update) override {
        update.AddSub(v, s, k);
    }
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFormatAttribute, Operation_Yamr)
{

    GetServer().Map("home/testing/yamr_table", "home/testing/output", new TSwapKVMap);

    NMR::TClient client(GetServer());
    NMR::TTable table(client, "home/testing/output");
    yvector<TOwningYaMRRow> actualRowList;
    for (auto it = table.Begin(); it.IsValid(); ++it) {
        actualRowList.emplace_back(it.GetKey().AsString(), it.GetSubKey().AsString(), it.GetValue().AsString());
    }

    const yvector<TOwningYaMRRow> expectedRowList = {
        {"lol", "", "gg"},
        {"bar", "", "foo"},
    };

    EXPECT_EQ(actualRowList, expectedRowList);
}

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TFormatAttribute, Operation_YamredDsv)
{
    GetServer().Map("home/testing/yamred_dsv_table", "home/testing/output", new TSwapKVMap);

    NMR::TClient client(GetServer());
    NMR::TTable table(client, "home/testing/output");

    yvector<TOwningYaMRRow> actualRowList;
    for (auto it = table.Begin(); it.IsValid(); ++it) {
        actualRowList.emplace_back(it.GetKey().AsString(), it.GetSubKey().AsString(), it.GetValue().AsString());
    }

    const yvector<TOwningYaMRRow> expectedRowList = {
        {"a=gg", "", "lol"},
        {"a=foo", "", "bar"},
    };

    EXPECT_EQ(actualRowList, expectedRowList);
}

////////////////////////////////////////////////////////////////////////////////

} // NCommonTest
} // NYT

using NYT::NCommonTest::TSwapKVMap;
REGISTER_SAVELOAD_CLASS(0x24, TSwapKVMap);
