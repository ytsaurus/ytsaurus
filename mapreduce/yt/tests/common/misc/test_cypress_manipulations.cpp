#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

#include <mapreduce/yt/interface/client.h>

#include <util/string/builder.h>
#include <util/stream/null.h>
#include <util/system/env.h>

namespace NYT {
namespace NCommonTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum EOpaqueType
{
    OT_STRING,
    OT_BOOL,
};

bool IsYt()
{
    return GetEnv("MR_RUNTIME") == "YT";
}

Stroka MaybeStripPrefix(const Stroka& path)
{
    // Right now GetTableList doesn't strip `//' prefix :(
    const Stroka prefix = "//";
    if (path.StartsWith(prefix)) {
        return path.substr(prefix.size());
    } else {
        return path;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCypressManipulations
    : public NTest::TTest
{
    public:
        void SetUp() override
        {
            TTest::SetUp();
            Server.Reset(new NMR::TServer(ServerName()));

            {
                NMR::TClient client(GetServer());
                yvector<Stroka> tableList;
                client.GetTableList(&tableList, NMR::GT_PREFIX_MATCH, "home/testing");
                for (const auto& table : tableList) {
                    client.Drop(table);
                }
            }

            if (IsYt()) {
                // Some tests generate trash like broken symlinks
                // that is not removed by previous piece of code.
                auto client = CreateClient(ServerName());
                client->Remove("//home/testing", TRemoveOptions().Recursive(true).Force(true));
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

            if (!IsYt()) {
                return;
            }

            auto client = CreateClient(ServerName());
            const Stroka targetPath = "//tmp_path_for_broken_link";
            client->Create(targetPath, NT_TABLE);
            client->Link(targetPath, "//" + path);
            client->Remove(targetPath);
        }

        NMR::TServer& GetServer()
        {
            return *Server;
        }

        void SetOpaque(const Stroka& path, EOpaqueType type, bool value)
        {
            if (!IsYt()) {
                return;
            }

            TNode opaqueValue;
            switch (type) {
                case OT_STRING:
                    opaqueValue = TNode(value ? "true" : "false");
                case OT_BOOL:
                    opaqueValue = TNode(value);
                    break;
                default:
                    Y_FAIL("unkwnown opaque type");
            }

            auto client = CreateClient(ServerName());
            client->Set("//" + path + "/@opaque", opaqueValue);
        }

    private:
        THolder<NMR::TServer> Server;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TCypressManipulations, GetTableList_Prefix_Simple)
{
    CreatePath("home/testing/foo");
    CreatePath("home/testing/bar");
    CreatePath("home/testing/baz");

    yvector<Stroka> tableList;
    NMR::TClient client(GetServer());
    client.GetTableList(&tableList, NMR::GT_PREFIX_MATCH, "home/testing");
    for (const auto& table : tableList) {
        Cout << MaybeStripPrefix(table) << Endl;
    }
}

YT_TEST(TCypressManipulations, GetTableList_Prefix_Opaque)
{

    const yvector<Stroka> expectedPaths = {
        "home/testing/bool_false/1",
        "home/testing/bool_false/2",
        "home/testing/bool_false/3",
        "home/testing/bool_true/4",
        "home/testing/bool_true/5",
        "home/testing/bool_true/6",
        "home/testing/none/7",
        "home/testing/none/8",
        "home/testing/none/9",
        "home/testing/string_false/10",
        "home/testing/string_false/11",
        "home/testing/string_false/12",
        "home/testing/string_true/13",
        "home/testing/string_true/14",
        "home/testing/string_true/15",
    };

    for (const auto& path : expectedPaths) {
        CreatePath(path);
    }

    SetOpaque("home/testing/bool_false", OT_BOOL, false);
    SetOpaque("home/testing/bool_true", OT_BOOL, true);
    SetOpaque("home/testing/string_false", OT_STRING, false);
    SetOpaque("home/testing/string_true", OT_STRING, true);

    yvector<Stroka> tableList;
    NMR::TClient client(GetServer());
    client.GetTableList(&tableList, NMR::GT_PREFIX_MATCH, "home/testing");
    Sort(tableList.begin(), tableList.end());
    UNIT_ASSERT_VALUES_EQUAL(tableList, expectedPaths);
}

YT_TEST(TCypressManipulations, GetTableList_Prefix_OpaqueString)
{
    CreatePath("home/testing/tt/foo");
    CreatePath("home/testing/tt/bar");
    CreatePath("home/testing/tt/baz");
    SetOpaque("home/testing/tt", OT_STRING, true);

    yvector<Stroka> tableList;
    NMR::TClient client(GetServer());
    client.GetTableList(&tableList, NMR::GT_PREFIX_MATCH, "home/testing");
    for (const auto& table : tableList) {
        Cout << MaybeStripPrefix(table) << Endl;
    }
}

YT_TEST(TCypressManipulations, GetTableList_Prefix_BrokenSymlink)
{
    CreatePath("home/testing/foo");
    CreatePath("home/testing/bar");
    CreateBrokenSymlink("home/testing/baz");

    yvector<Stroka> tableList;
    NMR::TClient client(GetServer());
    client.GetTableList(&tableList, NMR::GT_PREFIX_MATCH, "home/testing");
    for (const auto& table : tableList) {
        Cout << MaybeStripPrefix(table) << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // NCommonTest
} // NYT
