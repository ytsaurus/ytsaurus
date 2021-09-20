#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>

namespace NYT {
namespace NCommonTest {

using namespace NMR;

////////////////////////////////////////////////////////////////////////////////

class TMetaAttrs
    : public NTest::TTest
{
public:
    void SetUp() override
    {
        TTest::SetUp();
        Server_ = StealToHolder(new TServer(ServerName()));
        RemoveTables();
    }

    void TearDown() override
    {
        RemoveTables();
        TTest::TearDown();
    }

    TServer& Server() { return *Server_; }
    const char* Table() { return "tmp/table"; }

protected:
    void PrintAttrs()
    {
        TClient client(Server());
        TTableMetaAttrs attrs;
        client.GetTableMetaAttrs(Table(), attrs);

        TVector<std::pair<TString, TString>> sorted;
        for (auto it = attrs.Begin(); it != attrs.End(); ++it) {
            sorted.push_back(std::make_pair(it.GetName(), it.GetValueAsStroka()));
        }
        std::sort(sorted.begin(), sorted.end());
        Cout << "{" << Endl;
        for (auto& it : sorted) {
            Cout << it.first << "=" << it.second << Endl;
        }
        Cout << "}" << Endl;
    }

    void UpdateTable(TClient& client)
    {
        TUpdate update(client, Table());
        update.Add("a", "b");
    }

    void SetEmptyAttrs(TClient& client)
    {
        TTableMetaAttrs attrs;
        client.SetTableMetaAttrs(Table(), attrs);
    }

    void SetSomeAttrs(TClient& client)
    {
        TTableMetaAttrs attrs;
        attrs.SetAttr("x", TString("1"));
        attrs.SetAttr("y", TString("10"));
        client.SetTableMetaAttrs(Table(), attrs);
    }

    void AppendSomeAttrs(TClient& client)
    {
        TTableMetaAttrs attrs;
        attrs.SetAttr("y", TString("foo"));
        attrs.SetAttr("z", TString("bar"));
        client.AppendTableMetaAttrs(Table(), attrs);
    }

private:
    void RemoveTables()
    {
        TClient client(Server());
        client.Drop(Table());
    }

    THolder<TServer> Server_;
};

////////////////////////////////////////////////////////////////////////////////

YT_TEST(TMetaAttrs, Simple)
{
    {
        TClient client(Server());
        UpdateTable(client);
    }
    {
        TClient client(Server());
        SetSomeAttrs(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, SetOnEmptyTable)
{
    {
        TClient client(Server());
        SetSomeAttrs(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, SetEmpty)
{
    {
        TClient client(Server());
        UpdateTable(client);
    }
    {
        TClient client(Server());
        SetEmptyAttrs(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, SetAndUpdate)
{
    {
        TClient client(Server());
        SetSomeAttrs(client);
        UpdateTable(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, UpdateAndSet)
{
    {
        TClient client(Server());
        UpdateTable(client);
        SetSomeAttrs(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, SetAndRemove)
{
    {
        TClient client(Server());
        UpdateTable(client);
        SetSomeAttrs(client);
    }
    {
        TClient client(Server());
        SetEmptyAttrs(client);
    }
    PrintAttrs();
}

YT_TEST(TMetaAttrs, Append)
{
    {
        TClient client(Server());
        UpdateTable(client);
        SetSomeAttrs(client);
    }
    {
        TClient client(Server());
        AppendSomeAttrs(client);
    }
    PrintAttrs();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCommonTest
} // namespace NYT

