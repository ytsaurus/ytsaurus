#include <mapreduce/yt/tests/lib/lib.h>

#include <mapreduce/interface/all.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

namespace {

class TFixture
    : public NTest::TTest
{
public:
    void SetUp() override {
        TTest::SetUp();
        Server.Reset(new TServer(ServerName()));
    }

    void TearDown() override {
        TTest::TearDown();
    }

    TServer& GetServer() {
        return *Server;
    }

private:
    THolder<TServer> Server;
};

void PrintTableIterator(TTableIterator&& it) {
#define CALL_METHOD(it, method) \
    Cout << "\t" #method ": " << it.method() << Endl;

    CALL_METHOD(it, IsValid);
    CALL_METHOD(it, GetTableIndex);
    CALL_METHOD(it, GetRecordIndex);

#undef CALL_METHOD
}


void DoIteratorTest(TServer& server, const char* tableName) {
    Cout << "=======" << tableName << "=======" << Endl;
    TClient client(server);
    TTable table(client, tableName);

#define CALL_METHOD_K(key, method) \
    Cout << "Key: '" << key << "'\n"; \
    PrintTableIterator(table.method(key));

#define CHECK_METHOD(table, method) \
    Cout << "------CHECK " #method "------" << Endl; \
    CALL_METHOD_K("a", method); \
    CALL_METHOD_K("b", method); \
    CALL_METHOD_K("c", method); \
    CALL_METHOD_K("d", method); \
    CALL_METHOD_K("e", method); \
    CALL_METHOD_K("g", method); \
    CALL_METHOD_K("i", method); \
    CALL_METHOD_K("j", method); \
    CALL_METHOD_K("aa", method); \
    CALL_METHOD_K("bb", method); \
    CALL_METHOD_K("0", method); \
    CALL_METHOD_K("ee", method); \
    CALL_METHOD_K("", method); \
    CALL_METHOD_K("zzz", method);

    CHECK_METHOD(table, Find);
    CHECK_METHOD(table, LowerBound);
    CHECK_METHOD(table, UpperBound);

#undef CHECK_METHOD
#undef CALL_METHOD_K
}

void PrintTable(TServer& server, const char* tableName) {
    Cout << "~~~~~~" << tableName << "~~~~~~" << Endl;
    TClient client(server);
    TTable table(client, tableName);
    for (auto&& it = table.Begin(); it != table.End(); ++it) {
        Cout << it.GetKey().AsString()
            << "\t" << it.GetSubKey().AsString()
            << "\t" << it.GetValue().AsString()
            << "\n";
    }
}

} // anonymous namespace

YT_TEST(TFixture, IteratorMethods) {
    const char* TABLE = "tmp/table";
    const char* SORTED_TABLE = "tmp/sorted_table";
    {
        TClient client(GetServer());
        TUpdate update(client, TABLE);
        TUpdate updateSorted(client, SORTED_TABLE, UM_SORTED);
        for (int i = 0; i < 5; ++i) {
            auto key = Sprintf("%c", 'a' + (i * 2));
            auto subkey = Sprintf("%d", i * 2);
            auto value = Sprintf("%d", i * 4);
            update.AddSub(key, subkey, value);
            updateSorted.AddSub(key, subkey, value);
        }
    }

    const char* EMPTY_TABLE = "tmp/empty_table";
    {
        TClient client(GetServer());
        TUpdate update(client, EMPTY_TABLE);
    }

    const char* UNEXIST_TABLE = "tmp/unexist_table";

    //PrintTable(GetServer(), TABLE);
    //PrintTable(GetServer(), SORTED_TABLE);

    DoIteratorTest(GetServer(), TABLE);
    DoIteratorTest(GetServer(), SORTED_TABLE);
    DoIteratorTest(GetServer(), EMPTY_TABLE);
    DoIteratorTest(GetServer(), UNEXIST_TABLE);

    {
        TClient client(GetServer());
        client.Drop(TABLE);
        client.Drop(SORTED_TABLE);
        client.Drop(EMPTY_TABLE);
    }
}

} // NCommonTest
} // NYT
