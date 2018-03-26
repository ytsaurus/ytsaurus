#include <util/stream/file.h>
#include <library/unittest/registar.h>
#include <mapreduce/yt/interface/client.h>

SIMPLE_UNIT_TEST_SUITE(Suite)
{
    SIMPLE_UNIT_TEST(Test)
    {
        TFileInput f("yt_proxy_port.txt");
        TString port;
        f.ReadLine(port);

        auto client = NYT::CreateClient("localhost:" + port);

        UNIT_ASSERT(!client->Exists("//tmp/table"));
        client->Create("//tmp/table", NYT::NT_TABLE);
        UNIT_ASSERT(client->Exists("//tmp/table"));
    }
};

