#include <util/system/env.h>
#include <util/stream/file.h>
#include <library/cpp/unittest/registar.h>
#include <mapreduce/yt/interface/client.h>

Y_UNIT_TEST_SUITE(Suite)
{
    Y_UNIT_TEST(Test)
    {
        TString ytProxy = GetEnv("YT_PROXY");

        auto client = NYT::CreateClient(ytProxy);

        UNIT_ASSERT(!client->Exists("//tmp/table"));
        client->Create("//tmp/table", NYT::NT_TABLE);
        UNIT_ASSERT(client->Exists("//tmp/table"));
    }
};

