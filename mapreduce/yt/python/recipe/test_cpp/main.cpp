#include <util/system/env.h>
#include <util/stream/file.h>
#include <library/unittest/registar.h>
#include <mapreduce/yt/interface/client.h>

SIMPLE_UNIT_TEST_SUITE(Suite)
{
    SIMPLE_UNIT_TEST(Test)
    {
        TString ytProxy = GetEnv("YT_PROXY");

        auto client = NYT::CreateClient(ytProxy);

        UNIT_ASSERT(!client->Exists("//tmp/table"));
        client->Create("//tmp/table", NYT::NT_TABLE);
        UNIT_ASSERT(client->Exists("//tmp/table"));
    }
};

