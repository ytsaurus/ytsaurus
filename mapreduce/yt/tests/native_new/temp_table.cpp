#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <mapreduce/yt/util/temp_table.h>

#include <library/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/system/types.h>

#include <utility>

using namespace NYT;
using namespace NYT::NTesting;

// this function was by design written complicated to force copy constructor in the "Function" test case
static TTempTable GenerateTempTable(IClientBasePtr client, const TString& tablePrefix, const TString& tablePath) {
    auto tempTable1 = TTempTable(client, tablePrefix, tablePath);
    auto tempTable2 = TTempTable(client, tablePrefix, tablePath);
    if (tempTable1.Name().EndsWith("3")) {
        return tempTable1;
    }
    return tempTable2;
}

SIMPLE_UNIT_TEST_SUITE(TempTableTestSuite) {
    SIMPLE_UNIT_TEST(Simple)
    {
        auto client = CreateTestClient();
        TString tmpTableName;
        {
            auto tmpTable = TTempTable(client, "table", "//testing");
            tmpTableName = tmpTable.Name();
            UNIT_ASSERT(client->Exists(tmpTableName));
        }
        UNIT_ASSERT(!client->Exists(tmpTableName));
    }

    SIMPLE_UNIT_TEST(MoveSemantics)
    {
        auto client = CreateTestClient();
        TString tmpTableName1;
        TString tmpTableName2;
        {
            TTempTable tmpTable1 = GenerateTempTable(client, "table", "//testing");
            TTempTable tmpTable2 = GenerateTempTable(client, "table", "//testing");
            tmpTableName1 = tmpTable1.Name();
            tmpTableName2 = tmpTable2.Name();
            UNIT_ASSERT(client->Exists(tmpTableName1));
            UNIT_ASSERT(client->Exists(tmpTableName2));

            tmpTable1 = std::move(tmpTable2);
            UNIT_ASSERT(!client->Exists(tmpTableName1));
            UNIT_ASSERT(client->Exists(tmpTableName2));

            TTempTable& tmpTable3 = tmpTable1;
            tmpTable1 = std::move(tmpTable3);
            UNIT_ASSERT(client->Exists(tmpTableName2));
        }
        UNIT_ASSERT(!client->Exists(tmpTableName1));
        UNIT_ASSERT(!client->Exists(tmpTableName2));
    }

    SIMPLE_UNIT_TEST(UsageInContainers)
    {
        auto client = CreateTestClient();
        TVector<TTempTable> tmpTableGuard;
        TVector<TString> tmpTableNames;
        for (ui32 i = 0; i < 100; ++i)
        {
            tmpTableGuard.emplace_back(client, "table", "//testing");
        }
        for (const TTempTable& tmpTable: tmpTableGuard) {
            UNIT_ASSERT(client->Exists(tmpTable.Name()));
            tmpTableNames.push_back(tmpTable.Name());
        }
        tmpTableGuard.clear();
        for (const TString& tmpTableName: tmpTableNames) {
            UNIT_ASSERT(!client->Exists(tmpTableName));
        }
    }
}
