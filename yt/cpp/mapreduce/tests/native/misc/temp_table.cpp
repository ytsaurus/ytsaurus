#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/scope.h>
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

TEST(TempTableTestSuite, Simple)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString tmpTableName;
    {
        auto tmpTable = TTempTable(client, "table", workingDir + "");
        tmpTableName = tmpTable.Name();
        EXPECT_TRUE(client->Exists(tmpTableName));
    }
    EXPECT_TRUE(!client->Exists(tmpTableName));
}

TEST(TempTableTestSuite, MoveSemantics)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString tmpTableName1;
    TString tmpTableName2;
    {
        TTempTable tmpTable1 = GenerateTempTable(client, "table", workingDir + "");
        TTempTable tmpTable2 = GenerateTempTable(client, "table", workingDir + "");
        tmpTableName1 = tmpTable1.Name();
        tmpTableName2 = tmpTable2.Name();
        EXPECT_TRUE(client->Exists(tmpTableName1));
        EXPECT_TRUE(client->Exists(tmpTableName2));

        tmpTable1 = std::move(tmpTable2);
        EXPECT_TRUE(!client->Exists(tmpTableName1));
        EXPECT_TRUE(client->Exists(tmpTableName2));

        TTempTable& tmpTable3 = tmpTable1;
        tmpTable1 = std::move(tmpTable3);
        EXPECT_TRUE(client->Exists(tmpTableName2));
    }
    EXPECT_TRUE(!client->Exists(tmpTableName1));
    EXPECT_TRUE(!client->Exists(tmpTableName2));
}

TEST(TempTableTestSuite, UsageInContainers)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TVector<TTempTable> tmpTableGuard;
    TVector<TString> tmpTableNames;
    for (ui32 i = 0; i < 100; ++i)
    {
        tmpTableGuard.emplace_back(client, "table", workingDir + "");
    }
    for (const TTempTable& tmpTable: tmpTableGuard) {
        EXPECT_TRUE(client->Exists(tmpTable.Name()));
        tmpTableNames.push_back(tmpTable.Name());
    }
    tmpTableGuard.clear();
    for (const TString& tmpTableName: tmpTableNames) {
        EXPECT_TRUE(!client->Exists(tmpTableName));
    }
}

TEST(TempTableTestSuite, Release)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString tmpTableName;
    {
        auto tmpTable = TTempTable(client, "table", workingDir + "");
        tmpTableName = tmpTable.Name();
        EXPECT_TRUE(client->Exists(tmpTableName));
        EXPECT_EQ(tmpTable.Release(), tmpTableName);
    }
    EXPECT_TRUE(client->Exists(tmpTableName));
}

TEST(TempTableTestSuite, KeepTempTables)
{
    // TODO(max42): uncomment this when TTempTable looks on a client's config instead of the singleton,
    // and remove the global config patching.
    //
    // TConfigPtr config = MakeIntrusive<TConfig>();
    // config->KeepTempTables = true;
    // TTestFixture fixture(TCreateClientOptions().Config(config));

    TConfig::Get()->KeepTempTables = true;
    Y_DEFER {
        TConfig::Get()->KeepTempTables = false;
    };
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TString tmpTableName;
    {
        auto tmpTable = TTempTable(client, "table", workingDir + "");
        tmpTableName = tmpTable.Name();
        EXPECT_TRUE(client->Exists(tmpTableName));
    }
    EXPECT_TRUE(client->Exists(tmpTableName));
}

TEST(TempTableTestSuite, ConcurrentTempTables)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto tx1 = client->StartTransaction();
    TTempTable tmpTable1(tx1);

    auto tx2 = client->StartTransaction();
    TTempTable tmpTable2(tx2);
}

TEST(TempTableTestSuite, ConcurrentTempTablesGlobalTx)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();

    auto tx1 = client->StartTransaction();
    auto config1 = MakeIntrusive<NYT::TConfig>();
    config1->GlobalTxId = ToString(tx1->GetId());
    auto client1 = CreateClient(
        fixture.GetYtProxy(),
        TCreateClientOptions()
            .Config(config1)
    );
    TTempTable tmpTable1(client1);

    auto tx2 = client->StartTransaction();
    auto config2 = MakeIntrusive<NYT::TConfig>();
    config2->GlobalTxId = ToString(tx2->GetId());
    auto client2 = CreateClient(
        fixture.GetYtProxy(),
        TCreateClientOptions()
            .Config(config2)
    );
    TTempTable tmpTable2(client2);
}
