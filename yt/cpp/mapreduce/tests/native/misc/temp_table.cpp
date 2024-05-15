#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <library/cpp/testing/gtest/gtest.h>

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

template <class T, class R = decltype(std::declval<T>().Name())>
bool HasDangerousNameMethod(const T*) {
    return true;
}

bool HasDangerousNameMethod(...) {
    return false;
}

TEST(TempTableTestSuite, DangerousNameMethod)
{
    TTempTable* tt = nullptr;
    EXPECT_TRUE(!HasDangerousNameMethod(tt));
}
