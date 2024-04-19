#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(TempTableTestSuite) {
    Y_UNIT_TEST(Simple)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        TString tmpTableName;
        {
            auto tmpTable = TTempTable(client, "table", workingDir + "");
            tmpTableName = tmpTable.Name();
            UNIT_ASSERT(client->Exists(tmpTableName));
        }
        UNIT_ASSERT(!client->Exists(tmpTableName));
    }

    Y_UNIT_TEST(MoveSemantics)
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

    Y_UNIT_TEST(UsageInContainers)
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
            UNIT_ASSERT(client->Exists(tmpTable.Name()));
            tmpTableNames.push_back(tmpTable.Name());
        }
        tmpTableGuard.clear();
        for (const TString& tmpTableName: tmpTableNames) {
            UNIT_ASSERT(!client->Exists(tmpTableName));
        }
    }

    Y_UNIT_TEST(Release)
    {
        TTestFixture fixture;
        auto client = fixture.GetClient();
        auto workingDir = fixture.GetWorkingDir();
        TString tmpTableName;
        {
            auto tmpTable = TTempTable(client, "table", workingDir + "");
            tmpTableName = tmpTable.Name();
            UNIT_ASSERT(client->Exists(tmpTableName));
            UNIT_ASSERT_VALUES_EQUAL(tmpTable.Release(), tmpTableName);
        }
        UNIT_ASSERT(client->Exists(tmpTableName));
    }

    template <class T, class R = decltype(std::declval<T>().Name())>
    bool HasDangerousNameMethod(const T*) {
        return true;
    }

    bool HasDangerousNameMethod(...) {
        return false;
    }

    Y_UNIT_TEST(DangerousNameMethod)
    {
        TTempTable* tt = nullptr;
        UNIT_ASSERT(!HasDangerousNameMethod(tt));
    }
}
