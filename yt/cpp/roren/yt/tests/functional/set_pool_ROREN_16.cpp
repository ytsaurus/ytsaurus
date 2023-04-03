#include <library/cpp/testing/unittest/registar.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>
#include <yt/cpp/roren/yt/yt_read.h>
#include <yt/cpp/roren/yt/yt_write.h>

#include <library/cpp/yson/node/node.h>
#include <util/generic/ptr.h>
#include <util/system/env.h>

using namespace NYT::NTesting;
using namespace NRoren;
using NYT::TNode;

static TString GetTestProxy()
{
    // TODO: this should be returned by TTestFixture
    return GetEnv("YT_PROXY");
}

Y_UNIT_TEST_SUITE(SetPool_ROREN16) {
    Y_UNIT_TEST(SetPool_ROREN16)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const std::vector<TNode> data = {
            TNode()("value", "foo"),
            TNode()("value", "bar"),
        };

        auto writer = ytClient->CreateTableWriter<TNode>(inTablePath);
        for (const auto& row : data) {
            writer->AddRow(row);
        }
        writer->Finish();

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        config.SetPool("UniqueNameForYtPool");
        // If transaction doesn't attached then in-table invisible for roren and test fail
        auto pipeline = MakeYtPipeline(config);
        auto inTable = pipeline | YtRead<TNode>(inTablePath);
        inTable | YtWrite<TNode>(
            outTablePath,
            NYT::TTableSchema()
                .AddColumn("value", NTi::String())
        );

        pipeline.Run();

        std::vector<TNode> readData;
        auto reader = ytClient->CreateTableReader<TNode>(outTablePath);
        for (auto& cursor : *reader) {
            readData.emplace_back(cursor.MoveRow());
        }
        UNIT_ASSERT_VALUES_EQUAL(data, readData);

        auto result = ytClient->ListOperations(NYT::TListOperationsOptions());
        UNIT_ASSERT_VALUES_EQUAL(result.PoolCounts->at("UniqueNameForYtPool"), 1);
    }
}
