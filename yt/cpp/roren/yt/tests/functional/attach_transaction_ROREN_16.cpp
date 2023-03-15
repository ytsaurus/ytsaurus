#include <library/cpp/testing/unittest/registar.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>
#include <mapreduce/yt/interface/client.h>
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

Y_UNIT_TEST_SUITE(AttachTransactionROREN16) {
    Y_UNIT_TEST(Read)
    {
        TTestFixture f;
        const auto& ytClient = f.GetClient();
        auto inTablePath = f.GetWorkingDir() + "/in-table";
        auto outTablePath = f.GetWorkingDir() + "/out-table";
        const std::vector<TNode> data = {
            TNode()("value", "foo"),
            TNode()("value", "bar"),
        };

        auto tx = ytClient->StartTransaction();

        {
            auto writer = tx->CreateTableWriter<TNode>(inTablePath);
            for (const auto& row : data) {
                writer->AddRow(row);
            }
            writer->Finish();
        }

        TYtPipelineConfig config;
        config.SetCluster(GetTestProxy());
        config.SetWorkingDir("//tmp");
        // If transaction doesn't attached then in-table invisible for roren and test fail
        config.SetTransactionId(GetGuidAsString(tx->GetId()));
        auto pipeline = MakeYtPipeline(config);
        auto inTable = pipeline | YtRead<TNode>(inTablePath);
        inTable | YtWrite<TNode>(
            outTablePath,
            NYT::TTableSchema()
                .AddColumn("value", NTi::String())
        );

        pipeline.Run();

        std::vector<TNode> readData;
        {
            auto reader = tx->CreateTableReader<TNode>(outTablePath);
            for (auto& cursor : *reader) {
                readData.emplace_back(cursor.MoveRow());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(data, readData);
    }
}
