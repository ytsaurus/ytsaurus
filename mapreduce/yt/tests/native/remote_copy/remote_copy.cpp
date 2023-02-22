#include <mapreduce/yt/interface/config.h>

#include <mapreduce/yt/interface/client.h>

#include <library/cpp/yson/node/node_io.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>
#include <util/generic/vector.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

TString GetEnvChecked(const TString& name) {
    auto value = GetEnv(name);
    if (value.empty()) {
        ythrow yexception() << name << " is not specified" << Endl;
    }
    return value;
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(RemoteCopy) {
    Y_UNIT_TEST(Simple)
    {
        auto dstClient = CreateTestClient(GetEnvChecked("YT_PROXY_DST"));
        auto srcClient = CreateTestClient(GetEnvChecked("YT_PROXY_SRC"));
        auto dstTestingDir = CreateTestDirectory(dstClient);
        auto srcTestingDir = CreateTestDirectory(srcClient);

        TVector<TNode> writtenRows = {TNode()("foo", "bar")};
        {
            auto writer = srcClient->CreateTableWriter<TNode>(srcTestingDir + "/input");
            for (const auto& row : writtenRows) {
                writer->AddRow(row);
            }
            writer->Finish();
        }
        srcClient->Set(srcTestingDir + "/input/@my_attribute", TNode(42));

        dstClient->RemoteCopy(
            TRemoteCopyOperationSpec()
            .ClusterName(GetEnvChecked("YT_SRC_CLUSTER_NAME"))
            .AddInput(srcTestingDir + "/input")
            .Output(dstTestingDir + "/output")
            .CopyAttributes(true)
            .AddAttributeKey("my_attribute"));

        auto reader = dstClient->CreateTableReader<TNode>(dstTestingDir + "/output");
        TVector<TNode> outputRows;
        for (; reader->IsValid(); reader->Next()) {
            outputRows.push_back(reader->MoveRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(writtenRows, outputRows);
        UNIT_ASSERT_VALUES_EQUAL(dstClient->Get(dstTestingDir + "/output/@my_attribute"), TNode(42));
    }
}
