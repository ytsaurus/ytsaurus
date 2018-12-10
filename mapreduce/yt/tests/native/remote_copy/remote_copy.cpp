#include <mapreduce/yt/common/config.h>

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/node/node_io.h>

#include <mapreduce/yt/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/unittest/registar.h>

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

        TVector<TNode> writtenRows = {TNode()("foo", "bar")};
        {
            auto writer = srcClient->CreateTableWriter<TNode>("//testing/input");
            for (const auto& row : writtenRows) {
                writer->AddRow(row);
            }
            writer->Finish();
        }
        srcClient->Set("//testing/input/@my_attribute", TNode(42));

        dstClient->RemoteCopy(
            TRemoteCopyOperationSpec()
            .ClusterName(GetEnvChecked("YT_SRC_CLUSTER_NAME"))
            .AddInput("//testing/input")
            .Output("//testing/output")
            .CopyAttributes(true)
            .AddAttributeKey("my_attribute"));

        auto reader = dstClient->CreateTableReader<TNode>("//testing/output");
        TVector<TNode> outputRows;
        for (; reader->IsValid(); reader->Next()) {
            outputRows.push_back(reader->MoveRow());
        }

        UNIT_ASSERT_VALUES_EQUAL(writtenRows, outputRows);
        UNIT_ASSERT_VALUES_EQUAL(dstClient->Get("//testing/output/@my_attribute"), TNode(42));
    }
}
