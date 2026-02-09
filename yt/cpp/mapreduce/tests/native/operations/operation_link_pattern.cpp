#include "jobs.h"
#include "helpers.h"

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/client/operation_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NTesting;

TEST(OperationLinkPattern, CustomPattern)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto configPath = workingDir + "/client_config/default";
    TNode::TMapType configValue;
    configValue["operation_link_pattern"] = "test/{cluster_ui_host}/{operation_id}";
    client->Create(
        configPath,
        ENodeType::NT_MAP,
        TCreateOptions()
            .Recursive(true));
    client->Set(configPath, configValue, TSetOptions().Force(true));

    auto otherConfig = new TConfig();
    otherConfig->ConfigRemotePatchPath = workingDir + "/client_config";

    auto otherClient = fixture.CreateClient(
        TCreateClientOptions()
            .Config(otherConfig));

    auto input = workingDir + "/input";
    CreateTableWithFooColumn(otherClient, input);

    auto output = workingDir + "/output";

    auto mergeOperation = otherClient->Merge(
        TMergeOperationSpec()
            .AddInput(input)
            .Output(output)
            .MergeBy({"foo"}),
        TOperationOptions());

    auto proxy = fixture.GetYtProxy();
    auto operationId = ToString(mergeOperation->GetId());
    EXPECT_EQ(mergeOperation->GetWebInterfaceUrl(), "test/" + proxy + "/" + operationId);
}

TEST(OperationLinkPattern, BadType)
{
    TTestFixture fixture;

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    auto configPath = workingDir + "/client_config/default";
    TNode::TMapType configValue;
    configValue["operation_link_pattern"] = 1337;
    client->Create(
        configPath,
        ENodeType::NT_MAP,
        TCreateOptions()
            .Recursive(true));
    client->Set(configPath, configValue, TSetOptions().Force(true));

    auto otherConfig = new TConfig();
    otherConfig->ConfigRemotePatchPath = workingDir + "/client_config";

    auto otherClient = fixture.CreateClient(
        TCreateClientOptions()
            .Config(otherConfig));

    auto input = workingDir + "/input";
    CreateTableWithFooColumn(otherClient, input);

    auto output = workingDir + "/output";

    EXPECT_THROW(auto mergeOperation = otherClient->Merge(
        TMergeOperationSpec()
            .AddInput(input)
            .Output(output)
            .MergeBy({"foo"}),
        TOperationOptions()), yexception);
}
