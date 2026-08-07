#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/server.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TServerUnittestFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

TEST(TCompanionServerTest, CompanionInfoAndGetJfr)
{
    auto port = NTesting::GetFreePort();

    auto config = New<NCompanion::TCompanionExecutionConfig>();
    config->Port = port;

    TPipeline pipeline;
    pipeline.AddTransform<TServerUnittestFunction>("my_transform");
    pipeline.AddSource<TServerUnittestFunction>("my_source");

    auto server = New<TCompanionServer>(config, pipeline);
    server->Start();

    auto proxy = NCompanion::CreateCompanionProxy(Format("localhost:%v", static_cast<int>(port)));

    {
        auto req = proxy.CompanionInfo();
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_OK);

        auto info = ConvertTo<NCompanion::TCompanionInfoPtr>(NYson::TYsonStringBuf(rsp->payload()));
        ASSERT_EQ(std::ssize(info->Computations), 2);
        EXPECT_EQ(
            info->Computations["my_transform"]->CompanionComputationType,
            ECompanionComputationType::Transform);
        EXPECT_EQ(
            info->Computations["my_source"]->CompanionComputationType,
            ECompanionComputationType::Source);
    }

    {
        auto req = proxy.GetJfr();
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_ERROR);
        EXPECT_THAT(rsp->error_message(), testing::HasSubstr("JFR"));
    }

    server->Stop();
}

TEST(TCompanionServerTest, PutJob)
{
    auto port = NTesting::GetFreePort();

    auto config = New<NCompanion::TCompanionExecutionConfig>();
    config->Port = port;

    TPipeline pipeline;
    pipeline.AddTransform<TServerUnittestFunction>("my_transform");

    auto server = New<TCompanionServer>(config, pipeline);
    server->Start();

    auto proxy = NCompanion::CreateCompanionProxy(Format("localhost:%v", static_cast<int>(port)));

    auto buildRequest = [&] (const std::string& computationId) {
        auto req = proxy.PutJob();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), TGuid::Create());
        req->set_computation_id(computationId);
        auto* jobInfo = req->mutable_job_info();
        jobInfo->set_spec(R"({computation_class_name = "Shim"})");
        jobInfo->set_dynamic_spec("{}");
        return req;
    };

    {
        auto rsp = buildRequest("my_transform")->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    }

    {
        // A computation this companion does not host fails the RPC itself.
        auto rspOrError = buildRequest("unknown")->Invoke().BlockingGet();
        ASSERT_FALSE(rspOrError.IsOK());
        EXPECT_THAT(
            ToString(static_cast<const TError&>(rspOrError)),
            testing::HasSubstr("is not registered in this companion"));
    }

    server->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
