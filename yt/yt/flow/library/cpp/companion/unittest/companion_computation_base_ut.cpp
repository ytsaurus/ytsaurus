#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_computation_base.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

//! Companion client whose ProcessBatch responses are scripted by the test.
class TFakeProcessCompanionClient
    : public ICompanionClient
{
public:
    //! |statuses| are consumed by successive DoProcessWithCompanionSync calls;
    //! Ok when exhausted.
    explicit TFakeProcessCompanionClient(std::vector<ECompanionResponseStatus> statuses)
        : ScriptedStatuses_(std::move(statuses))
    { }

    TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& companionRequest,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        auto callIndex = SendJobInfoFlags_.size();
        SendJobInfoFlags_.push_back(companionRequest->SendJobInfo);
        CompanionResourceReferences_.push_back(companionRequest->CompanionResources);
        auto response = New<TCompanionResponse>();
        response->Status = callIndex < ScriptedStatuses_.size()
            ? ScriptedStatuses_[callIndex]
            : ECompanionResponseStatus::Ok;
        return response;
    }

    TCompanionInfoPtr GetCompanionInfo() override
    {
        YT_UNIMPLEMENTED();
    }

    TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& /*putJobRequest*/,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TCompanionResourceExecuteResponsePtr> ResourceExecute(
        const TResourceId& /*resourceId*/,
        ECompanionResourceCommand /*command*/,
        const NYson::TYsonString& /*argument*/) override
    {
        YT_UNIMPLEMENTED();
    }

    const std::vector<bool>& GetSendJobInfoFlags() const
    {
        return SendJobInfoFlags_;
    }

    const std::vector<std::vector<TCompanionResourceInstanceReference>>& GetCompanionResourceReferences() const
    {
        return CompanionResourceReferences_;
    }

private:
    const std::vector<ECompanionResponseStatus> ScriptedStatuses_;
    std::vector<bool> SendJobInfoFlags_;
    std::vector<std::vector<TCompanionResourceInstanceReference>> CompanionResourceReferences_;
};

using TFakeProcessCompanionClientPtr = TIntrusivePtr<TFakeProcessCompanionClient>;

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessWithCompanionHealingTest, HealsUninitializedResource)
{
    auto client = New<TFakeProcessCompanionClient>(std::vector{
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::Ok,
    });
    auto request = New<TCompanionProcessRequest>();
    int putRequiredCount = 0;

    auto response = ProcessWithCompanionHealing(
        client,
        request,
        /*reporter*/ nullptr,
        [&] {
            ++putRequiredCount;
            return request->CompanionResources;
        });

    EXPECT_EQ(ECompanionResponseStatus::Ok, response->Status);
    EXPECT_EQ(1, putRequiredCount);
    EXPECT_EQ(2u, client->GetSendJobInfoFlags().size());
    // Resource healing recreates the cached job so it rebinds resources.
    EXPECT_TRUE(client->GetSendJobInfoFlags()[1]);
}

TEST(TProcessWithCompanionHealingTest, RefreshesReferencesAfterResourceHealing)
{
    auto client = New<TFakeProcessCompanionClient>(std::vector{
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::Ok,
    });
    auto request = New<TCompanionProcessRequest>();
    auto incarnationId = TResourceInstanceId(TGuid::Create());
    TCompanionResourceInstanceReference staleReference;
    staleReference.ResourceId = "resource";
    staleReference.IncarnationId = incarnationId;
    staleReference.ConfigurationGeneration = 1;
    request->CompanionResources.push_back(staleReference);

    auto response = ProcessWithCompanionHealing(
        client,
        request,
        /*reporter*/ nullptr,
        [&] {
            TCompanionResourceInstanceReference currentReference;
            currentReference.ResourceId = "resource";
            currentReference.IncarnationId = incarnationId;
            currentReference.ConfigurationGeneration = 2;
            return std::vector{currentReference};
        });

    EXPECT_EQ(ECompanionResponseStatus::Ok, response->Status);
    ASSERT_EQ(2u, client->GetCompanionResourceReferences().size());
    ASSERT_EQ(1u, client->GetCompanionResourceReferences()[0].size());
    EXPECT_EQ(1u, client->GetCompanionResourceReferences()[0][0].ConfigurationGeneration);
    ASSERT_EQ(1u, client->GetCompanionResourceReferences()[1].size());
    EXPECT_EQ(2u, client->GetCompanionResourceReferences()[1][0].ConfigurationGeneration);
    EXPECT_TRUE(client->GetSendJobInfoFlags()[1]);
}

TEST(TProcessWithCompanionHealingTest, HealsJobAndResourceBackToBack)
{
    // After a companion restart both statuses can occur back to back.
    auto client = New<TFakeProcessCompanionClient>(std::vector{
        ECompanionResponseStatus::JobNotFound,
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::Ok,
    });
    auto request = New<TCompanionProcessRequest>();
    int putRequiredCount = 0;

    auto response = ProcessWithCompanionHealing(
        client,
        request,
        /*reporter*/ nullptr,
        [&] {
            ++putRequiredCount;
            return request->CompanionResources;
        });

    EXPECT_EQ(ECompanionResponseStatus::Ok, response->Status);
    EXPECT_EQ(1, putRequiredCount);
    const std::vector<bool> expectedSendJobInfoFlags{false, true, true};
    EXPECT_EQ(expectedSendJobInfoFlags, client->GetSendJobInfoFlags());
}

TEST(TProcessWithCompanionHealingTest, BoundedAtThreeAttempts)
{
    auto client = New<TFakeProcessCompanionClient>(std::vector{
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::ResourceNotInitialized,
        ECompanionResponseStatus::Ok,
    });
    auto request = New<TCompanionProcessRequest>();
    int putRequiredCount = 0;

    auto response = ProcessWithCompanionHealing(
        client,
        request,
        /*reporter*/ nullptr,
        [&] {
            ++putRequiredCount;
            return request->CompanionResources;
        });

    EXPECT_EQ(ECompanionResponseStatus::ResourceNotInitialized, response->Status);
    EXPECT_EQ(2, putRequiredCount);
    EXPECT_EQ(3u, client->GetSendJobInfoFlags().size());
}

TEST(TProcessWithCompanionHealingTest, DoesNotRetryOtherStatuses)
{
    auto client = New<TFakeProcessCompanionClient>(std::vector{
        ECompanionResponseStatus::Error,
    });
    auto request = New<TCompanionProcessRequest>();
    int putRequiredCount = 0;

    auto response = ProcessWithCompanionHealing(
        client,
        request,
        /*reporter*/ nullptr,
        [&] {
            ++putRequiredCount;
            return request->CompanionResources;
        });

    EXPECT_EQ(ECompanionResponseStatus::Error, response->Status);
    EXPECT_EQ(0, putRequiredCount);
    EXPECT_EQ(1u, client->GetSendJobInfoFlags().size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
