#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_computation_base.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/state_provider.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

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

    TFuture<void> RemoveJob(const TJobId& /*jobId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TCompanionJobList> ListJobs() override
    {
        return MakeFuture(TCompanionJobList{});
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

class TFakeJoinedStateKeyProvider
    : public IJoinedStateKeyProvider
{
public:
    THashMap<TKey, IStateHolderPtr> States;

    TFakeJoinedStateKeyProvider(
        TTableSchemaPtr keySchema,
        bool hasKeySchemaOverride,
        std::optional<THashSet<TStreamId>> keyProviderStreams)
        : KeySchema_(std::move(keySchema))
        , HasKeySchemaOverride_(hasKeySchemaOverride)
        , KeyProviderStreams_(std::move(keyProviderStreams))
        , ConverterCache_(CreatePayloadConverterCache(
            NQueryClient::CreateColumnEvaluatorCache(New<NQueryClient::TColumnEvaluatorCacheConfig>())))
    { }

    IStateHolderPtr GetState(const TKey& key) override
    {
        auto it = States.find(key);
        return it == States.end() ? nullptr : it->second;
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& /*keys*/) override
    {
        return OKFuture;
    }

    TTableSchemaPtr GetKeySchema() const override
    {
        return KeySchema_;
    }

    const IPayloadConverterCachePtr& GetConverterCache() const override
    {
        return ConverterCache_;
    }

    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override
    {
        return KeyProviderStreams_;
    }

    bool HasKeySchemaOverride() const override
    {
        return HasKeySchemaOverride_;
    }

private:
    const TTableSchemaPtr KeySchema_;
    const bool HasKeySchemaOverride_;
    const std::optional<THashSet<TStreamId>> KeyProviderStreams_;
    const IPayloadConverterCachePtr ConverterCache_;
};

const auto PayloadSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="word"; type="string";};])"""));

const auto OverrideKeySchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([
        {name="hash"; type="uint64"; expression="farm_hash(word)"; required=%true; sort_order="ascending";};
        {name="word"; type="string"; sort_order="ascending";};
    ])"""));

constexpr auto ValidTs = TSystemTimestamp(1'500'000'000);

TInputMessageConstPtr MakeMessage(const std::string& streamId, const std::string& word, const TKey& key)
{
    TMessageBuilder builder(TStreamId(streamId), PayloadSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue(word, 0));
    builder.SetMessageId(TMessageId(word));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    return New<TInputMessage>(builder.Finish(), key);
}

// The joiner block of the companion hosts must send a joiner the extract-derived keys — key schema
// override and key-provider filters applied over the whole batch — never the per-message group-by
// key.
TEST(TAddJoinedExternalStatesTest, UsesExtractDerivedKeys)
{
    auto provider = New<TFakeJoinedStateKeyProvider>(
        OverrideKeySchema,
        /*hasKeySchemaOverride*/ true,
        /*keyProviderStreams*/ THashSet<TStreamId>{TStreamId("in")});
    TJoinedStateKeyClient<TSimpleExternalState> client(provider);

    auto message = MakeMessage("in", "abc", MakeKey("group-by-key"));
    auto excludedMessage = MakeMessage("other", "def", MakeKey("def"));
    auto input = New<TInputContext>(
        std::vector<TInputMessageConstPtr>{message, excludedMessage},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    // States exist for the group-by keys too, so a host reverting to per-message keys would still
    // find a state — and fail on key identity below.
    auto overrideKey = client.ResolveKey(message);
    provider->States[overrideKey] = New<NFlow::TStateHolder<TSimpleExternalState>>();
    provider->States[MakeKey("group-by-key")] = New<NFlow::TStateHolder<TSimpleExternalState>>();
    provider->States[MakeKey("def")] = New<NFlow::TStateHolder<TSimpleExternalState>>();

    auto request = New<TCompanionProcessRequest>();
    THashMap<std::string, TJoinedStateKeyClient<TSimpleExternalState>> joiners;
    joiners.emplace("/j", client);

    AddJoinedExternalStates(request, joiners, input);

    const auto& holder = GetOrCrash(request->JoinedExternalStates, "/j");
    ASSERT_EQ(std::ssize(holder.StateItems), 1);
    EXPECT_EQ(holder.StateItems[0].Key, overrideKey);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
