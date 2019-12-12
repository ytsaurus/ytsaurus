#include <yp/client/api/native/client.h>
#include <yp/client/api/native/config.h>
#include <yp/client/api/native/helpers.h>
#include <yp/client/api/native/payload.h>
#include <yp/client/api/native/request.h>
#include <yp/client/api/native/response.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/shutdown.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/test_framework/framework.h>

#include <util/system/env.h>

namespace NYP::NClient::NApi::NNative {
namespace {

using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class TNativeClientTestSuite
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        Client_ = CreateTestClient();
    }

    virtual void TearDown() override
    {
        Client_.Reset();
    }

    const IClientPtr& GetClient() const
    {
        return Client_;
    }

    static void WaitForPodAssignment(
        const IClientPtr& client,
        const TObjectId& podId)
    {
        WaitForPredicate([&] {
            return IsAssignedPodSchedulingStatus(GetPodSchedulingStatus(client, podId));
        });
    }

    static NProto::TPodStatus_TScheduling GetPodSchedulingStatus(
        const IClientPtr& client,
        const TObjectId& podId)
    {
        auto payloads = WaitFor(client->GetObject(
            podId,
            EObjectType::Pod,
            {"/status/scheduling"}))
            .ValueOrThrow()
            .Result
            .ValuePayloads;

        NProto::TPodStatus_TScheduling schedulingStatus;
        ParsePayloads(
            payloads,
            &schedulingStatus);

        return schedulingStatus;
    }

    static bool IsAssignedPodSchedulingStatus(
        const NProto::TPodStatus_TScheduling& schedulingStatus)
    {
        return !schedulingStatus.has_error()
            && !schedulingStatus.node_id().empty()
            && schedulingStatus.state() == NProto::ESchedulingState::SS_ASSIGNED;
    }

    static NProto::TPodStatus_TEviction GetPodEvictionStatus(
        const IClientPtr& client,
        const TObjectId& podId)
    {
        auto payloads = WaitFor(client->GetObject(
            podId,
            EObjectType::Pod,
            {"/status/eviction"}))
            .ValueOrThrow()
            .Result
            .ValuePayloads;

        NProto::TPodStatus_TEviction evictionStatus;
        ParsePayloads(
            payloads,
            &evictionStatus);

        return evictionStatus;
    }

private:
    IClientPtr Client_;


    static IClientPtr CreateTestClient()
    {
        auto address = GetEnv("YP_MASTER_GRPC_INSECURE_ADDR");
        auto configNode = NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("connection")
                    .BeginMap()
                        .Item("secure").Value(false)
                        .Item("grpc_channel")
                            .BeginMap()
                                .Item("address").Value(address)
                            .EndMap()
                        .Item("authentication")
                            .BeginMap()
                                .Item("user").Value("root")
                            .EndMap()
                    .EndMap()
            .EndMap();

        auto config = New<TClientConfig>();
        config->Load(configNode);

        auto client = CreateClient(std::move(config));

        return client;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTimestamp GenerateTimestamp(const IClientPtr& client)
{
    return WaitFor(client->GenerateTimestamp())
        .ValueOrThrow()
        .Timestamp;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, GenerateTimestampTest)
{
    const auto& client = GetClient();

    auto timestamp1 = GenerateTimestamp(client);
    auto timestamp2 = GenerateTimestamp(client);

    EXPECT_LT(0u, timestamp1);
    EXPECT_LT(timestamp1, timestamp2);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, SelectObjectsTest)
{
    const auto& client = GetClient();

    auto timestamp = GenerateTimestamp(client);

    auto result = WaitFor(client->SelectObjects(
        EObjectType::Account,
        {"/meta/id", "/meta/acl", "/spec"}))
        .ValueOrThrow();

    EXPECT_LT(timestamp, result.Timestamp);

    EXPECT_EQ(1u, result.Results.size());

    EXPECT_EQ(0u, result.Results[0].Timestamps.size());

    TObjectId id;
    std::vector<NProto::TAccessControlEntry> acl;
    NProto::TAccountSpec spec;

    ParsePayloads(
        result.Results[0].ValuePayloads,
        &id,
        &acl,
        &spec);

    EXPECT_EQ("tmp", id);
    EXPECT_EQ("", spec.parent_id());

    const auto& defaultSegmentTotals = spec.resource_limits().per_segment().at("default");
    EXPECT_LT(0, defaultSegmentTotals.cpu().capacity());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, ParsePayloadsTest)
{
    // Incorrect number of arguments.
    {
        std::vector<TPayload> payloads;
        TString attribute;
        EXPECT_THROW(ParsePayloads(payloads, &attribute), TErrorException);
    }

    // Parsing yson string.
    {
        std::vector<TPayload> payloads{TYsonPayload{TYsonString("abcde")}};
        TYsonString attribute;
        ParsePayloads(payloads, &attribute);
        EXPECT_EQ(EYsonType::Node, attribute.GetType());
        EXPECT_EQ("abcde", attribute.GetData());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, CreateObjectTest)
{
    const auto& client = GetClient();

    auto podSetId = WaitFor(client->CreateObject(EObjectType::PodSet))
        .ValueOrThrow()
        .ObjectId;

    EXPECT_LT(0u, podSetId.size());

    TSelectObjectsOptions options;
    options.Filter = Format("[/meta/id] = %Qv",
        podSetId);
    auto selectResult = WaitFor(client->SelectObjects(
        EObjectType::PodSet,
        {"/meta/id", "/spec/account_id"},
        options))
        .ValueOrThrow();

    EXPECT_EQ(1u, selectResult.Results.size());

    TObjectId id;
    TObjectId accountId;

    ParsePayloads(
        selectResult.Results[0].ValuePayloads,
        &id,
        &accountId);

    EXPECT_EQ(id, podSetId);
    EXPECT_EQ("tmp", accountId);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, RequestAbortPodEvictionTest)
{
    const auto& client = GetClient();

    auto nodeId = WaitFor(client->CreateObject(EObjectType::Node))
        .ValueOrThrow()
        .ObjectId;

    for (TStringBuf kind : {"cpu", "memory", "slot"}) {
        WaitFor(client->CreateObject(
            EObjectType::Resource,
            BuildYsonPayload(BIND([&nodeId, kind] (IYsonConsumer* consumer) {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("meta")
                            .BeginMap()
                                .Item("node_id").Value(nodeId)
                            .EndMap()
                        .Item("spec")
                            .BeginMap()
                                .Item(kind)
                                    .BeginMap()
                                        .Item("total_capacity").Value(100500L * 100500)
                                    .EndMap()
                            .EndMap()
                    .EndMap();
            }))))
            .ValueOrThrow();
    }

    WaitFor(UpdateNodeHfsmState(
        client,
        nodeId,
        EHfsmState::Up,
        "Test"))
        .ValueOrThrow();

    auto podSetId = WaitFor(client->CreateObject(EObjectType::PodSet))
        .ValueOrThrow()
        .ObjectId;

    auto podId = WaitFor(client->CreateObject(
        EObjectType::Pod,
        BuildYsonPayload(BIND([&podSetId] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("meta")
                        .BeginMap()
                            .Item("pod_set_id").Value(podSetId)
                        .EndMap()
                    .Item("spec")
                        .BeginMap()
                            .Item("enable_scheduling").Value(true)
                        .EndMap()
                .EndMap();
        }))))
        .ValueOrThrow()
        .ObjectId;

    WaitForPodAssignment(client, podId);

    auto requestEviction = [&] {
        WaitFor(RequestPodEviction(
            client,
            podId,
            "Test",
            /*validateDisruptionBudget*/ false))
            .ValueOrThrow();
    };

    auto abortEviction = [&] {
        WaitFor(AbortPodEviction(
            client,
            podId,
            "Test"))
            .ValueOrThrow();
    };

    auto getEvictionState = [&] {
        return GetPodEvictionStatus(client, podId).state();
    };

    EXPECT_EQ(NProto::EEvictionState::ES_NONE, getEvictionState());

    requestEviction();
    EXPECT_EQ(NProto::EEvictionState::ES_REQUESTED, getEvictionState());

    EXPECT_THROW(requestEviction(), TErrorException);

    abortEviction();
    EXPECT_EQ(NProto::EEvictionState::ES_NONE, getEvictionState());

    EXPECT_THROW(abortEviction(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTestSuite, BatchSelectObjects)
{
    const auto& client = GetClient();

    static constexpr size_t OBJECT_COUNT = 10;
    for (size_t objectIndex = 0; objectIndex < OBJECT_COUNT; ++objectIndex) {
        WaitFor(client->CreateObject(EObjectType::PodSet))
            .ValueOrThrow();
    }

    auto parseObjectIds = [] (const TSelectObjectsResult& selectResult) {
        std::vector<TObjectId> ids;
        ids.reserve(selectResult.Results.size());
        for (const auto& result : selectResult.Results) {
            auto& id = ids.emplace_back();
            ParsePayloads(
                result.ValuePayloads,
                &id);
        }
        return ids;
    };

    auto expectedIds = parseObjectIds(WaitFor(client->SelectObjects(
        EObjectType::PodSet,
        {"/meta/id"}))
        .ValueOrThrow());
    EXPECT_LE(OBJECT_COUNT, expectedIds.size());

    TSelectObjectsOptions options;
    options.Limit = 3;
    auto ids = parseObjectIds(BatchSelectObjects(
        client,
        EObjectType::PodSet,
        {"/meta/id"},
        options));

    std::sort(expectedIds.begin(), expectedIds.end());
    EXPECT_EQ(expectedIds, ids);
}

TEST_F(TNativeClientTestSuite, BatchSelectObjectsOptions)
{
    const auto& client = GetClient();

    auto select = [&] (const TSelectObjectsOptions& options) {
        return BatchSelectObjects(
            client,
            EObjectType::PodSet,
            {"/meta/id"},
            options);
    };

    TSelectObjectsOptions options;
    EXPECT_THROW(select(options), TErrorException);

    options.Limit = -10;
    EXPECT_THROW(select(options), TErrorException);

    options.Limit = 0;
    EXPECT_THROW(select(options), TErrorException);

    options.Offset.emplace();
    EXPECT_THROW(select(options), TErrorException);

    options.Offset = std::nullopt;
    options.Limit = 10;
    select(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NClient::NApi::NNative
