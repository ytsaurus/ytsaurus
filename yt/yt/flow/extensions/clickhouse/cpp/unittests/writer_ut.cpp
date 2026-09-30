#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/columns/numeric.h>

#include <atomic>
#include <system_error>
#include <tuple>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TWriterOwner
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TWriterOwner);

////////////////////////////////////////////////////////////////////////////////

class TCountingStatusErrorState
    : public IStatusErrorState
{
public:
    std::atomic<int> SetErrorCount = 0;
    std::atomic<int> ClearErrorCount = 0;

    void SetError(TError) override
    {
        ++SetErrorCount;
    }

    void ClearError() override
    {
        ++ClearErrorCount;
    }

    TStatus GetStatus() const override
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TCountingStatusErrorState);

////////////////////////////////////////////////////////////////////////////////

using TObservedDynamicParameters = std::tuple<TDuration, TDuration, i64, bool, TDuration>;

TObservedDynamicParameters ObserveDynamicParameters(
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    return {
        dynamicParameters->WriteTimeout,
        dynamicParameters->RetryBackoff,
        dynamicParameters->MaxInsertAttempts,
        dynamicParameters->AsyncInsert,
        dynamicParameters->ReplayHorizon,
    };
}

TClickHouseTargetMetadata MakeReplicatedMetadata(std::string replicaName)
{
    return {
        .Engine = "ReplicatedMergeTree",
        .Columns = {
            {.Name = "id", .Type = "Int64"},
            {.Name = "source", .Type = "String", .DefaultKind = "DEFAULT", .DefaultExpression = "'flow'"},
        },
        .ReplicationIdentity = TClickHouseReplicationIdentity{"default", "/tables/events"},
        .ReplicaName = std::move(replicaName),
    };
}

TEST(TClickHouseMetadataBlockTest, EmptyTerminalIsIgnoredAndMalformedBlocksAreRejected)
{
    EXPECT_FALSE(ShouldConsumeClickHouseMetadataBlock(0, 0, 1));
    EXPECT_TRUE(ShouldConsumeClickHouseMetadataBlock(1, 0, 1));
    EXPECT_TRUE(ShouldConsumeClickHouseMetadataBlock(1, 1, 1));
    EXPECT_THROW(ShouldConsumeClickHouseMetadataBlock(0, 1, 1), TErrorException);
    EXPECT_THROW(ShouldConsumeClickHouseMetadataBlock(1, 1, 2), TErrorException);
    EXPECT_THROW(ShouldConsumeClickHouseMetadataBlock(2, 0, 1), TErrorException);
}

TEST(TClickHouseDedupWindowTest, SelectsWindowForInsertMode)
{
    EXPECT_EQ(
        GetClickHouseDedupWindowSettingName(false),
        "replicated_deduplication_window_seconds");
    EXPECT_EQ(
        GetClickHouseDedupWindowSettingName(true),
        "replicated_deduplication_window_seconds_for_async_inserts");
}

TEST(TClickHouseTargetMetadataTest, IdentityAndCompleteSchemaAreRequired)
{
    TClickHouseShard shard{.Name = "a", .Hosts = {"a1", "a2"}, .Database = "default", .Table = "events"};
    const NLogging::TLogger logger("Test");
    auto first = MakeReplicatedMetadata("r1");
    auto second = MakeReplicatedMetadata("r2");
    EXPECT_NO_THROW(ValidateFailoverMetadata(shard, {first, second}, logger));
    const std::vector<std::function<void(TClickHouseTargetMetadata&)>> changes{
        [] (auto& value) {
            value.Engine.clear();
        },
        [] (auto& value) {
            value.Columns.clear();
        },
        [] (auto& value) {
            value.Engine = "ReplicatedReplacingMergeTree";
        },
        [] (auto& value) {
            value.Columns[1].Name = "other";
        },
        [] (auto& value) {
            value.Columns[1].Type = "Int64";
        },
        [] (auto& value) {
            value.Columns[1].DefaultKind = "MATERIALIZED";
        },
        [] (auto& value) {
            value.Columns[1].DefaultExpression = "'other'";
        },
        [] (auto& value) {
            std::swap(value.Columns[0], value.Columns[1]);
        },
        [] (auto& value) {
            value.ReplicationIdentity->ZookeeperName = "other";
        },
        [] (auto& value) {
            value.ReplicationIdentity->ZookeeperName.clear();
        },
        [] (auto& value) {
            value.ReplicationIdentity->ZookeeperPath = "/tables/other";
        },
        [] (auto& value) {
            value.ReplicationIdentity->ZookeeperPath.clear();
        },
        [] (auto& value) {
            value.ReplicaName.clear();
        },
        [] (auto& value) {
            value.ReplicationIdentity.reset();
        },
    };
    for (const auto& change : changes) {
        auto changed = second;
        change(changed);
        EXPECT_THROW(ValidateFailoverMetadata(shard, {first, changed}, logger), TErrorException);
    }
    EXPECT_THROW(ValidateFailoverMetadata(shard, {first}, logger), TErrorException);
    auto shared = first;
    shared.Engine = "SharedMergeTree";
    shared.ReplicationIdentity.reset();
    EXPECT_THROW(ValidateFailoverMetadata(shard, {shared, shared}, logger), TErrorException);
    shard.Hosts.resize(1);
    EXPECT_NO_THROW(ValidateFailoverMetadata(shard, {shared}, logger));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterTest, ReconfigureAffectsOnlyNextRequest)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();

    auto initialDynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    initialDynamicParameters->WriteTimeout = TDuration::Seconds(60);
    initialDynamicParameters->RetryBackoff = TDuration::Zero();
    initialDynamicParameters->MaxInsertAttempts = 3;
    initialDynamicParameters->AsyncInsert = false;
    initialDynamicParameters->ReplayHorizon = TDuration::Days(1);

    auto updatedDynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(120);
    updatedDynamicParameters->RetryBackoff = TDuration::Seconds(1);
    updatedDynamicParameters->MaxInsertAttempts = 1;
    updatedDynamicParameters->AsyncInsert = true;
    updatedDynamicParameters->ReplayHorizon = TDuration::Days(2);

    TClickHouseWriterPtr writer;
    int resetAttempts = 0;
    int insertAttempts = 0;
    std::vector<TObservedDynamicParameters> appliedDynamicParameters;
    std::vector<bool> asyncInsertValues;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
            ++resetAttempts;
        },
        .ApplyConnectionDynamicParameters = [&] (int /*shardIndex*/, const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters) {
            appliedDynamicParameters.push_back(ObserveDynamicParameters(dynamicParameters));
        },
        .Insert = [&] (int /*shardIndex*/, bool asyncInsert) {
            asyncInsertValues.push_back(asyncInsert);
            ++insertAttempts;
            if (insertAttempts == 1) {
                writer->Reconfigure(updatedDynamicParameters);
            }
            if (insertAttempts == 1 || insertAttempts == 2 || insertAttempts == 4) {
                throw std::runtime_error("insert failed");
            }
        },
    };

    writer = New<TClickHouseWriter>(
        parameters,
        initialDynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto firstWrite = writer->Write(
        EWriteGuarantee::AtLeastOnce,
        {TClickHouseShardWrite{}});
    auto secondWrite = writer->Write(
        EWriteGuarantee::AtLeastOnce,
        {TClickHouseShardWrite{}});

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    auto firstResult = WaitFor(firstWrite.WithTimeout(TDuration::Seconds(5)));
    auto secondResult = WaitFor(secondWrite.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(firstResult.IsOK());
    EXPECT_FALSE(secondResult.IsOK());
    EXPECT_EQ(secondResult.GetMessage(), "Giving up insert into ClickHouse");
    EXPECT_EQ(resetAttempts, 2);
    EXPECT_EQ(
        appliedDynamicParameters,
        std::vector<TObservedDynamicParameters>({
            ObserveDynamicParameters(initialDynamicParameters),
            ObserveDynamicParameters(updatedDynamicParameters),
        }));
    EXPECT_EQ(asyncInsertValues, std::vector<bool>({false, false, false, true}));

    owner.Reset();
    (void)writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterTest, ConfigurationFailureDoesNotReconnectOrAcknowledgeAtMostOnce)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();

    auto dynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    dynamicParameters->WriteTimeout = TDuration::Seconds(60);
    dynamicParameters->RetryBackoff = TDuration::Zero();
    dynamicParameters->MaxInsertAttempts = 3;

    int configurationAttempts = 0;
    int resetAttempts = 0;
    int insertAttempts = 0;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
            ++resetAttempts;
            throw std::system_error(std::make_error_code(std::errc::connection_refused));
        },
        .ApplyConnectionDynamicParameters = [&] (int /*shardIndex*/, const TDynamicCommonClickHouseSinkParametersPtr&) {
            ++configurationAttempts;
            if (configurationAttempts == 1) {
                throw std::runtime_error("configuration failed");
            }
        },
        .Insert = [&] (int /*shardIndex*/, bool /*asyncInsert*/) {
            ++insertAttempts;
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto writeFuture = writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    auto writeResult = WaitFor(writeFuture.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(writeResult.IsOK());
    EXPECT_EQ(configurationAttempts, 2);
    EXPECT_EQ(resetAttempts, 0);
    EXPECT_EQ(insertAttempts, 1);

    owner.Reset();
    (void)writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterTest, FailedResetStaysPendingUntilSuccess)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();

    auto dynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    dynamicParameters->WriteTimeout = TDuration::Seconds(60);
    dynamicParameters->RetryBackoff = TDuration::Zero();
    dynamicParameters->MaxInsertAttempts = 3;

    int configurationAttempts = 0;
    int resetAttempts = 0;
    int insertAttempts = 0;
    std::vector<std::string> events;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
            ++resetAttempts;
            events.push_back("reset");
            if (resetAttempts == 1) {
                throw std::system_error(std::make_error_code(std::errc::connection_refused));
            }
        },
        .ApplyConnectionDynamicParameters = [&] (int /*shardIndex*/, const TDynamicCommonClickHouseSinkParametersPtr&) {
            ++configurationAttempts;
            events.push_back("apply");
        },
        .Insert = [&] (int /*shardIndex*/, bool /*asyncInsert*/) {
            ++insertAttempts;
            events.push_back("insert");
            if (insertAttempts == 1) {
                throw std::system_error(std::make_error_code(std::errc::connection_reset));
            }
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto firstWrite = writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});
    auto secondWrite = writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    auto firstResult = WaitFor(firstWrite.WithTimeout(TDuration::Seconds(5)));
    auto secondResult = WaitFor(secondWrite.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(firstResult.IsOK());
    EXPECT_TRUE(secondResult.IsOK());
    EXPECT_EQ(configurationAttempts, 2);
    EXPECT_EQ(resetAttempts, 1);
    EXPECT_EQ(insertAttempts, 1);
    // The reconnect runs after the reconfigure, so recreating the client cannot rewind the
    // shard's endpoint cursor back onto the endpoint the reconnect was meant to leave.
    EXPECT_EQ(events, std::vector<std::string>({"apply", "insert", "apply", "reset"}));

    auto thirdWrite = writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});
    auto thirdResult = WaitFor(thirdWrite.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(thirdResult.IsOK());
    EXPECT_EQ(configurationAttempts, 3);
    EXPECT_EQ(resetAttempts, 2);
    EXPECT_EQ(insertAttempts, 2);
    EXPECT_EQ(
        events,
        std::vector<std::string>({
            "apply",
            "insert",
            "apply",
            "reset",
            "apply",
            "reset",
            "insert",
        }));

    owner.Reset();
    (void)writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{}});
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TCommonClickHouseSinkParametersPtr MakeThreeShardParameters()
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"ch-a"}},
        {"b", {"ch-b"}},
        {"c", {"ch-c"}},
    };
    parameters->Table = "events";
    return parameters;
}

std::vector<TClickHouseShardWrite> MakeThreeShardWrites()
{
    return {
        TClickHouseShardWrite{.ShardIndex = 0},
        TClickHouseShardWrite{.ShardIndex = 1},
        TClickHouseShardWrite{.ShardIndex = 2},
    };
}

TDynamicCommonClickHouseSinkParametersPtr MakeShardTestDynamicParameters(i64 maxInsertAttempts)
{
    auto dynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    dynamicParameters->WriteTimeout = TDuration::Seconds(60);
    dynamicParameters->RetryBackoff = TDuration::Zero();
    dynamicParameters->MaxInsertAttempts = maxInsertAttempts;
    return dynamicParameters;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterMetadataTest, VisitsEveryEndpointOnceAndPublishesAtomically)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a1", "a2"}},
        {"b", {"b1", "b2"}},
    };
    parameters->Table = "events";
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);

    std::vector<std::pair<int, std::string>> visits;
    std::vector<int> configuredShards;
    bool mismatchLaterEndpoint = false;
    bool failMultipleEndpoints = false;
    TClickHouseWriterTestHooks testHooks{
        .QueryEndpointMetadata = [&] (int shardIndex, const std::string& host) {
            visits.emplace_back(shardIndex, host);
            if (failMultipleEndpoints && (host == "a1" || host == "b2")) {
                throw std::runtime_error("metadata-failure-" + host);
            }
            auto metadata = MakeReplicatedMetadata(Format("replica-%v", host));
            metadata.ReplicationIdentity = TClickHouseReplicationIdentity{
                Format("keeper-%v", shardIndex),
                Format("/tables/events-%v", shardIndex),
            };
            if (mismatchLaterEndpoint && host == "b2") {
                metadata.Columns[1].Type = "Int64";
            }
            return metadata;
        },
        .ResetConnection = [] (int) {
        },
        .ApplyConnectionDynamicParameters = [&] (int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr&) {
            configuredShards.push_back(shardIndex);
        },
        .Insert = [] (int, bool) {
        },
    };

    auto profiler = CreateSyncStatusProfiler();
    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        profiler->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    mismatchLaterEndpoint = true;
    EXPECT_THROW(writer->Connect(), std::exception);
    EXPECT_EQ(
        visits,
        (std::vector<std::pair<int, std::string>>{
            {0, "a1"},
            {0, "a2"},
            {1, "b1"},
            {1, "b2"},
        }));
    EXPECT_TRUE(configuredShards.empty());
    EXPECT_TRUE(writer->GetTableColumns().empty());

    mismatchLaterEndpoint = false;
    visits.clear();
    EXPECT_NO_THROW(writer->Connect());
    EXPECT_EQ(
        visits,
        (std::vector<std::pair<int, std::string>>{
            {0, "a1"},
            {0, "a2"},
            {1, "b1"},
            {1, "b2"},
        }));
    EXPECT_EQ(configuredShards, (std::vector<int>{0, 1}));
    const auto publishedColumns = writer->GetTableColumns();

    auto changedDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    changedDynamicParameters->WriteTimeout = TDuration::Seconds(61);
    writer->Reconfigure(changedDynamicParameters);
    failMultipleEndpoints = true;
    visits.clear();
    EXPECT_NO_THROW(writer->Connect());
    EXPECT_TRUE(visits.empty());
    EXPECT_EQ(configuredShards, (std::vector<int>{0, 1, 0, 1}));
    EXPECT_EQ(writer->GetTableColumns(), publishedColumns);
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());
}

TEST(TClickHouseWriterMetadataTest, ConfigurationRetryReusesPublishedMetadata)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto profiler = CreateSyncStatusProfiler();
    int phase = 1;
    int metadataQueries = 0;
    TClickHouseWriterTestHooks testHooks{
        .QueryEndpointMetadata = [&] (int shardIndex, const std::string&) {
            ++metadataQueries;
            if (phase == 2 && shardIndex == 1) {
                throw std::runtime_error("metadata-failure-b");
            }
            return MakeReplicatedMetadata(Format("replica-%v", shardIndex));
        },
        .ResetConnection = [] (int) {
        },
        .ApplyConnectionDynamicParameters = [&] (int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr&) {
            if (phase == 1 && shardIndex == 0) {
                throw std::runtime_error("configuration-failure-a");
            }
        },
        .Insert = [] (int, bool) {
        },
    };
    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        profiler->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    EXPECT_THROW(writer->Connect(), std::exception);
    EXPECT_NE(
        ToString(profiler->GetStatus().Errors.at("/writing")).find("configuration-failure-a"),
        std::string::npos);
    EXPECT_EQ(metadataQueries, 3);

    phase = 2;
    EXPECT_NO_THROW(writer->Connect());
    EXPECT_EQ(metadataQueries, 3);
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());
}

TEST(TClickHouseWriterMetadataTest, WriteTimeoutReconfigureDoesNotRepeatEndpointIntrospection)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "host";
    parameters->Table = "events";
    auto initialDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto updatedDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);

    int metadataQueries = 0;
    int configurationAttempts = 0;
    bool failConfiguration = false;
    TClickHouseWriterTestHooks testHooks{
        .QueryEndpointMetadata = [&] (int, const std::string&) {
            ++metadataQueries;
            return MakeReplicatedMetadata("replica");
        },
        .ApplyConnectionDynamicParameters = [&] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
            ++configurationAttempts;
            if (failConfiguration) {
                failConfiguration = false;
                throw std::runtime_error("configuration failure");
            }
        },
    };
    auto writer = New<TClickHouseWriter>(
        parameters,
        initialDynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    EXPECT_NO_THROW(writer->Connect());
    EXPECT_EQ(metadataQueries, 1);
    EXPECT_EQ(configurationAttempts, 1);

    writer->Reconfigure(updatedDynamicParameters);
    failConfiguration = true;
    EXPECT_THROW(writer->Connect(), std::exception);
    EXPECT_EQ(metadataQueries, 1);
    EXPECT_EQ(configurationAttempts, 2);

    EXPECT_NO_THROW(writer->Connect());
    EXPECT_EQ(metadataQueries, 1);
    EXPECT_EQ(configurationAttempts, 3);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterShardTest, ShardsInsertSequentiallyAndRetryOnlyTheFailingShard)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 5);

    std::vector<int> insertedShards;
    std::vector<int> resetShards;
    int failingShardAttempts = 0;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int shardIndex) {
            resetShards.push_back(shardIndex);
        },
        .ApplyConnectionDynamicParameters = [&] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
        },
        .Insert = [&] (int shardIndex, bool /*asyncInsert*/) {
            insertedShards.push_back(shardIndex);
            if (shardIndex == 1 && ++failingShardAttempts <= 2) {
                throw std::system_error(std::make_error_code(std::errc::connection_reset));
            }
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    auto writeFuture = writer->Write(EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites());

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writeFuture.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_EQ(insertedShards, (std::vector<int>{0, 1, 1, 1, 2}));
    EXPECT_EQ(resetShards, (std::vector<int>{1, 1}));

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, AtMostOnceAcknowledgesPerShard)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);

    std::vector<int> insertedShards;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
        },
        .ApplyConnectionDynamicParameters = [&] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
        },
        .Insert = [&] (int shardIndex, bool /*asyncInsert*/) {
            insertedShards.push_back(shardIndex);
            if (shardIndex == 1) {
                throw std::runtime_error("shard is down");
            }
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    auto writeFuture = writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writeFuture.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_EQ(insertedShards, (std::vector<int>{0, 1, 2}));

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, HealthTracksEachFailingShardUntilItsRecovery)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto profiler = CreateSyncStatusProfiler();
    std::atomic<int> phase = 0;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [] (int) {
        },
        .ApplyConnectionDynamicParameters = [] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
        },
        .Insert = [&] (int shardIndex, bool) {
            if ((phase.load() == 0 && shardIndex < 2) ||
                (phase.load() == 1 && shardIndex == 0))
            {
                throw std::runtime_error(shardIndex == 0 ? "failure-a" : "failure-b");
            }
        },
    };
    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        profiler->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites())
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    auto errorText = ToString(profiler->GetStatus().Errors.at("/writing"));
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("failure-b"), std::string::npos);

    phase = 1;
    EXPECT_TRUE(WaitFor(writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites())
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    errorText = ToString(profiler->GetStatus().Errors.at("/writing"));
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("failure-b"), std::string::npos);

    phase = 2;
    EXPECT_TRUE(WaitFor(writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites())
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, ConfigurationRecoveryPreservesWritingError)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    dynamicParameters->RetryBackoff = TDuration::MilliSeconds(10);
    auto profiler = CreateSyncStatusProfiler();
    std::atomic<int> configurationPhase = 0;
    std::atomic<bool> failWriting = true;
    std::atomic<int> insertAttempts = 0;
    std::atomic<int> shard0ConfigurationAttempts = 0;
    std::atomic<int> shard1ConfigurationAttempts = 0;
    std::atomic<int> shard2ConfigurationAttempts = 0;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [] (int) {
        },
        .ApplyConnectionDynamicParameters = [&] (int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr&) {
            if (shardIndex == 0) {
                ++shard0ConfigurationAttempts;
            } else if (shardIndex == 1) {
                ++shard1ConfigurationAttempts;
            } else {
                ++shard2ConfigurationAttempts;
            }
            if (configurationPhase.load() == 1 && shardIndex < 2) {
                throw std::runtime_error(shardIndex == 0
                        ? "configuration-failure-a"
                        : "configuration-failure-b");
            }
            if (configurationPhase.load() == 2 && shardIndex == 1) {
                throw std::runtime_error("configuration-failure-b");
            }
        },
        .Insert = [&] (int shardIndex, bool) {
            ++insertAttempts;
            if (shardIndex == 0 && failWriting.load()) {
                throw std::runtime_error("failure-a");
            }
        },
    };
    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        profiler->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{.ShardIndex = 0}})
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    EXPECT_NE(
        ToString(profiler->GetStatus().Errors.at("/writing")).find("failure-a"),
        std::string::npos);

    configurationPhase = 1;
    const auto insertsBeforeBlockedRequest = insertAttempts.load();
    const auto shard0AttemptsBeforeFailure = shard0ConfigurationAttempts.load();
    const auto shard1AttemptsBeforeFailure = shard1ConfigurationAttempts.load();
    const auto shard2AttemptsBeforeFailure = shard2ConfigurationAttempts.load();
    auto blockedWrite = writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{.ShardIndex = 2}});
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    EXPECT_FALSE(blockedWrite.IsSet());
    EXPECT_GT(shard0ConfigurationAttempts.load(), shard0AttemptsBeforeFailure);
    EXPECT_GT(shard1ConfigurationAttempts.load(), shard1AttemptsBeforeFailure);
    EXPECT_GT(shard2ConfigurationAttempts.load(), shard2AttemptsBeforeFailure);
    EXPECT_EQ(insertAttempts.load(), insertsBeforeBlockedRequest);
    auto errorText = ToString(profiler->GetStatus().Errors.at("/writing"));
    EXPECT_NE(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("configuration-failure-b"), std::string::npos);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);

    configurationPhase = 2;
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    EXPECT_FALSE(blockedWrite.IsSet());
    errorText = ToString(profiler->GetStatus().Errors.at("/writing"));
    EXPECT_EQ(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("configuration-failure-b"), std::string::npos);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);

    configurationPhase = 3;
    EXPECT_TRUE(WaitFor(blockedWrite.WithTimeout(TDuration::Seconds(5))).IsOK());
    errorText = ToString(profiler->GetStatus().Errors.at("/writing"));
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("configuration-failure-b"), std::string::npos);

    failWriting = false;
    EXPECT_TRUE(WaitFor(writer->Write(
        EWriteGuarantee::AtMostOnce,
        {TClickHouseShardWrite{.ShardIndex = 0}})
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, ExactlyOnceGiveUpStopsRemainingShards)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 2);

    std::vector<int> insertedShards;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
        },
        .ApplyConnectionDynamicParameters = [&] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
        },
        .Insert = [&] (int shardIndex, bool /*asyncInsert*/) {
            insertedShards.push_back(shardIndex);
            if (shardIndex == 1) {
                throw std::runtime_error("shard is down");
            }
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    auto writeFuture = writer->Write(EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites());

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    auto result = WaitFor(writeFuture.WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(result.GetMessage(), "Giving up insert into ClickHouse");
    EXPECT_EQ(insertedShards, (std::vector<int>{0, 1, 1}));

    auto rejected = WaitFor(
        writer->Write(EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites())
            .WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(rejected.IsOK());
    EXPECT_EQ(rejected.GetMessage(), "Giving up insert into ClickHouse");
    EXPECT_EQ(insertedShards, (std::vector<int>{0, 1, 1}));

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, AppliesConnectionParametersOncePerRequest)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);

    std::vector<int> configuredShards;
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [&] (int /*shardIndex*/) {
        },
        .ApplyConnectionDynamicParameters = [&] (int shardIndex, const TDynamicCommonClickHouseSinkParametersPtr&) {
            configuredShards.push_back(shardIndex);
        },
        .Insert = [&] (int /*shardIndex*/, bool /*asyncInsert*/) {
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        CreateSyncStatusProfiler()->ErrorState("/writing"),
        NLogging::TLogger("Test"),
        std::move(testHooks));

    auto writeFuture = writer->Write(EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites());

    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writeFuture.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_EQ(configuredShards, (std::vector<int>{0, 1, 2}));

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST(TClickHouseWriterShardTest, HealthyRequestsDoNotRepublishUnchangedErrorState)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto errorState = New<TCountingStatusErrorState>();
    auto owner = New<TWriterOwner>();

    TClickHouseWriterTestHooks testHooks{
        .ResetConnection = [] (int) {
        },
        .ApplyConnectionDynamicParameters = [] (int, const TDynamicCommonClickHouseSinkParametersPtr&) {
        },
        .Insert = [] (int, bool) {
        },
    };

    auto writer = New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        errorState,
        NLogging::TLogger("Test"),
        std::move(testHooks));
    auto actionQueue = New<TActionQueue>("ClickHouseWriterTest");
    auto runFuture = BIND(&TClickHouseWriter::Run, writer, MakeWeak(owner))
        .AsyncVia(actionQueue->GetInvoker())
        .Run();

    EXPECT_TRUE(WaitFor(writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites())
            .WithTimeout(TDuration::Seconds(5)))
            .IsOK());
    EXPECT_EQ(errorState->SetErrorCount.load(), 0);
    EXPECT_EQ(errorState->ClearErrorCount.load(), 0);

    owner.Reset();
    (void)writer->Write(EWriteGuarantee::AtMostOnce, MakeThreeShardWrites());
    WaitFor(runFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseBlockBuilder MakeBatchPreparationBlockBuilder(const TTableSchemaPtr& schema)
{
    std::vector<TClickHouseTableColumn> columns{
        {.Name = "data", .Type = "Int64"},
    };
    return TClickHouseBlockBuilder(ResolveColumns(columns, schema));
}

class TBatchPreparationFixture
{
public:
    TBatchPreparationFixture()
        : Schema(New<TTableSchema>(std::vector{
              TColumnSchema("user_id", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
              TColumnSchema("data", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
          }))
        , Storage([&] {
            auto streamSpec = New<TStreamSpec>();
            streamSpec->Schema = Schema;
            THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
            specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
            return New<TComputationStreamSpecStorage>(
                New<TStreamSpecs>(std::move(specs)),
                New<TTableSchema>(),
                nullptr);
        }())
        , BlockBuilder(MakeBatchPreparationBlockBuilder(Schema))
    { }

    TOutputMessageConstPtr MakeMessage(i64 id, i64 userId) const
    {
        TMessageBuilder builder("test", Schema);
        builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
        builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
        builder.SetEventTimestamp(TSystemTimestamp(1700000000));
        builder.Payload().Set<i64>(userId, "user_id");
        builder.Payload().Set<i64>(id, "data");
        return New<TOutputMessage>(builder.Finish(), Storage);
    }

    const TTableSchemaPtr Schema;
    const TComputationStreamSpecStoragePtr Storage;
    const TClickHouseBlockBuilder BlockBuilder;
};

std::vector<i64> ReadBlockData(const clickhouse::Block& block)
{
    std::vector<i64> result;
    auto column = block[0]->As<clickhouse::ColumnInt64>();
    result.reserve(column->Size());
    for (size_t index = 0; index < column->Size(); ++index) {
        result.push_back(column->At(index));
    }
    return result;
}

TEST(TClickHouseShardWriteBuilderTest, EmptyInputYieldsNoWrites)
{
    TBatchPreparationFixture fixture;
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "host";
    TClickHouseShardRouter router(ResolveShards(*parameters), {"user_id"}, {fixture.Schema});

    EXPECT_TRUE(BuildClickHouseShardWrites(router, fixture.BlockBuilder, "batch", {}).empty());
}

TEST(TClickHouseShardWriteBuilderTest, UnshardedInputPreservesOrderAndBareToken)
{
    TBatchPreparationFixture fixture;
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "host";
    TClickHouseShardRouter router(ResolveShards(*parameters), {"user_id"}, {fixture.Schema});
    std::vector<TOutputMessageConstPtr> messages{
        fixture.MakeMessage(4, 1),
        fixture.MakeMessage(1, 2),
        fixture.MakeMessage(9, 3),
    };

    auto writes = BuildClickHouseShardWrites(router, fixture.BlockBuilder, "whole-batch", messages);

    ASSERT_EQ(writes.size(), 1u);
    EXPECT_EQ(writes[0].ShardIndex, 0);
    EXPECT_EQ(writes[0].DedupToken, std::optional<std::string>("whole-batch"));
    EXPECT_EQ(ReadBlockData(writes[0].Block), (std::vector<i64>{4, 1, 9}));
}

TEST(TClickHouseShardWriteBuilderTest, SingleNamedShardUsesSuffixedToken)
{
    TBatchPreparationFixture fixture;
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {{"named", {"host"}}};
    TClickHouseShardRouter router(ResolveShards(*parameters), {"user_id"}, {fixture.Schema});
    std::vector<TOutputMessageConstPtr> messages{
        fixture.MakeMessage(2, 1),
        fixture.MakeMessage(3, 2),
    };

    auto writes = BuildClickHouseShardWrites(router, fixture.BlockBuilder, "whole-batch", messages);

    ASSERT_EQ(writes.size(), 1u);
    EXPECT_EQ(writes[0].ShardIndex, 0);
    EXPECT_EQ(writes[0].DedupToken, std::optional<std::string>("whole-batch:named"));
    EXPECT_EQ(ReadBlockData(writes[0].Block), (std::vector<i64>{2, 3}));
}

TEST(TClickHouseShardWriteBuilderTest, ShardedInputPreservesPerShardOrderAndWholeBatchToken)
{
    TBatchPreparationFixture fixture;
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a-host"}},
        {"b", {"b-host"}},
        {"c", {"c-host"}},
    };
    TClickHouseShardRouter router(ResolveShards(*parameters), {"user_id"}, {fixture.Schema});
    std::vector<TOutputMessageConstPtr> messages;
    std::vector<std::vector<i64>> expected(router.GetShards().size());
    for (i64 id = 0; id < 100 && messages.size() < 8; ++id) {
        auto message = fixture.MakeMessage(id, id);
        const int shardIndex = router.SelectShard(message);
        if (shardIndex == 1 || expected[shardIndex].size() == 4) {
            continue;
        }
        messages.push_back(message);
        expected[shardIndex].push_back(id);
    }
    ASSERT_EQ(messages.size(), 8u);

    auto writes = BuildClickHouseShardWrites(router, fixture.BlockBuilder, "whole-batch", messages);

    ASSERT_EQ(writes.size(), 2u);
    for (const auto& write : writes) {
        const auto& shard = router.GetShards()[write.ShardIndex];
        EXPECT_FALSE(expected[write.ShardIndex].empty());
        EXPECT_EQ(write.DedupToken, std::optional("whole-batch:" + shard.Name));
        EXPECT_EQ(ReadBlockData(write.Block), expected[write.ShardIndex]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseClientOptionsTest, SingleHostFormHasNoEndpoints)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "primary.clickhouse";
    parameters->Port = 9001;

    auto options = MakeClientOptions(
        *parameters,
        ResolveShards(*parameters).front(),
        TDuration::Seconds(60));

    EXPECT_EQ(options.host, "primary.clickhouse");
    EXPECT_EQ(options.port, 9001);
    EXPECT_TRUE(options.endpoints.empty());
}

TEST(TClickHouseHostSelectionPolicyTest, OrderedAndRotatedOrdersAreExact)
{
    const std::vector<std::string> hosts{"a", "b", "c"};
    EXPECT_EQ(OrderHostsForClient(hosts, EClickHouseHostSelectionPolicy::OrderedRoundRobin, 2), hosts);
    EXPECT_EQ(OrderHostsForClient(hosts, EClickHouseHostSelectionPolicy::RandomStart, 0), hosts);
    EXPECT_EQ(OrderHostsForClient(hosts, EClickHouseHostSelectionPolicy::RandomStart, 1),
        (std::vector<std::string>{"b", "c", "a"}));
    EXPECT_EQ(OrderHostsForClient(hosts, EClickHouseHostSelectionPolicy::RandomStart, 2),
        (std::vector<std::string>{"c", "a", "b"}));
    EXPECT_EQ(OrderHostsForClient({"a"}, EClickHouseHostSelectionPolicy::RandomStart, 0),
        (std::vector<std::string>{"a"}));
}

TEST(TClickHouseClientOptionsTest, TrailingHostsBecomeEndpoints)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Hosts = {"primary.clickhouse", "first.clickhouse", "second.clickhouse"};
    parameters->Port = 9001;

    auto options = MakeClientOptions(
        *parameters,
        ResolveShards(*parameters).front(),
        TDuration::Seconds(60));

    EXPECT_EQ(options.host, "primary.clickhouse");
    EXPECT_EQ(options.port, 9001);
    EXPECT_EQ(
        options.endpoints,
        (std::vector<clickhouse::Endpoint>{
            {.host = "first.clickhouse", .port = 9001},
            {.host = "second.clickhouse", .port = 9001},
        }));
}

TEST(TClickHouseClientOptionsTest, RandomStartRotatesPrimaryHostAndEndpoints)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Hosts = {"a", "b", "c"};
    parameters->HostSelectionPolicy = EClickHouseHostSelectionPolicy::RandomStart;
    parameters->Port = 9001;

    auto options = MakeClientOptions(
        *parameters,
        ResolveShards(*parameters).front(),
        TDuration::Seconds(60),
        /*startOffset*/ 1);

    EXPECT_EQ(options.host, "b");
    EXPECT_EQ(
        options.endpoints,
        (std::vector<clickhouse::Endpoint>{
            {.host = "c", .port = 9001},
            {.host = "a", .port = 9001},
        }));
}

TEST(TClickHouseClientOptionsTest, SingleHostShardHasNoEndpoints)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {{"a", {"ch-a-1"}}};
    parameters->Port = 9002;
    parameters->Database = "db";

    auto options = MakeClientOptions(
        *parameters,
        ResolveShards(*parameters).front(),
        TDuration::Seconds(60));

    EXPECT_EQ(options.host, "ch-a-1");
    EXPECT_EQ(options.port, 9002);
    EXPECT_TRUE(options.endpoints.empty());
    EXPECT_EQ(options.default_database, "db");
}

TEST(TClickHouseClientOptionsTest, ShardReplicasBecomeEndpointsInOrder)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"ch-a-1", "ch-a-2", "ch-a-3"}},
        {"b", {"ch-b-1"}},
    };
    parameters->Port = 9002;
    parameters->Database = "db";

    auto shards = ResolveShards(*parameters);
    ASSERT_EQ(std::ssize(shards), 2);

    auto options = MakeClientOptions(*parameters, shards[0], TDuration::Seconds(60));
    EXPECT_EQ(options.host, "ch-a-1");
    EXPECT_EQ(options.port, 9002);
    EXPECT_EQ(
        options.endpoints,
        (std::vector<clickhouse::Endpoint>{
            {.host = "ch-a-2", .port = 9002},
            {.host = "ch-a-3", .port = 9002},
        }));

    auto otherOptions = MakeClientOptions(*parameters, shards[1], TDuration::Seconds(60));
    EXPECT_EQ(otherOptions.host, "ch-b-1");
    EXPECT_TRUE(otherOptions.endpoints.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
