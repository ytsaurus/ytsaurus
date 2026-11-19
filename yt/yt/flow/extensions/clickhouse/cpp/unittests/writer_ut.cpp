#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/columns/numeric.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/exceptions.h>

#include <atomic>
#include <mutex>
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

TTableSchemaPtr MakeMetadataTestSchema()
{
    return New<TTableSchema>(std::vector{
        TColumnSchema("id", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    });
}

std::system_error MakeRefusal(const std::string& what = "refused")
{
    return std::system_error(std::make_error_code(std::errc::connection_refused), what);
}

std::system_error MakeConnectionReset()
{
    return std::system_error(std::make_error_code(std::errc::connection_reset));
}

clickhouse::ServerException MakeServerException(int code)
{
    return clickhouse::ServerException(std::make_shared<clickhouse::Exception>(
        clickhouse::Exception{.code = code, .display_text = Format("server error %v", code)}));
}

struct TFakeOperation
{
    std::string Kind;
    int ShardIndex = 0;
    std::string Host;
    EClickHouseConnectionPurpose Purpose = EClickHouseConnectionPurpose::Write;
    int Generation = 0;
    TDuration SocketTimeout;
    std::optional<std::string> DedupToken;
    bool AsyncInsert = false;
};

class TFakeClickHouseReplicas
    : public std::enable_shared_from_this<TFakeClickHouseReplicas>
{
public:
    std::function<void(const TFakeOperation&)> Fail;
    std::function<TClickHouseTargetMetadata(const TFakeOperation&)> Metadata;
    std::function<int(int hostCount)> SelectRandomStartOffset;

    TClickHouseWriterTestHooks MakeHooks();

    void Record(const TFakeOperation& operation)
    {
        {
            auto guard = std::lock_guard(Lock_);
            Operations_.push_back(operation);
        }
        if (Fail) {
            Fail(operation);
        }
    }

    TClickHouseTargetMetadata GetMetadata(const TFakeOperation& operation) const
    {
        return Metadata ? Metadata(operation) : MakeReplicatedMetadata(operation.Host);
    }

    std::vector<TFakeOperation> GetOperations(EClickHouseConnectionPurpose purpose) const
    {
        auto guard = std::lock_guard(Lock_);
        std::vector<TFakeOperation> result;
        for (const auto& operation : Operations_) {
            if (operation.Purpose == purpose) {
                result.push_back(operation);
            }
        }
        return result;
    }

    std::vector<std::string> Describe(EClickHouseConnectionPurpose purpose = EClickHouseConnectionPurpose::Write) const
    {
        std::vector<std::string> result;
        for (const auto& operation : GetOperations(purpose)) {
            result.push_back(operation.Kind + " " + operation.Host);
        }
        return result;
    }

    int Count(const std::string& kind, EClickHouseConnectionPurpose purpose = EClickHouseConnectionPurpose::Write) const
    {
        return std::ranges::count_if(GetOperations(purpose), [&] (const auto& operation) {
            return operation.Kind == kind;
        });
    }

    void ClearOperations()
    {
        auto guard = std::lock_guard(Lock_);
        Operations_.clear();
    }

private:
    mutable std::mutex Lock_;
    std::vector<TFakeOperation> Operations_;
    std::atomic<int> LastGeneration_ = 0;

    friend class TFakeClickHouseConnection;
};

class TFakeClickHouseConnection
    : public IClickHouseConnection
{
public:
    TFakeClickHouseConnection(std::shared_ptr<TFakeClickHouseReplicas> replicas, TFakeOperation identity)
        : Replicas_(std::move(replicas))
        , Identity_(std::move(identity))
    { }

    TClickHouseTargetMetadata QueryTargetMetadata() override
    {
        auto operation = MakeOperation("metadata");
        Replicas_->Record(operation);
        return Replicas_->GetMetadata(operation);
    }

    std::optional<TDuration> QueryDedupWindow(bool /*asyncInsert*/) override
    {
        Replicas_->Record(MakeOperation("dedup"));
        return TDuration::Days(7);
    }

    void Insert(const TClickHouseShardWrite& shardWrite, bool asyncInsert, bool /*useInputFunction*/) override
    {
        auto operation = MakeOperation("insert");
        operation.DedupToken = shardWrite.DedupToken;
        operation.AsyncInsert = asyncInsert;
        Replicas_->Record(operation);
    }

private:
    const std::shared_ptr<TFakeClickHouseReplicas> Replicas_;
    const TFakeOperation Identity_;

    TFakeOperation MakeOperation(std::string kind) const
    {
        auto operation = Identity_;
        operation.Kind = std::move(kind);
        return operation;
    }
};

TClickHouseWriterTestHooks TFakeClickHouseReplicas::MakeHooks()
{
    auto self = shared_from_this();
    return {
        .CreateConnection = [self] (
            int shardIndex,
            const std::string& host,
            EClickHouseConnectionPurpose purpose,
            TDuration socketTimeout) -> std::unique_ptr<IClickHouseConnection> {
            TFakeOperation operation{
                .Kind = "create",
                .ShardIndex = shardIndex,
                .Host = host,
                .Purpose = purpose,
                .Generation = ++self->LastGeneration_,
                .SocketTimeout = socketTimeout,
            };
            self->Record(operation);
            return std::make_unique<TFakeClickHouseConnection>(self, operation);
        },
        .SelectRandomStartOffset = [self] (int hostCount) {
            return self->SelectRandomStartOffset(hostCount);
        },
    };
}

std::shared_ptr<TFakeClickHouseReplicas> MakeReplicas()
{
    return std::make_shared<TFakeClickHouseReplicas>();
}

TClickHouseWriterPtr MakeFakeWriter(
    const TCommonClickHouseSinkParametersPtr& parameters,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters,
    const std::shared_ptr<TFakeClickHouseReplicas>& replicas,
    IStatusErrorStatePtr errorState = CreateSyncStatusProfiler()->ErrorState("/writing"))
{
    return New<TClickHouseWriter>(
        parameters,
        dynamicParameters,
        ResolveShards(*parameters),
        std::move(errorState),
        NLogging::TLogger("Test"),
        replicas->MakeHooks());
}

IInvokerPtr GetWriterRunInvoker()
{
    static const auto queue = New<TActionQueue>("ClickHouseWriterTest");
    return queue->GetInvoker();
}

class TRunningWriter
{
public:
    explicit TRunningWriter(TClickHouseWriterPtr writer)
        : Writer_(std::move(writer))
        , Owner_(New<TWriterOwner>())
        , RunFuture_(BIND(&TClickHouseWriter::Run, Writer_, MakeWeak(Owner_))
                .AsyncVia(GetWriterRunInvoker())
                .Run())
    { }

    ~TRunningWriter()
    {
        RequestStop();
    }

    TError Stop(TDuration timeout = TDuration::Seconds(5))
    {
        RequestStop();
        return WaitFor(RunFuture_.ToUncancelable().WithTimeout(timeout));
    }

    TFuture<void> GetRunFuture() const
    {
        return RunFuture_;
    }

private:
    const TClickHouseWriterPtr Writer_;
    TIntrusivePtr<TWriterOwner> Owner_;
    const TFuture<void> RunFuture_;

    void RequestStop()
    {
        if (!Owner_) {
            return;
        }

        Owner_.Reset();
        YT_UNUSED_FUTURE(Writer_->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}}));
    }
};

TError WriteAndWait(
    const TClickHouseWriterPtr& writer,
    EWriteGuarantee guarantee,
    std::vector<TClickHouseShardWrite> shardWrites = {TClickHouseShardWrite{}})
{
    return WaitFor(writer->Write(guarantee, std::move(shardWrites)).WithTimeout(TDuration::Seconds(5)));
}

template <class TPredicate>
bool WaitUntil(TPredicate predicate)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(5);
    while (!predicate()) {
        if (TInstant::Now() > deadline) {
            return false;
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
    }
    return true;
}

std::string GetWritingError(const IStatusProfilerPtr& profiler)
{
    const auto errors = profiler->GetStatus().Errors;
    auto it = errors.find("/writing");
    return it == errors.end() ? std::string() : std::string(ToString(it->second));
}

TCommonClickHouseSinkParametersPtr MakeHostsParameters(std::vector<std::string> hosts)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Hosts = std::move(hosts);
    parameters->Table = "events";
    return parameters;
}

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
    EXPECT_NO_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", first, nullptr, logger));
    EXPECT_NO_THROW(ValidateClickHouseEndpointMetadata(shard, "a2", second, &first, logger));
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
        EXPECT_THROW(ValidateClickHouseEndpointMetadata(shard, "a2", changed, &first, logger), TErrorException);
    }
    auto incomplete = first;
    incomplete.ReplicaName.clear();
    EXPECT_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", incomplete, nullptr, logger), TErrorException);

    auto shared = first;
    shared.Engine = "SharedMergeTree";
    shared.ReplicationIdentity.reset();
    EXPECT_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", shared, nullptr, logger), TErrorException);
    auto plain = shared;
    plain.Engine = "MergeTree";
    EXPECT_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", plain, nullptr, logger), TErrorException);
    shard.Hosts.resize(1);
    EXPECT_NO_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", shared, nullptr, logger));
    EXPECT_NO_THROW(ValidateClickHouseEndpointMetadata(shard, "a1", plain, nullptr, logger));
}

////////////////////////////////////////////////////////////////////////////////
class TClickHouseDegradedStartupTest
    : public testing::TestWithParam<int>
{ };

TEST_P(TClickHouseDegradedStartupTest, UnavailableReplicaDoesNotBlockStartupOrWrites)
{
    const std::vector<std::string> hosts{"a1", "a2"};
    const auto& deadHost = hosts[GetParam()];
    const auto& liveHost = hosts[1 - GetParam()];
    auto replicas = MakeReplicas();
    replicas->Fail = [deadHost] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && operation.Host == deadHost) {
            throw MakeRefusal();
        }
    };
    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(MakeHostsParameters(hosts), MakeShardTestDynamicParameters(3), replicas, profiler->ErrorState("/writing"));

    EXPECT_NO_THROW(writer->Connect(MakeMetadataTestSchema()));
    EXPECT_EQ(
        replicas->Describe(EClickHouseConnectionPurpose::Probe),
        deadHost == "a1"
            ? (std::vector<std::string>{"create a1", "create a2", "metadata a2"})
            : (std::vector<std::string>{"create a1", "metadata a1", "create a2"}));
    EXPECT_EQ(
        writer->GetShardTargetIdentityFingerprints(),
        std::vector{BuildShardTargetIdentityFingerprint(
            writer->GetShards().front(),
            TClickHouseReplicationIdentity{"default", "/tables/events"})});
    EXPECT_TRUE(GetWritingError(profiler).empty());

    TRunningWriter running(writer);
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::ExactlyOnce).IsOK());
    std::vector<std::string> expected;
    if (deadHost == "a1") {
        expected.push_back("create a1");
    }
    expected.insert(expected.end(), {"create " + liveHost, "metadata " + liveHost, "dedup " + liveHost, "insert " + liveHost});
    EXPECT_EQ(replicas->Describe(), expected);
    EXPECT_TRUE(GetWritingError(profiler).empty());

    EXPECT_TRUE(running.Stop().IsOK());
}

INSTANTIATE_TEST_SUITE_P(
    DeadReplica,
    TClickHouseDegradedStartupTest,
    testing::Values(0, 1));

TEST(TClickHouseWriterStartupTest, StartupRetryAfterExhaustedShardClearsErrors)
{
    auto replicas = MakeReplicas();
    std::atomic<bool> secondReplicaUp = false;
    replicas->Fail = [&] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && (operation.Host == "a1" || !secondReplicaUp.load())) {
            throw MakeRefusal();
        }
    };
    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), MakeShardTestDynamicParameters(3), replicas, profiler->ErrorState("/writing"));
    const auto schema = MakeMetadataTestSchema();

    try {
        writer->Connect(schema);
        ADD_FAILURE() << "Connect must fail without an available replica";
    } catch (const TClickHouseAdmissionException& ex) {
        EXPECT_EQ(ex.GetKind(), EClickHouseAdmissionFailureKind::Unavailable);
        EXPECT_THAT(ToString(ex.Error()), testing::HasSubstr("No available ClickHouse replica"));
    }
    EXPECT_THAT(GetWritingError(profiler), testing::HasSubstr("No available ClickHouse replica"));
    EXPECT_TRUE(replicas->Describe().empty());

    secondReplicaUp = true;
    EXPECT_NO_THROW(writer->Connect(schema));
    EXPECT_EQ(replicas->Count("create", EClickHouseConnectionPurpose::Probe), 4);
    EXPECT_EQ(replicas->Describe(), (std::vector<std::string>{"create a1", "create a2", "metadata a2", "dedup a2"}));
    EXPECT_TRUE(GetWritingError(profiler).empty());
}

TEST(TClickHouseWriterStartupTest, EveryShardStartsWithItsRemainingReplica)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a1", "a2"}},
        {"b", {"b1", "b2"}},
    };
    parameters->Table = "events";
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && (operation.Host == "a1" || operation.Host == "b2")) {
            throw MakeRefusal();
        }
    };
    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(3), replicas, profiler->ErrorState("/writing"));
    EXPECT_NO_THROW(writer->Connect(MakeMetadataTestSchema()));
    TRunningWriter running(writer);
    EXPECT_TRUE(WriteAndWait(
        writer,
        EWriteGuarantee::ExactlyOnce,
        {TClickHouseShardWrite{.ShardIndex = 0}, TClickHouseShardWrite{.ShardIndex = 1}})
            .IsOK());
    std::vector<std::string> inserts;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "insert") {
            inserts.push_back(operation.Host);
        }
    }
    EXPECT_EQ(inserts, (std::vector<std::string>{"a2", "b1"}));
    EXPECT_TRUE(GetWritingError(profiler).empty());

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterStartupTest, WhollyUnavailableShardPublishesNothing)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a1"}},
        {"b", {"b1", "b2"}},
    };
    parameters->Table = "events";
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && operation.Host.starts_with("b")) {
            throw MakeRefusal(operation.Host + "-down");
        }
    };
    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(3), replicas, profiler->ErrorState("/writing"));
    EXPECT_THROW(writer->Connect(MakeMetadataTestSchema()), TClickHouseAdmissionException);
    EXPECT_TRUE(replicas->Describe().empty());
    const auto error = GetWritingError(profiler);
    EXPECT_THAT(error, testing::HasSubstr("No available ClickHouse replica in shard \"b\""));
    EXPECT_THAT(error, testing::HasSubstr("b1-down"));
    EXPECT_THAT(error, testing::HasSubstr("b2-down"));
}

TEST(TClickHouseWriterStartupTest, IndeterminateProbeRejectsStartupDespiteHealthyPeer)
{
    for (bool serverError : {false, true}) {
        SCOPED_TRACE(serverError);
        auto replicas = MakeReplicas();
        replicas->Fail = [&] (const TFakeOperation& operation) {
            if (operation.Kind == "metadata" && operation.Host == "a1") {
                if (serverError) {
                    throw MakeServerException(999);
                }
                throw clickhouse::ProtocolError("can't read data packet from input stream");
            }
        };
        auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), MakeShardTestDynamicParameters(3), replicas);
        try {
            writer->Connect(MakeMetadataTestSchema());
            ADD_FAILURE() << "Connect must reject an indeterminate probe";
        } catch (const TClickHouseAdmissionException& ex) {
            EXPECT_EQ(ex.GetKind(), EClickHouseAdmissionFailureKind::Indeterminate);
        }
        EXPECT_TRUE(replicas->Describe().empty());
    }
}

class TClickHouseMixedOutageTest
    : public testing::TestWithParam<bool>
{ };

TEST_P(TClickHouseMixedOutageTest, UnavailableSiblingDoesNotHideIncompatiblePeer)
{
    const bool deadFirst = GetParam();
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && operation.Host == "dead") {
            throw MakeRefusal();
        }
    };
    replicas->Metadata = [] (const TFakeOperation& operation) {
        auto metadata = MakeReplicatedMetadata(operation.Host);
        if (operation.Host == "other") {
            metadata.ReplicationIdentity->ZookeeperPath = "/tables/other";
        }
        return metadata;
    };
    auto writer = MakeFakeWriter(
        MakeHostsParameters(deadFirst
                ? std::vector<std::string>{"dead", "good", "other"}
                : std::vector<std::string>{"other", "good", "dead"}),
        MakeShardTestDynamicParameters(3),
        replicas);
    try {
        writer->Connect(MakeMetadataTestSchema());
        ADD_FAILURE() << "Connect must reject an incompatible peer";
    } catch (const TClickHouseAdmissionException& ex) {
        EXPECT_EQ(ex.GetKind(), EClickHouseAdmissionFailureKind::Incompatible);
        EXPECT_THAT(ToString(ex.Error()), testing::HasSubstr("different replicated table"));
    }
    EXPECT_TRUE(replicas->Describe().empty());
}

INSTANTIATE_TEST_SUITE_P(
    Order,
    TClickHouseMixedOutageTest,
    testing::Values(true, false));

TEST(TClickHouseWriterStartupTest, ConfiguredFailoverStillRequiresReplicatedEngine)
{
    for (const auto& engine : {"MergeTree", "SharedMergeTree"}) {
        SCOPED_TRACE(engine);
        auto replicas = MakeReplicas();
        replicas->Fail = [] (const TFakeOperation& operation) {
            if (operation.Kind == "create" && operation.Host == "a1") {
                throw MakeRefusal();
            }
        };
        replicas->Metadata = [&] (const TFakeOperation& operation) {
            auto metadata = MakeReplicatedMetadata(operation.Host);
            metadata.Engine = engine;
            metadata.ReplicationIdentity.reset();
            return metadata;
        };
        auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), MakeShardTestDynamicParameters(3), replicas);
        EXPECT_THROW(writer->Connect(MakeMetadataTestSchema()), TClickHouseAdmissionException);
        EXPECT_TRUE(replicas->Describe().empty());
    }
}

TEST(TClickHouseWriterStartupTest, CrossShardMismatchIsRejectedDespiteUnavailableSibling)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a1", "a2"}},
        {"b", {"b1"}},
    };
    parameters->Table = "events";
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && operation.Host == "a1") {
            throw MakeRefusal();
        }
    };
    replicas->Metadata = [] (const TFakeOperation& operation) {
        auto metadata = MakeReplicatedMetadata(operation.Host);
        if (operation.Host == "b1") {
            metadata.Columns[1].DefaultExpression = "'other'";
        }
        return metadata;
    };
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(3), replicas);
    try {
        writer->Connect(MakeMetadataTestSchema());
        ADD_FAILURE() << "Connect must reject different shard schemas";
    } catch (const TClickHouseAdmissionException& ex) {
        EXPECT_EQ(ex.GetKind(), EClickHouseAdmissionFailureKind::Incompatible);
        EXPECT_THAT(ToString(ex.Error()), testing::HasSubstr("different ordered schemas"));
    }
    EXPECT_TRUE(replicas->Describe().empty());
}

TEST(TClickHouseWriterMetadataTest, ProbesEveryEndpointOnceAndAdmitsEveryGeneration)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"a1", "a2"}},
        {"b", {"b1", "b2"}},
    };
    parameters->Table = "events";
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);

    auto replicas = MakeReplicas();
    std::atomic<bool> mismatchLaterEndpoint = true;
    std::atomic<bool> failReplacementMetadata = false;
    replicas->Fail = [&] (const TFakeOperation& operation) {
        if (failReplacementMetadata.load() &&
            operation.Kind == "metadata" &&
            operation.Purpose == EClickHouseConnectionPurpose::Write &&
            operation.Host == "b1")
        {
            throw clickhouse::ProtocolError("truncated metadata");
        }
    };
    replicas->Metadata = [&] (const TFakeOperation& operation) {
        auto metadata = MakeReplicatedMetadata(operation.Host);
        metadata.ReplicationIdentity = TClickHouseReplicationIdentity{
            Format("keeper-%v", operation.ShardIndex),
            Format("/tables/events-%v", operation.ShardIndex),
        };
        if (mismatchLaterEndpoint.load() && operation.Host == "b2") {
            metadata.Columns[1].Type = "Int64";
        }
        return metadata;
    };

    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas, profiler->ErrorState("/writing"));
    const auto schema = MakeMetadataTestSchema();
    const std::vector<std::string> fullProbe{
        "create a1",
        "metadata a1",
        "create a2",
        "metadata a2",
        "create b1",
        "metadata b1",
        "create b2",
        "metadata b2",
    };
    EXPECT_THROW(writer->Connect(schema), std::exception);
    EXPECT_EQ(replicas->Describe(EClickHouseConnectionPurpose::Probe), fullProbe);
    EXPECT_TRUE(replicas->Describe().empty());

    mismatchLaterEndpoint = false;
    replicas->ClearOperations();
    const auto columns = writer->Connect(schema);
    ASSERT_EQ(columns.size(), 1u);
    EXPECT_EQ(columns.front().Name, "id");
    EXPECT_EQ(replicas->Describe(EClickHouseConnectionPurpose::Probe), fullProbe);
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{"create a1", "metadata a1", "dedup a1", "create b1", "metadata b1", "dedup b1"}));

    auto changedDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    changedDynamicParameters->WriteTimeout = TDuration::Seconds(61);
    writer->Reconfigure(changedDynamicParameters);
    replicas->ClearOperations();
    EXPECT_NO_THROW(writer->Connect(schema));
    EXPECT_TRUE(replicas->Describe(EClickHouseConnectionPurpose::Probe).empty());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{"create a1", "metadata a1", "dedup a1", "create b1", "metadata b1", "dedup b1"}));
    EXPECT_TRUE(GetWritingError(profiler).empty());

    auto thirdDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    thirdDynamicParameters->WriteTimeout = TDuration::Seconds(62);
    writer->Reconfigure(thirdDynamicParameters);
    failReplacementMetadata = true;
    try {
        writer->Connect(schema);
        ADD_FAILURE() << "Connect must reject an indeterminate replacement";
    } catch (const TClickHouseAdmissionException& ex) {
        EXPECT_EQ(ex.GetKind(), EClickHouseAdmissionFailureKind::Indeterminate);
    }
    EXPECT_THAT(GetWritingError(profiler), testing::HasSubstr("truncated metadata"));
}

TEST(TClickHouseWriterMetadataTest, ConfigurationRetryReusesReferencesButAdmitsEveryReplacement)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto profiler = CreateSyncStatusProfiler();
    auto replicas = MakeReplicas();
    std::atomic<bool> failShardB = true;
    replicas->Fail = [&] (const TFakeOperation& operation) {
        if (failShardB.load() &&
            operation.Purpose == EClickHouseConnectionPurpose::Write &&
            operation.Kind == "metadata" &&
            operation.ShardIndex == 1)
        {
            throw clickhouse::ProtocolError("configuration-failure-b");
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas, profiler->ErrorState("/writing"));
    const auto schema = MakeMetadataTestSchema();

    EXPECT_THROW(writer->Connect(schema), TClickHouseAdmissionException);
    EXPECT_THAT(GetWritingError(profiler), testing::HasSubstr("configuration-failure-b"));
    EXPECT_EQ(replicas->Count("metadata", EClickHouseConnectionPurpose::Probe), 3);
    EXPECT_EQ(replicas->Count("metadata"), 3);

    failShardB = false;
    EXPECT_NO_THROW(writer->Connect(schema));
    EXPECT_EQ(replicas->Count("metadata", EClickHouseConnectionPurpose::Probe), 3);
    EXPECT_EQ(replicas->Count("metadata"), 6);
    EXPECT_TRUE(GetWritingError(profiler).empty());
}

TEST(TClickHouseWriterMetadataTest, WriteTimeoutRecreationAdmitsReplacementConnections)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "host";
    parameters->Table = "events";
    auto updatedDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);

    auto replicas = MakeReplicas();
    std::atomic<bool> refuseNextCreate = false;
    replicas->Fail = [&] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && refuseNextCreate.exchange(false)) {
            throw MakeRefusal();
        }
    };
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(3), replicas);
    const auto schema = MakeMetadataTestSchema();

    EXPECT_NO_THROW(writer->Connect(schema));
    EXPECT_EQ(replicas->Count("create", EClickHouseConnectionPurpose::Probe), 1);

    writer->Reconfigure(updatedDynamicParameters);
    refuseNextCreate = true;
    EXPECT_THROW(writer->Connect(schema), TClickHouseAdmissionException);
    EXPECT_NO_THROW(writer->Connect(schema));
    EXPECT_EQ(replicas->Count("create", EClickHouseConnectionPurpose::Probe), 1);

    std::vector<std::pair<std::string, TDuration>> writeOperations;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        writeOperations.emplace_back(operation.Kind, operation.SocketTimeout);
    }
    EXPECT_EQ(
        writeOperations,
        (std::vector<std::pair<std::string, TDuration>>{
            {"create", TDuration::Seconds(60)},
            {"metadata", TDuration::Seconds(60)},
            {"dedup", TDuration::Seconds(60)},
            {"create", TDuration::Seconds(61)},
            {"create", TDuration::Seconds(61)},
            {"metadata", TDuration::Seconds(61)},
            {"dedup", TDuration::Seconds(61)},
        }));
}

class TClickHouseRestoredReplicaTest
    : public testing::TestWithParam<EWriteGuarantee>
{ };

TEST_P(TClickHouseRestoredReplicaTest, RestoredReplicaIsAdmittedBeforeUse)
{
    for (bool compatible : {true, false}) {
        SCOPED_TRACE(compatible);
        auto replicas = MakeReplicas();
        auto firstReplicaUp = std::make_shared<std::atomic<bool>>(false);
        auto failSecondReplicaInsert = std::make_shared<std::atomic<bool>>(false);
        replicas->Fail = [firstReplicaUp, failSecondReplicaInsert] (const TFakeOperation& operation) {
            if (operation.Kind == "create" && operation.Host == "a1" && !firstReplicaUp->load()) {
                throw MakeRefusal();
            }
            if (operation.Kind == "insert" && operation.Host == "a2" && failSecondReplicaInsert->exchange(false)) {
                throw MakeConnectionReset();
            }
        };
        replicas->Metadata = [compatible] (const TFakeOperation& operation) {
            auto metadata = MakeReplicatedMetadata(operation.Host);
            if (!compatible && operation.Host == "a1") {
                metadata.ReplicationIdentity->ZookeeperPath = "/tables/restored-elsewhere";
            }
            return metadata;
        };
        auto profiler = CreateSyncStatusProfiler();
        auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), MakeShardTestDynamicParameters(3), replicas, profiler->ErrorState("/writing"));
        writer->Connect(MakeMetadataTestSchema());
        TRunningWriter running(writer);
        EXPECT_TRUE(WriteAndWait(writer, GetParam(), {TClickHouseShardWrite{.DedupToken = "first"}}).IsOK());

        *firstReplicaUp = true;
        *failSecondReplicaInsert = true;
        replicas->ClearOperations();
        auto result = WriteAndWait(writer, GetParam(), {TClickHouseShardWrite{.DedupToken = "second"}});
        if (compatible) {
            EXPECT_TRUE(result.IsOK());
            EXPECT_EQ(
                replicas->Describe(),
                (std::vector<std::string>{"insert a2", "create a1", "metadata a1", "dedup a1", "insert a1"}));
            for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
                if (operation.Kind == "insert") {
                    EXPECT_EQ(operation.DedupToken, std::optional<std::string>("second"));
                }
            }
            EXPECT_TRUE(GetWritingError(profiler).empty());
        } else {
            EXPECT_FALSE(result.IsOK());
            EXPECT_EQ(result.GetMessage(), "ClickHouse replica is incompatible with the target table");
            EXPECT_EQ(replicas->Describe(), (std::vector<std::string>{"insert a2", "create a1", "metadata a1"}));
            EXPECT_THAT(GetWritingError(profiler), testing::HasSubstr("different replicated table"));

            replicas->ClearOperations();
            auto rejected = WriteAndWait(writer, GetParam());
            EXPECT_EQ(rejected.GetMessage(), "ClickHouse replica is incompatible with the target table");
            EXPECT_TRUE(replicas->Describe().empty());
        }

        EXPECT_TRUE(running.Stop().IsOK());
    }
}

INSTANTIATE_TEST_SUITE_P(
    Guarantees,
    TClickHouseRestoredReplicaTest,
    testing::Values(EWriteGuarantee::ExactlyOnce, EWriteGuarantee::AtLeastOnce));

TEST(TClickHouseWriterAdmissionTest, ExhaustedReplicasRetryWithoutBudgetUntilRecovery)
{
    auto replicas = MakeReplicas();
    auto replicasDown = std::make_shared<std::atomic<bool>>(false);
    auto failFirstInsert = std::make_shared<std::atomic<bool>>(true);
    replicas->Fail = [replicasDown, failFirstInsert] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && replicasDown->load()) {
            throw MakeRefusal(operation.Host + "-down");
        }
        if (operation.Kind == "insert" && failFirstInsert->exchange(false)) {
            *replicasDown = true;
            throw MakeConnectionReset();
        }
    };
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 1);
    dynamicParameters->RetryBackoff = TDuration::MilliSeconds(1);
    auto profiler = CreateSyncStatusProfiler();
    auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), dynamicParameters, replicas, profiler->ErrorState("/writing"));
    writer->Connect(MakeMetadataTestSchema());
    TRunningWriter running(writer);

    auto write = writer->Write(EWriteGuarantee::ExactlyOnce, {TClickHouseShardWrite{.DedupToken = "token"}});
    EXPECT_TRUE(WaitUntil([&] {
        return replicas->Count("create") >= 7;
    }));
    EXPECT_FALSE(write.IsSet());
    const auto error = GetWritingError(profiler);
    EXPECT_THAT(error, testing::HasSubstr("No available ClickHouse replica"));
    EXPECT_THAT(error, testing::HasSubstr("a1-down"));
    EXPECT_THAT(error, testing::HasSubstr("a2-down"));
    EXPECT_EQ(replicas->Count("insert"), 1);

    *replicasDown = false;
    EXPECT_TRUE(WaitFor(write.WithTimeout(TDuration::Seconds(5))).IsOK());
    std::vector<std::optional<std::string>> tokens;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "insert") {
            tokens.push_back(operation.DedupToken);
        }
    }
    EXPECT_EQ(tokens, (std::vector<std::optional<std::string>>{"token", "token"}));
    EXPECT_TRUE(GetWritingError(profiler).empty());

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterAdmissionTest, StoppedWriterFailsPendingRequest)
{
    auto replicas = MakeReplicas();
    auto replicasDown = std::make_shared<std::atomic<bool>>(false);
    replicas->Fail = [replicasDown] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && replicasDown->load()) {
            throw MakeRefusal();
        }
        if (operation.Kind == "insert") {
            *replicasDown = true;
            throw MakeConnectionReset();
        }
    };
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 1);
    dynamicParameters->RetryBackoff = TDuration::MilliSeconds(1);
    auto writer = MakeFakeWriter(MakeHostsParameters({"a1", "a2"}), dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    TRunningWriter running(writer);

    auto write = writer->Write(EWriteGuarantee::ExactlyOnce, {TClickHouseShardWrite{}});
    EXPECT_TRUE(WaitUntil([&] {
        return replicas->Count("create") >= 3;
    }));
    EXPECT_TRUE(running.Stop().IsOK());
    auto result = WaitFor(write.WithTimeout(TDuration::Seconds(5)));
    EXPECT_FALSE(result.IsOK());
}

TEST(TClickHouseWriterAdmissionTest, UnfinishedRunSurvivesTimedOutStopAndDestruction)
{
    auto replicas = MakeReplicas();
    auto insertReleased = NewPromise<void>();
    auto releaseInsertOnExit = Finally([insertReleased] {
        insertReleased.TrySet();
    });
    replicas->Fail = [insertReleased = insertReleased.ToFuture()] (const TFakeOperation& operation) {
        if (operation.Kind == "insert") {
            WaitFor(insertReleased.WithTimeout(TDuration::Seconds(30)))
                .ThrowOnError();
        }
    };
    auto writer = MakeFakeWriter(MakeHostsParameters({"a1"}), MakeShardTestDynamicParameters(1), replicas);
    writer->Connect(MakeMetadataTestSchema());
    auto write = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}});
    TFuture<void> run;

    {
        TRunningWriter running(writer);
        run = running.GetRunFuture();
        EXPECT_TRUE(WaitUntil([&] {
            return replicas->Count("insert") == 1;
        }));

        const auto unfinishedStop = running.Stop(TDuration::MilliSeconds(100));
        EXPECT_TRUE(unfinishedStop.FindMatching(NYT::EErrorCode::Timeout)) << ToString(unfinishedStop);
    }
    EXPECT_FALSE(write.IsSet());
    EXPECT_FALSE(run.IsSet());

    insertReleased.Set();
    EXPECT_TRUE(WaitFor(write.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_TRUE(WaitFor(run.ToUncancelable().WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_EQ(replicas->Count("insert"), 1);
}

struct TIndeterminatePeerCase
{
    bool TimeoutRecreation = false;
    EClickHouseHostSelectionPolicy Policy = EClickHouseHostSelectionPolicy::OrderedRoundRobin;
    bool FailFirstInsertAfterAdmission = false;
};

void PrintTo(const TIndeterminatePeerCase& value, std::ostream* os)
{
    *os << (value.TimeoutRecreation ? "timeout" : "reset") << "_"
        << ToString(value.Policy) << "_"
        << (value.FailFirstInsertAfterAdmission ? "carry" : "success");
}

class TClickHouseIndeterminatePeerTest
    : public testing::TestWithParam<TIndeterminatePeerCase>
{ };

TEST_P(TClickHouseIndeterminatePeerTest, HealthyPeerIsAdmittedAfterIndeterminateCandidate)
{
    const auto& testCase = GetParam();
    auto parameters = MakeHostsParameters({"a", "b"});
    parameters->HostSelectionPolicy = testCase.Policy;
    auto initialDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 2);
    initialDynamicParameters->RetryBackoff = TDuration::MilliSeconds(1);
    auto updatedDynamicParameters = CloneYsonStruct(initialDynamicParameters);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);

    auto replicas = MakeReplicas();
    const std::vector<int> randomOffsets{1, 0};
    auto randomDraws = std::make_shared<std::atomic<int>>(0);
    auto observedHostCounts = std::make_shared<std::vector<int>>();
    replicas->SelectRandomStartOffset = [randomOffsets, randomDraws, observedHostCounts] (int hostCount) {
        observedHostCounts->push_back(hostCount);
        return randomOffsets.at((*randomDraws)++);
    };
    auto started = std::make_shared<std::atomic<bool>>(false);
    auto failInsertOnB = std::make_shared<std::atomic<bool>>(!testCase.TimeoutRecreation);
    auto failInsertAfterAdmission = std::make_shared<std::atomic<bool>>(testCase.FailFirstInsertAfterAdmission);
    auto admittedReplacement = std::make_shared<std::atomic<bool>>(false);
    replicas->Fail = [started, failInsertOnB, failInsertAfterAdmission, admittedReplacement] (const TFakeOperation& operation) {
        if (operation.Purpose != EClickHouseConnectionPurpose::Write) {
            return;
        }
        if (!started->load() && operation.Kind == "create" && operation.Host == "a") {
            throw MakeRefusal();
        }
        if (started->load() && operation.Kind == "metadata" && operation.Host == "a") {
            throw clickhouse::ProtocolError("truncated metadata response");
        }
        if (started->load() && operation.Kind == "dedup" && operation.Host == "b") {
            *admittedReplacement = true;
        }
        if (operation.Kind == "insert" && failInsertOnB->exchange(false)) {
            throw MakeConnectionReset();
        }
        if (operation.Kind == "insert" && admittedReplacement->load() && failInsertAfterAdmission->exchange(false)) {
            throw std::runtime_error("unclassified insert failure");
        }
    };
    auto writer = MakeFakeWriter(parameters, initialDynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    EXPECT_EQ(replicas->Describe().back(), "dedup b");
    *started = true;
    replicas->ClearOperations();
    if (testCase.TimeoutRecreation) {
        writer->Reconfigure(updatedDynamicParameters);
    }
    TRunningWriter running(writer);

    auto result = WriteAndWait(writer, EWriteGuarantee::ExactlyOnce, {TClickHouseShardWrite{.DedupToken = "token"}});
    std::vector<std::string> expected;
    if (!testCase.TimeoutRecreation) {
        expected.push_back("insert b");
    }
    expected.insert(expected.end(), {"create a", "metadata a", "create b", "metadata b", "dedup b", "insert b"});
    const int expectedDraws = testCase.Policy == EClickHouseHostSelectionPolicy::RandomStart
        ? (testCase.TimeoutRecreation ? 2 : 1)
        : 0;
    if (testCase.FailFirstInsertAfterAdmission) {
        EXPECT_EQ(result.GetMessage(), "Giving up insert into ClickHouse");
        EXPECT_EQ(replicas->Describe(), expected);
        ASSERT_TRUE(running.Stop().IsOK());
        EXPECT_EQ(*observedHostCounts, std::vector<int>(expectedDraws, 2));
        return;
    }
    EXPECT_TRUE(result.IsOK());
    EXPECT_EQ(replicas->Describe(), expected);

    const auto operations = replicas->GetOperations(EClickHouseConnectionPurpose::Write);
    for (const auto& operation : operations) {
        if (operation.Kind == "insert") {
            EXPECT_EQ(operation.DedupToken, std::optional<std::string>("token"));
        }
    }
    const auto& failedCandidate = operations[expected.size() - 6];
    const auto& admittedCandidate = operations[expected.size() - 4];
    const auto& lastInsert = operations.back();
    EXPECT_EQ(failedCandidate.Host, "a");
    EXPECT_NE(lastInsert.Generation, failedCandidate.Generation);
    EXPECT_EQ(lastInsert.Generation, admittedCandidate.Generation);
    EXPECT_EQ(
        lastInsert.SocketTimeout,
        testCase.TimeoutRecreation ? TDuration::Seconds(61) : TDuration::Seconds(60));
    EXPECT_EQ(randomDraws->load(), expectedDraws);

    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::ExactlyOnce).IsOK());

    ASSERT_TRUE(running.Stop().IsOK());
    EXPECT_EQ(*observedHostCounts, std::vector<int>(expectedDraws, 2));
}

INSTANTIATE_TEST_SUITE_P(
    Paths,
    TClickHouseIndeterminatePeerTest,
    testing::ValuesIn(std::vector<TIndeterminatePeerCase>{
        {.TimeoutRecreation = false, .Policy = EClickHouseHostSelectionPolicy::OrderedRoundRobin},
        {.TimeoutRecreation = false, .Policy = EClickHouseHostSelectionPolicy::RandomStart},
        {.TimeoutRecreation = true, .Policy = EClickHouseHostSelectionPolicy::OrderedRoundRobin},
        {.TimeoutRecreation = true, .Policy = EClickHouseHostSelectionPolicy::RandomStart},
        {.TimeoutRecreation = false, .Policy = EClickHouseHostSelectionPolicy::OrderedRoundRobin, .FailFirstInsertAfterAdmission = true},
        {.TimeoutRecreation = false, .Policy = EClickHouseHostSelectionPolicy::RandomStart, .FailFirstInsertAfterAdmission = true},
        {.TimeoutRecreation = true, .Policy = EClickHouseHostSelectionPolicy::OrderedRoundRobin, .FailFirstInsertAfterAdmission = true},
        {.TimeoutRecreation = true, .Policy = EClickHouseHostSelectionPolicy::RandomStart, .FailFirstInsertAfterAdmission = true},
    }));

TEST(TClickHouseWriterAdmissionTest, StagingRollbackKeepsFailedShardCursor)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"c", {"c1", "c2"}},
        {"d", {"a", "b"}},
    };
    parameters->Table = "events";
    auto initialDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 2);
    initialDynamicParameters->RetryBackoff = TDuration::MilliSeconds(1);
    auto updatedDynamicParameters = CloneYsonStruct(initialDynamicParameters);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);

    auto replicas = MakeReplicas();
    auto started = std::make_shared<std::atomic<bool>>(false);
    replicas->Fail = [started] (const TFakeOperation& operation) {
        if (operation.Purpose != EClickHouseConnectionPurpose::Write || operation.Host != "a") {
            return;
        }
        if (!started->load() && operation.Kind == "create") {
            throw MakeRefusal();
        }
        if (started->load() && operation.Kind == "metadata") {
            throw MakeServerException(999);
        }
    };
    auto writer = MakeFakeWriter(parameters, initialDynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    *started = true;
    replicas->ClearOperations();
    writer->Reconfigure(updatedDynamicParameters);
    TRunningWriter running(writer);

    EXPECT_TRUE(WriteAndWait(
        writer,
        EWriteGuarantee::ExactlyOnce,
        {TClickHouseShardWrite{.ShardIndex = 0}, TClickHouseShardWrite{.ShardIndex = 1}})
            .IsOK());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{
            "create c1",
            "metadata c1",
            "dedup c1",
            "create a",
            "metadata a",
            "create c1",
            "metadata c1",
            "dedup c1",
            "create b",
            "metadata b",
            "dedup b",
            "insert c1",
            "insert b",
        }));
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "insert") {
            EXPECT_EQ(operation.SocketTimeout, TDuration::Seconds(61));
        }
    }

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterAdmissionTest, IndeterminateTraversalVisitsEveryCandidateBeforeRepeating)
{
    auto replicas = MakeReplicas();
    auto started = std::make_shared<std::atomic<bool>>(false);
    replicas->Fail = [started] (const TFakeOperation& operation) {
        if (!started->load() || operation.Purpose != EClickHouseConnectionPurpose::Write) {
            return;
        }
        if (operation.Kind == "metadata") {
            throw clickhouse::ProtocolError("truncated metadata response");
        }
        if (operation.Kind == "insert") {
            throw MakeConnectionReset();
        }
    };
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 7);
    auto writer = MakeFakeWriter(MakeHostsParameters({"h0", "h1", "h2"}), dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    *started = true;
    replicas->ClearOperations();
    TRunningWriter running(writer);

    auto result = WriteAndWait(writer, EWriteGuarantee::AtLeastOnce);
    EXPECT_EQ(result.GetMessage(), "Giving up ClickHouse connection admission");
    std::vector<std::string> candidates;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "create") {
            candidates.push_back(operation.Host);
        }
    }
    EXPECT_EQ(candidates, (std::vector<std::string>{"h1", "h2", "h0", "h1", "h2", "h0", "h1"}));
    EXPECT_EQ(replicas->Count("insert"), 1);

    EXPECT_TRUE(running.Stop().IsOK());
}

class TClickHouseAdmissionBudgetTest
    : public testing::TestWithParam<EWriteGuarantee>
{ };

TEST_P(TClickHouseAdmissionBudgetTest, ExhaustedIndeterminateBudgetFollowsGuaranteePolicy)
{
    auto replicas = MakeReplicas();
    auto unhealthy = std::make_shared<std::atomic<bool>>(false);
    auto failedAdmissions = std::make_shared<std::atomic<int>>(0);
    replicas->Fail = [unhealthy, failedAdmissions] (const TFakeOperation& operation) {
        if (!unhealthy->load() || operation.Purpose != EClickHouseConnectionPurpose::Write) {
            return;
        }
        if (operation.Kind == "metadata") {
            if (++*failedAdmissions % 2 == 0) {
                throw MakeServerException(999);
            }
            throw std::system_error(std::make_error_code(std::errc::connection_reset), "closed");
        }
        if (operation.Kind == "insert") {
            throw MakeConnectionReset();
        }
    };
    auto writer = MakeFakeWriter(MakeHostsParameters({"only"}), MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3), replicas);
    writer->Connect(MakeMetadataTestSchema());
    *unhealthy = true;
    replicas->ClearOperations();
    TRunningWriter running(writer);

    auto result = WriteAndWait(writer, GetParam());
    EXPECT_EQ(result.GetMessage(), "Giving up ClickHouse connection admission");
    EXPECT_EQ(failedAdmissions->load(), 3);
    EXPECT_EQ(replicas->Count("create"), 3);
    EXPECT_EQ(replicas->Count("insert"), 1);

    *unhealthy = false;
    replicas->ClearOperations();
    auto next = WriteAndWait(writer, GetParam());
    if (GetParam() == EWriteGuarantee::ExactlyOnce) {
        EXPECT_EQ(next.GetMessage(), "Giving up ClickHouse connection admission");
        EXPECT_TRUE(replicas->Describe().empty());
    } else {
        EXPECT_TRUE(next.IsOK());
        EXPECT_EQ(
            replicas->Describe(),
            (std::vector<std::string>{"create only", "metadata only", "dedup only", "insert only"}));
    }

    EXPECT_TRUE(running.Stop().IsOK());
}

INSTANTIATE_TEST_SUITE_P(
    Guarantees,
    TClickHouseAdmissionBudgetTest,
    testing::Values(EWriteGuarantee::ExactlyOnce, EWriteGuarantee::AtLeastOnce));

TEST(TClickHouseWriterAdmissionTest, RandomStartUsesOneOffsetPerSelectionSequence)
{
    auto parameters = MakeHostsParameters({"a", "b", "c"});
    parameters->HostSelectionPolicy = EClickHouseHostSelectionPolicy::RandomStart;
    auto replicas = MakeReplicas();
    std::atomic<int> randomDraws = 0;
    replicas->SelectRandomStartOffset = [&] (int hostCount) {
        EXPECT_EQ(hostCount, 3);
        ++randomDraws;
        return 2;
    };
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Purpose == EClickHouseConnectionPurpose::Write &&
            operation.Kind == "create" &&
            operation.Host != "b")
        {
            throw MakeRefusal();
        }
    };
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(3), replicas);
    writer->Connect(MakeMetadataTestSchema());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{"create c", "create a", "create b", "metadata b", "dedup b"}));
    EXPECT_EQ(randomDraws.load(), 1);
}

TEST(TClickHouseWriterTest, ReconfigureAffectsOnlyNextRequest)
{
    auto parameters = MakeHostsParameters({"host"});

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

    auto weakWriter = std::make_shared<TWeakPtr<TClickHouseWriter>>();
    auto replicas = MakeReplicas();
    auto insertAttempts = std::make_shared<std::atomic<int>>(0);
    replicas->Fail = [weakWriter, insertAttempts, updatedDynamicParameters] (const TFakeOperation& operation) {
        if (operation.Kind != "insert") {
            return;
        }
        const int attempt = ++*insertAttempts;
        if (attempt == 1) {
            weakWriter->Lock()->Reconfigure(updatedDynamicParameters);
        }
        if (attempt == 1 || attempt == 2 || attempt == 4) {
            throw std::runtime_error("insert failed");
        }
    };
    auto writer = MakeFakeWriter(parameters, initialDynamicParameters, replicas);
    *weakWriter = writer;
    writer->Connect(MakeMetadataTestSchema());
    auto firstWrite = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}});
    auto secondWrite = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}});
    TRunningWriter running(writer);

    auto firstResult = WaitFor(firstWrite.WithTimeout(TDuration::Seconds(5)));
    auto secondResult = WaitFor(secondWrite.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(firstResult.IsOK());
    EXPECT_FALSE(secondResult.IsOK());
    EXPECT_EQ(secondResult.GetMessage(), "Giving up insert into ClickHouse");

    std::vector<TDuration> createdTimeouts;
    std::vector<bool> asyncInsertValues;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "create") {
            createdTimeouts.push_back(operation.SocketTimeout);
        } else if (operation.Kind == "insert") {
            asyncInsertValues.push_back(operation.AsyncInsert);
        }
    }
    EXPECT_EQ(
        createdTimeouts,
        (std::vector<TDuration>{
            TDuration::Seconds(60),
            TDuration::Seconds(60),
            TDuration::Seconds(60),
            TDuration::Seconds(120),
        }));
    EXPECT_EQ(asyncInsertValues, std::vector<bool>({false, false, false, true}));

    EXPECT_TRUE(running.Stop().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TClickHouseArrayWriterTest
    : public testing::TestWithParam<EWriteGuarantee>
{ };

TEST_P(TClickHouseArrayWriterTest, InvalidAsyncRecoversWithoutDroppingRequest)
{
    for (bool initiallyAsync : {false, true}) {
        SCOPED_TRACE(initiallyAsync);
        auto parameters = MakeHostsParameters({"host"});
        auto initial = New<TDynamicCommonClickHouseSinkParameters>();
        initial->AsyncInsert = initiallyAsync;
        initial->RetryBackoff = TDuration::MilliSeconds(10);
        auto invalid = New<TDynamicCommonClickHouseSinkParameters>();
        invalid->AsyncInsert = true;
        invalid->RetryBackoff = TDuration::MilliSeconds(10);
        auto corrected = New<TDynamicCommonClickHouseSinkParameters>();
        corrected->RetryBackoff = TDuration::MilliSeconds(10);
        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("tokens", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
            TColumnSchema("plain", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
        });
        auto profiler = CreateSyncStatusProfiler();
        auto insertAttempts = std::make_shared<std::atomic<int>>(0);
        auto weakWriter = std::make_shared<TWeakPtr<TClickHouseWriter>>();
        auto replicas = MakeReplicas();
        replicas->Metadata = [] (const TFakeOperation& operation) {
            auto metadata = MakeReplicatedMetadata(operation.Host);
            metadata.Columns = {
                {.Name = "tokens", .Type = "SimpleAggregateFunction(groupUniqArrayArray, Array(String))"},
                {.Name = "plain", .Type = "Array(String)"},
            };
            return metadata;
        };
        replicas->Fail = [insertAttempts, weakWriter, invalid] (const TFakeOperation& operation) {
            if (operation.Kind != "insert") {
                return;
            }
            if (++*insertAttempts == 1) {
                weakWriter->Lock()->Reconfigure(invalid);
                throw MakeConnectionReset();
            }
        };
        auto writer = MakeFakeWriter(parameters, initial, replicas, profiler->ErrorState("/writing"));
        *weakWriter = writer;
        writer->Connect(schema);
        if (!initiallyAsync) {
            writer->Reconfigure(invalid);
        }

        TRunningWriter running(writer);
        auto write = writer->Write(GetParam(), {TClickHouseShardWrite{}});
        WaitUntil([&] {
            return !profiler->GetStatus().Errors.empty() || write.IsSet();
        });
        const auto errors = profiler->GetStatus().Errors;
        EXPECT_TRUE(errors.contains("/writing"));
        if (errors.contains("/writing")) {
            EXPECT_THAT(ToString(GetOrCrash(errors, "/writing")), testing::HasSubstr("async_insert=false"));
        }
        EXPECT_FALSE(write.IsSet());
        EXPECT_EQ(insertAttempts->load(), 0);

        writer->Reconfigure(corrected);
        EXPECT_TRUE(WaitFor(write.WithTimeout(TDuration::Seconds(5))).IsOK());
        EXPECT_EQ(insertAttempts->load(), 2);
        EXPECT_TRUE(profiler->GetStatus().Errors.empty());

        auto nextWrite = writer->Write(GetParam(), {TClickHouseShardWrite{}});
        WaitUntil([&] {
            return !profiler->GetStatus().Errors.empty() || nextWrite.IsSet();
        });
        EXPECT_TRUE(profiler->GetStatus().Errors.contains("/writing"));
        EXPECT_FALSE(nextWrite.IsSet());
        EXPECT_EQ(insertAttempts->load(), 2);

        ASSERT_TRUE(running.Stop().IsOK());
        EXPECT_FALSE(WaitFor(nextWrite.WithTimeout(TDuration::Seconds(5))).IsOK());
        std::vector<bool> observedAsync;
        for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
            if (operation.Kind == "insert") {
                observedAsync.push_back(operation.AsyncInsert);
            }
        }
        EXPECT_EQ(observedAsync, std::vector<bool>({false, false}));
    }
}

TEST_P(TClickHouseArrayWriterTest, PlainArrayAndOmittedAggregateKeepAsync)
{
    auto parameters = MakeHostsParameters({"host"});
    auto dynamicParameters = New<TDynamicCommonClickHouseSinkParameters>();
    dynamicParameters->AsyncInsert = true;
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("tokens", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
    });
    auto replicas = MakeReplicas();
    replicas->Metadata = [] (const TFakeOperation& operation) {
        auto metadata = MakeReplicatedMetadata(operation.Host);
        metadata.Columns = {
            {.Name = "tokens", .Type = "Array(String)"},
            {.Name = "omitted", .Type = "SimpleAggregateFunction(groupUniqArrayArray, Array(String))"},
        };
        return metadata;
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas);
    writer->Connect(schema);
    TRunningWriter running(writer);

    EXPECT_TRUE(WriteAndWait(writer, GetParam()).IsOK());
    std::vector<bool> observedAsync;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "insert") {
            observedAsync.push_back(operation.AsyncInsert);
        }
    }
    EXPECT_EQ(observedAsync, std::vector<bool>{true});

    EXPECT_TRUE(running.Stop().IsOK());
}

INSTANTIATE_TEST_SUITE_P(
    Guarantees,
    TClickHouseArrayWriterTest,
    testing::Values(EWriteGuarantee::ExactlyOnce, EWriteGuarantee::AtLeastOnce));

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterTest, ConfigurationFailureRetriesBeforeInsert)
{
    auto parameters = MakeHostsParameters({"host"});
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto updatedDynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);

    auto replicas = MakeReplicas();
    auto refuseNextCreate = std::make_shared<std::atomic<bool>>(false);
    replicas->Fail = [refuseNextCreate] (const TFakeOperation& operation) {
        if (operation.Kind == "create" && refuseNextCreate->exchange(false)) {
            throw MakeRefusal();
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    replicas->ClearOperations();
    writer->Reconfigure(updatedDynamicParameters);
    *refuseNextCreate = true;
    TRunningWriter running(writer);

    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce).IsOK());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{"create host", "create host", "metadata host", "dedup host", "insert host"}));

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterTest, FailedReadmissionStaysPendingUntilSuccess)
{
    auto parameters = MakeHostsParameters({"host"});
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);

    auto replicas = MakeReplicas();
    auto started = std::make_shared<std::atomic<bool>>(false);
    auto insertAttempts = std::make_shared<std::atomic<int>>(0);
    auto createAttempts = std::make_shared<std::atomic<int>>(0);
    replicas->Fail = [started, insertAttempts, createAttempts] (const TFakeOperation& operation) {
        if (!started->load() || operation.Purpose != EClickHouseConnectionPurpose::Write) {
            return;
        }
        if (operation.Kind == "create" && ++*createAttempts == 1) {
            throw MakeRefusal();
        }
        if (operation.Kind == "insert" && ++*insertAttempts == 1) {
            throw MakeConnectionReset();
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    *started = true;
    replicas->ClearOperations();
    auto firstWrite = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}});
    auto secondWrite = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{}});
    TRunningWriter running(writer);

    EXPECT_TRUE(WaitFor(firstWrite.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_TRUE(WaitFor(secondWrite.WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{
            "insert host",
            "create host",
            "create host",
            "metadata host",
            "dedup host",
            "insert host",
            "insert host",
        }));

    EXPECT_TRUE(running.Stop().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseWriterShardTest, ShardsInsertSequentiallyAndRetryOnlyTheFailingShard)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 5);
    auto replicas = MakeReplicas();
    auto failingShardAttempts = std::make_shared<std::atomic<int>>(0);
    replicas->Fail = [failingShardAttempts] (const TFakeOperation& operation) {
        if (operation.Kind == "insert" && operation.ShardIndex == 1 && ++*failingShardAttempts <= 2) {
            throw MakeConnectionReset();
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    replicas->ClearOperations();
    TRunningWriter running(writer);

    std::vector<TClickHouseShardWrite> writes;
    for (int shardIndex = 0; shardIndex < 3; ++shardIndex) {
        writes.push_back({.ShardIndex = shardIndex, .DedupToken = Format("batch:%v", shardIndex)});
    }
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::ExactlyOnce, std::move(writes)).IsOK());
    std::vector<int> insertedShards;
    std::vector<int> readmittedShards;
    for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
        if (operation.Kind == "insert") {
            insertedShards.push_back(operation.ShardIndex);
            EXPECT_EQ(operation.DedupToken, std::optional<std::string>(Format("batch:%v", operation.ShardIndex)));
        } else if (operation.Kind == "create") {
            readmittedShards.push_back(operation.ShardIndex);
        }
    }
    EXPECT_EQ(insertedShards, (std::vector<int>{0, 1, 1, 1, 2}));
    EXPECT_EQ(readmittedShards, (std::vector<int>{1, 1}));

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterShardTest, HealthTracksEachFailingShardUntilItsRecovery)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    auto profiler = CreateSyncStatusProfiler();
    auto phase = std::make_shared<std::atomic<int>>(0);
    auto replicas = MakeReplicas();
    replicas->Fail = [phase] (const TFakeOperation& operation) {
        if (operation.Kind != "insert") {
            return;
        }
        if ((phase->load() == 0 && operation.ShardIndex < 2) ||
            (phase->load() == 1 && operation.ShardIndex == 0))
        {
            throw std::runtime_error(operation.ShardIndex == 0 ? "failure-a" : "failure-b");
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas, profiler->ErrorState("/writing"));
    writer->Connect(MakeMetadataTestSchema());
    TRunningWriter running(writer);

    for (int shardIndex : {0, 1}) {
        EXPECT_FALSE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = shardIndex}}).IsOK());
    }
    auto errorText = GetWritingError(profiler);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("failure-b"), std::string::npos);

    *phase = 1;
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = 1}}).IsOK());
    errorText = GetWritingError(profiler);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("failure-b"), std::string::npos);

    *phase = 2;
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = 0}}).IsOK());
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterShardTest, ConfigurationRecoveryPreservesWritingError)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3);
    dynamicParameters->RetryBackoff = TDuration::MilliSeconds(10);
    auto updatedDynamicParameters = CloneYsonStruct(dynamicParameters);
    updatedDynamicParameters->WriteTimeout = TDuration::Seconds(61);
    auto profiler = CreateSyncStatusProfiler();
    auto configurationPhase = std::make_shared<std::atomic<int>>(0);
    auto failWriting = std::make_shared<std::atomic<bool>>(true);
    auto replicas = MakeReplicas();
    replicas->Fail = [configurationPhase, failWriting] (const TFakeOperation& operation) {
        if (operation.Purpose != EClickHouseConnectionPurpose::Write) {
            return;
        }
        if (operation.Kind == "create") {
            if (configurationPhase->load() == 1 && operation.ShardIndex < 2) {
                throw MakeRefusal(operation.ShardIndex == 0 ? "configuration-failure-a" : "configuration-failure-b");
            }
            if (configurationPhase->load() == 2 && operation.ShardIndex == 1) {
                throw MakeRefusal("configuration-failure-b");
            }
        }
        if (operation.Kind == "insert" && operation.ShardIndex == 0 && failWriting->load()) {
            throw std::runtime_error("failure-a");
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas, profiler->ErrorState("/writing"));
    writer->Connect(MakeMetadataTestSchema());
    TRunningWriter running(writer);

    EXPECT_FALSE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = 0}}).IsOK());
    EXPECT_NE(GetWritingError(profiler).find("failure-a"), std::string::npos);

    *configurationPhase = 1;
    writer->Reconfigure(updatedDynamicParameters);
    const auto insertsBeforeBlockedRequest = replicas->Count("insert");
    std::vector<int> createsBeforeFailure;
    for (int shardIndex = 0; shardIndex < 3; ++shardIndex) {
        createsBeforeFailure.push_back(std::ranges::count_if(
            replicas->GetOperations(EClickHouseConnectionPurpose::Write),
            [&] (const auto& operation) {
                return operation.Kind == "create" && operation.ShardIndex == shardIndex;
            }));
    }
    auto blockedWrite = writer->Write(EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = 2}});
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    EXPECT_FALSE(blockedWrite.IsSet());
    for (int shardIndex = 0; shardIndex < 3; ++shardIndex) {
        EXPECT_GT(
            std::ranges::count_if(
                replicas->GetOperations(EClickHouseConnectionPurpose::Write),
                [&] (const auto& operation) {
                    return operation.Kind == "create" && operation.ShardIndex == shardIndex;
                }),
            createsBeforeFailure[shardIndex]);
    }
    EXPECT_EQ(replicas->Count("insert"), insertsBeforeBlockedRequest);
    auto errorText = GetWritingError(profiler);
    EXPECT_NE(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("configuration-failure-b"), std::string::npos);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);

    *configurationPhase = 2;
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    EXPECT_FALSE(blockedWrite.IsSet());
    errorText = GetWritingError(profiler);
    EXPECT_EQ(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_NE(errorText.find("configuration-failure-b"), std::string::npos);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);

    *configurationPhase = 3;
    EXPECT_TRUE(WaitFor(blockedWrite.WithTimeout(TDuration::Seconds(5))).IsOK());
    errorText = GetWritingError(profiler);
    EXPECT_NE(errorText.find("failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("configuration-failure-a"), std::string::npos);
    EXPECT_EQ(errorText.find("configuration-failure-b"), std::string::npos);

    *failWriting = false;
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, {TClickHouseShardWrite{.ShardIndex = 0}}).IsOK());
    EXPECT_TRUE(profiler->GetStatus().Errors.empty());

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterShardTest, ExactlyOnceGiveUpStopsRemainingShards)
{
    auto parameters = MakeThreeShardParameters();
    auto dynamicParameters = MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 2);
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Kind == "insert" && operation.ShardIndex == 1) {
            throw std::runtime_error("shard is down");
        }
    };
    auto writer = MakeFakeWriter(parameters, dynamicParameters, replicas);
    writer->Connect(MakeMetadataTestSchema());
    TRunningWriter running(writer);

    auto insertedShards = [&] {
        std::vector<int> result;
        for (const auto& operation : replicas->GetOperations(EClickHouseConnectionPurpose::Write)) {
            if (operation.Kind == "insert") {
                result.push_back(operation.ShardIndex);
            }
        }
        return result;
    };
    auto result = WriteAndWait(writer, EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites());
    EXPECT_FALSE(result.IsOK());
    EXPECT_EQ(result.GetMessage(), "Giving up insert into ClickHouse");
    EXPECT_EQ(insertedShards(), (std::vector<int>{0, 1, 1}));

    auto rejected = WriteAndWait(writer, EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites());
    EXPECT_FALSE(rejected.IsOK());
    EXPECT_EQ(rejected.GetMessage(), "Giving up insert into ClickHouse");
    EXPECT_EQ(insertedShards(), (std::vector<int>{0, 1, 1}));

    EXPECT_TRUE(running.Stop().IsOK());
}

TEST(TClickHouseWriterShardTest, HealthyRequestsReuseAdmittedConnections)
{
    auto parameters = MakeThreeShardParameters();
    auto errorState = New<TCountingStatusErrorState>();
    auto replicas = MakeReplicas();
    auto writer = MakeFakeWriter(parameters, MakeShardTestDynamicParameters(/*maxInsertAttempts*/ 3), replicas, errorState);
    writer->Connect(MakeMetadataTestSchema());
    replicas->ClearOperations();
    TRunningWriter running(writer);

    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::AtLeastOnce, MakeThreeShardWrites()).IsOK());
    EXPECT_TRUE(WriteAndWait(writer, EWriteGuarantee::ExactlyOnce, MakeThreeShardWrites()).IsOK());
    EXPECT_EQ(replicas->Count("create"), 0);
    EXPECT_EQ(replicas->Count("metadata"), 0);
    EXPECT_TRUE(replicas->Describe(EClickHouseConnectionPurpose::Probe).empty());
    EXPECT_EQ(replicas->Count("insert"), 6);
    EXPECT_EQ(errorState->SetErrorCount.load(), 0);
    EXPECT_EQ(errorState->ClearErrorCount.load(), 0);

    EXPECT_TRUE(running.Stop().IsOK());
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

TEST(TClickHouseClientOptionsTest, EveryConfiguredHostGetsSingleHostOptions)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {
        {"a", {"ch-a-1", "ch-a-2", "ch-a-3"}},
        {"b", {"ch-b-1"}},
    };
    parameters->Port = 9002;
    parameters->Database = "db";

    for (const auto& shard : ResolveShards(*parameters)) {
        for (const auto& host : shard.Hosts) {
            auto options = MakeClientOptions(*parameters, shard, host, TDuration::Seconds(60));
            EXPECT_EQ(options.host, host);
            EXPECT_EQ(options.port, 9002);
            EXPECT_EQ(options.default_database, "db");
            EXPECT_TRUE(options.endpoints.empty());
            EXPECT_EQ(options.connection_recv_timeout, std::chrono::seconds(60));
        }
    }
}

TEST(TClickHouseClientOptionsTest, FlatHostListGetsSingleHostOptions)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Hosts = {"primary.clickhouse", "first.clickhouse", "second.clickhouse"};
    parameters->Port = 9001;
    parameters->HostSelectionPolicy = EClickHouseHostSelectionPolicy::RandomStart;

    const auto shard = ResolveShards(*parameters).front();
    for (const auto& host : shard.Hosts) {
        auto options = MakeClientOptions(*parameters, shard, host, TDuration::Seconds(60));
        EXPECT_EQ(options.host, host);
        EXPECT_EQ(options.port, 9001);
        EXPECT_TRUE(options.endpoints.empty());
    }
}

TEST(TClickHouseHostSelectionPolicyTest, CandidateStartFollowsPolicy)
{
    EXPECT_EQ(SelectClickHouseCandidateStart(EClickHouseHostSelectionPolicy::OrderedRoundRobin, 3, 2), 0);
    EXPECT_EQ(SelectClickHouseCandidateStart(EClickHouseHostSelectionPolicy::RandomStart, 3, 0), 0);
    EXPECT_EQ(SelectClickHouseCandidateStart(EClickHouseHostSelectionPolicy::RandomStart, 3, 1), 1);
    EXPECT_EQ(SelectClickHouseCandidateStart(EClickHouseHostSelectionPolicy::RandomStart, 3, 2), 2);
    EXPECT_EQ(SelectClickHouseCandidateStart(EClickHouseHostSelectionPolicy::RandomStart, 1, 0), 0);
}

TEST(TClickHouseHostSelectionPolicyTest, OrderedTraversalKeepsTrailingCandidates)
{
    auto replicas = MakeReplicas();
    replicas->Fail = [] (const TFakeOperation& operation) {
        if (operation.Purpose == EClickHouseConnectionPurpose::Write &&
            operation.Kind == "create" &&
            operation.Host != "c")
        {
            throw MakeRefusal();
        }
    };
    auto writer = MakeFakeWriter(MakeHostsParameters({"a", "b", "c"}), MakeShardTestDynamicParameters(3), replicas);
    writer->Connect(MakeMetadataTestSchema());
    EXPECT_EQ(
        replicas->Describe(),
        (std::vector<std::string>{"create a", "create b", "create c", "metadata c", "dedup c"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
