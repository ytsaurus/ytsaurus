#include "sink.h"

#include "block_builder.h"

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/string/format.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/columns/string.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/exceptions.h>

#include <util/random/random.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <algorithm>
#include <set>
#include <system_error>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto MetadataProbeTimeout = std::chrono::seconds(5);

clickhouse::CompressionMethod ToCompressionMethod(EClickHouseCodec codec)
{
    switch (codec) {
        case EClickHouseCodec::None:
            return clickhouse::CompressionMethod::None;
        case EClickHouseCodec::Lz4:
            return clickhouse::CompressionMethod::LZ4;
        case EClickHouseCodec::Zstd:
            return clickhouse::CompressionMethod::ZSTD;
    }
    YT_ABORT();
}

std::string QuoteIdentifier(const std::string& name)
{
    std::string result = "`";
    for (char c : name) {
        if (c == '`') {
            result += '`';
        }
        result += c;
    }
    result += '`';
    return result;
}

std::string QuoteLiteral(const std::string& value)
{
    std::string result = "'";
    for (char c : value) {
        if (c == '\'' || c == '\\') {
            result += '\\';
        }
        result += c;
    }
    result += '\'';
    return result;
}

std::vector<std::vector<std::string>> QueryMetadataRows(
    clickhouse::Client& client,
    const std::string& query,
    size_t columnCount)
{
    std::vector<std::vector<std::string>> rows;
    client.Select(query, [&] (const clickhouse::Block& block) {
        if (!ShouldConsumeClickHouseMetadataBlock(
            block.GetColumnCount(),
            block.GetRowCount(),
            columnCount))
        {
            return;
        }
        for (size_t rowIndex = 0; rowIndex < block.GetRowCount(); ++rowIndex) {
            std::vector<std::string> row;
            row.reserve(columnCount);
            for (size_t columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                row.emplace_back(block[columnIndex]->As<clickhouse::ColumnString>()->At(rowIndex));
            }
            rows.push_back(std::move(row));
        }
    });
    return rows;
}

std::vector<NTableClient::TTableSchemaPtr> CollectInputStreamSchemas(
    const TSinkContextPtr& context,
    const TSinkSpecPtr& spec)
{
    std::vector<NTableClient::TTableSchemaPtr> schemas;
    for (const auto& streamId : spec->InputStreamIds) {
        schemas.push_back(context->StreamSpecStorage->GetSchema(streamId));
    }
    return schemas;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool ShouldConsumeClickHouseMetadataBlock(
    size_t actualColumnCount,
    size_t rowCount,
    size_t expectedColumnCount)
{
    if (actualColumnCount == 0 && rowCount == 0) {
        return false;
    }
    if (actualColumnCount != expectedColumnCount) {
        THROW_ERROR_EXCEPTION(
            "Unexpected ClickHouse metadata result column count: expected %v, got %v",
            expectedColumnCount,
            actualColumnCount);
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger MakeSinkLogger(
    const NLogging::TLogger& logger,
    const TCommonClickHouseSinkParameters& parameters)
{
    auto result = logger
        .WithTag("Database", parameters.Database)
        .WithTag("Table", parameters.Table);
    if (!parameters.Host.empty()) {
        return result.WithTag("Host", parameters.Host);
    }
    if (!parameters.Hosts.empty()) {
        return result.WithTag("Hosts", parameters.Hosts);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> OrderHostsForClient(
    const std::vector<std::string>& hosts,
    EClickHouseHostSelectionPolicy policy,
    size_t startOffset)
{
    YT_VERIFY(!hosts.empty());
    auto ordered = hosts;
    switch (policy) {
        case EClickHouseHostSelectionPolicy::OrderedRoundRobin:
            return ordered;
        case EClickHouseHostSelectionPolicy::RandomStart:
            YT_VERIFY(startOffset < ordered.size());
            std::rotate(ordered.begin(), ordered.begin() + startOffset, ordered.end());
            return ordered;
    }
    YT_ABORT();
}

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    TDuration writeTimeout,
    size_t startOffset)
{
    YT_VERIFY(!shard.Hosts.empty());
    auto socketTimeout = std::chrono::milliseconds(writeTimeout.MilliSeconds());
    const auto hosts = OrderHostsForClient(shard.Hosts, parameters.HostSelectionPolicy, startOffset);
    // The client tries |host|/|port| first and only then the endpoints round-robin, so the
    // shard's first host must not be repeated among them.
    std::vector<clickhouse::Endpoint> additionalEndpoints;
    additionalEndpoints.reserve(hosts.size() - 1);
    for (int hostIndex = 1; hostIndex < std::ssize(hosts); ++hostIndex) {
        additionalEndpoints.push_back({
            .host = hosts[hostIndex],
            .port = shard.Port,
        });
    }
    auto options = clickhouse::ClientOptions()
        .SetHost(hosts.front())
        .SetPort(shard.Port)
        .SetEndpoints(additionalEndpoints)
        .SetUser(parameters.User)
        .SetDefaultDatabase(shard.Database)
        .SetCompressionMethod(ToCompressionMethod(parameters.Codec))
        .SetConnectionSendTimeout(socketTimeout)
        .SetConnectionRecvTimeout(socketTimeout);
    if (!parameters.PasswordEnvVar.empty()) {
        options.SetPassword(std::string(GetEnv(TString(parameters.PasswordEnvVar))));
    }
    if (parameters.EnableTls) {
        options.SetSSLOptions(clickhouse::ClientOptions::SSLOptions()
                .SetPathToCAFiles(parameters.TlsCaFiles)
                .SetPathToCADirectory(parameters.TlsCaDirectory)
                .SetSkipVerification(parameters.TlsSkipVerification));
    }
    return options;
}

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    TDuration writeTimeout)
{
    const auto startOffset = parameters.HostSelectionPolicy == EClickHouseHostSelectionPolicy::RandomStart && shard.Hosts.size() > 1
        ? RandomNumber<ui64>(shard.Hosts.size())
        : 0;
    return MakeClientOptions(parameters, shard, writeTimeout, startOffset);
}

////////////////////////////////////////////////////////////////////////////////

std::string BuildDedupToken(const std::vector<TOutputMessageConstPtr>& messages)
{
    YT_VERIFY(!messages.empty());
    const TMessageId* maxMessageId = &messages.front()->MessageId;
    for (const auto& message : messages) {
        if (*maxMessageId < message->MessageId) {
            maxMessageId = &message->MessageId;
        }
    }
    return std::string(maxMessageId->Underlying());
}

std::vector<TClickHouseShardWrite> BuildClickHouseShardWrites(
    const TClickHouseShardRouter& router,
    const TClickHouseBlockBuilder& blockBuilder,
    const std::optional<std::string>& batchDedupToken,
    const std::vector<TOutputMessageConstPtr>& messages)
{
    if (messages.empty()) {
        return {};
    }
    const auto& shards = router.GetShards();
    std::vector<TClickHouseShardWrite> writes;
    if (shards.size() == 1) {
        writes.reserve(1);
        writes.push_back(TClickHouseShardWrite{
            .ShardIndex = 0,
            .DedupToken = batchDedupToken
                ? std::optional(BuildShardDedupToken(*batchDedupToken, shards.front()))
                : std::nullopt,
            .Block = blockBuilder.Build(messages),
        });
        return writes;
    }
    std::vector<int> selectedShards;
    selectedShards.reserve(messages.size());
    std::vector<size_t> counts(shards.size(), 0);
    for (const auto& message : messages) {
        const int shardIndex = router.SelectShard(message);
        selectedShards.push_back(shardIndex);
        ++counts[shardIndex];
    }
    std::vector<std::vector<TOutputMessageConstPtr>> groups(shards.size());
    size_t nonemptyCount = 0;
    for (size_t shardIndex = 0; shardIndex < shards.size(); ++shardIndex) {
        groups[shardIndex].reserve(counts[shardIndex]);
        nonemptyCount += counts[shardIndex] != 0;
    }
    for (size_t index = 0; index < messages.size(); ++index) {
        groups[selectedShards[index]].push_back(messages[index]);
    }
    writes.reserve(nonemptyCount);
    for (int shardIndex = 0; shardIndex < std::ssize(shards); ++shardIndex) {
        if (groups[shardIndex].empty()) {
            continue;
        }
        writes.push_back(TClickHouseShardWrite{
            .ShardIndex = shardIndex,
            .DedupToken = batchDedupToken
                ? std::optional(BuildShardDedupToken(*batchDedupToken, shards[shardIndex]))
                : std::nullopt,
            .Block = blockBuilder.Build(groups[shardIndex]),
        });
    }
    return writes;
}

EClickHouseErrorKind ClassifyClickHouseError(const std::exception& ex)
{
    if (dynamic_cast<const clickhouse::ValidationError*>(&ex) ||
        dynamic_cast<const clickhouse::UnimplementedError*>(&ex) ||
        dynamic_cast<const clickhouse::AssertionError*>(&ex))
    {
        return EClickHouseErrorKind::Permanent;
    }
    if (dynamic_cast<const clickhouse::ProtocolError*>(&ex) ||
        dynamic_cast<const clickhouse::OpenSSLError*>(&ex) ||
        dynamic_cast<const std::system_error*>(&ex))
    {
        return EClickHouseErrorKind::Retryable;
    }
    return EClickHouseErrorKind::Unclassified;
}

TClickHouseRequestOptions MakeClickHouseRequestOptions(
    const TDynamicCommonClickHouseSinkParameters& dynamicParameters)
{
    return {
        .AsyncInsert = dynamicParameters.AsyncInsert,
        .MaxInsertAttempts = dynamicParameters.MaxInsertAttempts,
    };
}

EClickHouseFailureAction GetClickHouseFailureAction(
    EWriteGuarantee guarantee,
    bool insertStarted,
    EClickHouseErrorKind errorKind,
    const TClickHouseRequestOptions& requestOptions,
    TClickHouseAttemptState* attemptState)
{
    if (!insertStarted) {
        return EClickHouseFailureAction::Retry;
    }
    if (guarantee == EWriteGuarantee::AtMostOnce) {
        return EClickHouseFailureAction::Acknowledge;
    }
    if (errorKind == EClickHouseErrorKind::Unclassified) {
        ++attemptState->UnclassifiedInsertAttempts;
    }
    if (errorKind == EClickHouseErrorKind::Permanent ||
        attemptState->UnclassifiedInsertAttempts >= requestOptions.MaxInsertAttempts)
    {
        return EClickHouseFailureAction::Fail;
    }
    return EClickHouseFailureAction::Retry;
}

std::string BuildInsertHeader(
    const std::string& database,
    const std::string& table,
    const std::vector<TResolvedColumn>& columns,
    bool asyncInsert,
    const std::optional<std::string>& dedupToken)
{
    TStringBuilder builder;
    builder.AppendFormat("INSERT INTO %v.%v (",
        QuoteIdentifier(database),
        QuoteIdentifier(table));
    for (int columnIndex = 0; columnIndex < std::ssize(columns); ++columnIndex) {
        if (columnIndex > 0) {
            builder.AppendChar(',');
        }
        builder.AppendString(QuoteIdentifier(columns[columnIndex].Name));
    }
    builder.AppendChar(')');

    std::vector<std::string> settings;
    if (dedupToken) {
        settings.push_back("insert_deduplication_token=" + QuoteLiteral(*dedupToken));
    }
    if (asyncInsert) {
        settings.push_back("async_insert=1");
        if (dedupToken) {
            settings.push_back("async_insert_deduplicate=1");
        }
        settings.push_back("wait_for_async_insert=1");
    }
    for (int settingIndex = 0; settingIndex < std::ssize(settings); ++settingIndex) {
        builder.AppendString(settingIndex == 0 ? " SETTINGS " : ", ");
        builder.AppendString(settings[settingIndex]);
    }

    builder.AppendString(" VALUES");
    return builder.Flush();
}

bool IsBlockDeduplicatingEngine(const std::string& engine)
{
    return engine == "SharedMergeTree" ||
        (engine.starts_with("Replicated") && engine.ends_with("MergeTree"));
}

bool IsPlainMergeTreeEngine(const std::string& engine)
{
    return !IsBlockDeduplicatingEngine(engine) && engine.ends_with("MergeTree");
}

void ValidateTargetEngine(
    const std::string& engine,
    const std::string& database,
    const std::string& table,
    bool hasFailoverHosts,
    const NLogging::TLogger& Logger)
{
    if (engine == "Distributed") {
        THROW_ERROR_EXCEPTION(
            "Target table %v.%v uses the Distributed engine; the sink routes rows to "
            "shards itself and must target each shard's local table",
            database,
            table);
    }
    if (engine == "SharedMergeTree" && hasFailoverHosts) {
        THROW_ERROR_EXCEPTION(
            "Target table %v.%v uses SharedMergeTree with multiple hosts; the sink cannot prove "
            "that all endpoints expose the same logical table, so use one host per shard",
            database,
            table);
    }
    if (IsBlockDeduplicatingEngine(engine)) {
        return;
    }
    if (hasFailoverHosts) {
        THROW_ERROR_EXCEPTION(
            "Target table %v.%v engine %Qv does not prove that failover endpoints expose the "
            "same logical table; more than one host per shard requires matching "
            "Replicated*MergeTree replication identity",
            database,
            table,
            engine);
    }
    if (IsPlainMergeTreeEngine(engine)) {
        YT_TLOG_WARNING(
            "Target table uses plain engine; exactly-once degrades to at-least-once "
            "unless non_replicated_deduplication_window is explicitly sized")
            .With("Database", database)
            .With("Table", table)
            .With("Engine", engine);
        return;
    }
    THROW_ERROR_EXCEPTION(
        "Target table %v.%v engine %Qv does not deduplicate inserted blocks; "
        "exactly-once requires Replicated*MergeTree / SharedMergeTree",
        database,
        table,
        engine);
}

void ValidateFailoverMetadata(
    const TClickHouseShard& shard,
    const std::vector<TClickHouseTargetMetadata>& endpoints,
    const NLogging::TLogger& logger)
{
    if (endpoints.empty() || endpoints.size() != shard.Hosts.size()) {
        THROW_ERROR_EXCEPTION(
            "ClickHouse shard %Qv endpoint metadata count does not match its host count",
            shard.Name);
    }

    const auto& first = endpoints.front();
    ValidateTargetEngine(
        first.Engine,
        shard.Database,
        shard.Table,
        shard.Hosts.size() > 1,
        logger);
    for (int endpointIndex = 0; endpointIndex < std::ssize(endpoints); ++endpointIndex) {
        const auto& endpoint = endpoints[endpointIndex];
        if (endpoint.Engine.empty() || endpoint.Columns.empty()) {
            THROW_ERROR_EXCEPTION(
                "ClickHouse endpoint %Qv of shard %Qv returned incomplete table metadata",
                shard.Hosts[endpointIndex],
                shard.Name);
        }
        if (endpoint.Engine != first.Engine || endpoint.Columns != first.Columns) {
            THROW_ERROR_EXCEPTION(
                "ClickHouse endpoint %Qv of shard %Qv exposes different table metadata",
                shard.Hosts[endpointIndex],
                shard.Name);
        }
        if (endpoint.Engine.starts_with("Replicated") && endpoint.Engine.ends_with("MergeTree")) {
            if (!endpoint.ReplicationIdentity ||
                endpoint.ReplicationIdentity->ZookeeperName.empty() ||
                endpoint.ReplicationIdentity->ZookeeperPath.empty() ||
                endpoint.ReplicaName.empty())
            {
                THROW_ERROR_EXCEPTION(
                    "ClickHouse endpoint %Qv of shard %Qv returned incomplete replication identity",
                    shard.Hosts[endpointIndex],
                    shard.Name);
            }
            if (endpoint.ReplicationIdentity != first.ReplicationIdentity) {
                THROW_ERROR_EXCEPTION(
                    "ClickHouse endpoint %Qv of shard %Qv belongs to a different replicated table",
                    shard.Hosts[endpointIndex],
                    shard.Name);
            }
        }
    }
}

void ValidateAndUpdateShardTargetIdentityFingerprints(
    TClickHouseShardTopologyState* state,
    const std::vector<std::string>& currentFingerprints,
    const std::deque<TMessageId>& pendingBatchBounds,
    bool isSharded)
{
    const auto& persisted = state->TargetIdentityFingerprints;
    if (persisted && *persisted == currentFingerprints) {
        return;
    }
    if (!pendingBatchBounds.empty() && (persisted || isSharded)) {
        THROW_ERROR_EXCEPTION(
            "Refusing to start the ClickHouse sink: a shard target identity changed or was "
            "not previously stamped while %v batch(es) are still undelivered; replaying them "
            "against another deduplication log would duplicate rows. Restore endpoints for the "
            "previous logical tables, let the pipeline drain to completion, then apply the change",
            pendingBatchBounds.size())
            .With("persisted_target_identity_fingerprints", persisted.value_or(std::vector<std::string>{}))
            .With("current_target_identity_fingerprints", currentFingerprints)
            .With("oldest_undelivered_batch_bound", pendingBatchBounds.front());
    }
    state->TargetIdentityFingerprints = currentFingerprints;
}

void WarnIfDedupWindowBelowReplayHorizon(
    std::optional<TDuration> dedupWindow,
    TDuration replayHorizon,
    TStringBuf dedupWindowSettingName,
    const std::string& database,
    const std::string& table,
    const NLogging::TLogger& Logger)
{
    if (!dedupWindow) {
        YT_TLOG_WARNING(
            "Could not introspect the block dedup window for the target table; ensure "
            "the configured time-based deduplication window outlives the replay horizon "
            "(see the table-creation guidance), otherwise exactly-once degrades to at-least-once")
            .With("Database", database)
            .With("DedupWindowSetting", dedupWindowSettingName)
            .With("Table", table)
            .With("ReplayHorizon", replayHorizon);
        return;
    }
    if (*dedupWindow < replayHorizon) {
        YT_TLOG_WARNING(
            "Server-default block dedup window is shorter than the replay "
            "horizon; a replay outliving the dedup token degrades exactly-once to at-least-once. "
            "Size the time-based deduplication window per the table-creation guidance "
            "(per-table overrides are not introspectable via the native client)")
            .With("DedupWindow", *dedupWindow)
            .With("DedupWindowSetting", dedupWindowSettingName)
            .With("Database", database)
            .With("Table", table)
            .With("ReplayHorizon", replayHorizon);
    }
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseWriter::TClickHouseWriter(
    TCommonClickHouseSinkParametersPtr parameters,
    TDynamicCommonClickHouseSinkParametersPtr dynamicParameters,
    std::vector<TClickHouseShard> shards,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger,
    std::optional<TClickHouseWriterTestHooks> testHooks)
    : Parameters_(std::move(parameters))
    , DynamicParameters_(std::move(dynamicParameters))
    , Shards_(std::move(shards))
    , ErrorState_(std::move(errorState))
    , Logger(std::move(logger))
    , TestHooks_(std::move(testHooks))
    , Connections_(Shards_.size())
{
    YT_VERIFY(!Shards_.empty());
}

NLogging::TLogger TClickHouseWriter::MakeShardLogger(int shardIndex) const
{
    const auto& shard = Shards_[shardIndex];
    if (shard.Name.empty()) {
        return Logger;
    }
    return Logger.WithTag("Shard", shard.Name);
}

void TClickHouseWriter::SetShardError(
    int shardIndex,
    EClickHouseShardErrorPhase phase,
    TError error)
{
    auto& errors = ActiveShardErrors_[shardIndex];
    auto& destination = phase == EClickHouseShardErrorPhase::Configuration
        ? errors.Configuration
        : errors.Writing;
    destination = std::move(error);
    PublishShardErrors();
}

void TClickHouseWriter::ClearShardError(int shardIndex, EClickHouseShardErrorPhase phase)
{
    auto it = ActiveShardErrors_.find(shardIndex);
    if (it == ActiveShardErrors_.end()) {
        return;
    }
    auto& destination = phase == EClickHouseShardErrorPhase::Configuration
        ? it->second.Configuration
        : it->second.Writing;
    if (!destination) {
        return;
    }
    destination.reset();
    if (!it->second.Configuration && !it->second.Writing) {
        ActiveShardErrors_.erase(it);
    }
    PublishShardErrors();
}

void TClickHouseWriter::PublishShardErrors()
{
    if (ActiveShardErrors_.empty()) {
        ErrorState_->ClearError();
        return;
    }
    auto aggregate = TError("ClickHouse shards have active errors");
    for (const auto& [shardIndex, errors] : ActiveShardErrors_) {
        auto shardError = TError("ClickHouse shard is unhealthy")
            .With("shard_index", shardIndex)
            .With("shard", Shards_[shardIndex].Name);
        if (errors.Configuration) {
            shardError = shardError.With(*errors.Configuration);
        }
        if (errors.Writing) {
            shardError = shardError.With(*errors.Writing);
        }
        aggregate = aggregate.With(shardError);
    }
    ErrorState_->SetError(std::move(aggregate));
}

TClickHouseTargetMetadata TClickHouseWriter::QueryEndpointMetadata(
    int shardIndex,
    const std::string& host,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    if (TestHooks_ && TestHooks_->QueryEndpointMetadata) {
        return TestHooks_->QueryEndpointMetadata(shardIndex, host);
    }

    auto endpointShard = Shards_[shardIndex];
    endpointShard.Hosts = {host};
    auto options = MakeClientOptions(*Parameters_, endpointShard, dynamicParameters->WriteTimeout);
    options.SetConnectionConnectTimeout(MetadataProbeTimeout);
    options.SetConnectionSendTimeout(MetadataProbeTimeout);
    options.SetConnectionRecvTimeout(MetadataProbeTimeout);
    options.SetSendRetries(0);
    options.SetRetryTimeout(std::chrono::seconds(0));
    clickhouse::Client client(options);

    const auto database = QuoteLiteral(endpointShard.Database);
    const auto table = QuoteLiteral(endpointShard.Table);
    auto engines = QueryMetadataRows(client, Format("SELECT engine FROM system.tables WHERE database = %v AND name = %v", database, table), 1);
    THROW_ERROR_EXCEPTION_IF(
        engines.size() != 1 || engines.front().front().empty(),
        "Missing or ambiguous ClickHouse table metadata for host %Qv",
        host);

    TClickHouseTargetMetadata result;
    result.Engine = engines.front().front();
    auto columns = QueryMetadataRows(client, Format("SELECT name, type, default_kind, default_expression FROM system.columns "
        "WHERE database = %v AND table = %v ORDER BY position",
        database,
        table),
        4);
    std::set<std::string> names;
    for (const auto& column : columns) {
        THROW_ERROR_EXCEPTION_IF(
            column[0].empty() || column[1].empty() || !names.insert(column[0]).second,
            "Missing or ambiguous ClickHouse column metadata for host %Qv",
            host);
        result.Columns.push_back({column[0], column[1], column[2], column[3]});
    }
    THROW_ERROR_EXCEPTION_IF(
        result.Columns.empty(),
        "ClickHouse target has no columns on host %Qv",
        host);

    if (result.Engine.starts_with("Replicated") && result.Engine.ends_with("MergeTree")) {
        auto replicas = QueryMetadataRows(client, Format("SELECT zookeeper_name, zookeeper_path, replica_name FROM system.replicas "
            "WHERE database = %v AND table = %v",
            database,
            table),
            3);
        THROW_ERROR_EXCEPTION_IF(
            replicas.size() != 1,
            "Missing or ambiguous ClickHouse replication metadata for host %Qv",
            host);
        result.ReplicationIdentity = TClickHouseReplicationIdentity{replicas[0][0], replicas[0][1]};
        result.ReplicaName = replicas[0][2];
    }
    return result;
}

void TClickHouseWriter::IntrospectEndpoints(
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    std::vector<std::vector<TClickHouseTargetMetadata>> metadata(Shards_.size());
    std::vector<TError> validationErrors;
    std::optional<int> referenceShardIndex;
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        auto& endpoints = metadata[shardIndex];
        endpoints.reserve(Shards_[shardIndex].Hosts.size());
        std::vector<TError> shardErrors;
        for (const auto& host : Shards_[shardIndex].Hosts) {
            try {
                endpoints.push_back(QueryEndpointMetadata(shardIndex, host, dynamicParameters));
            } catch (const std::exception& ex) {
                shardErrors.push_back(
                    TError("Failed to introspect ClickHouse endpoint")
                        .With("configured_host", host)
                        .With(ex));
            }
        }
        if (shardErrors.empty()) {
            try {
                ValidateFailoverMetadata(Shards_[shardIndex], endpoints, MakeShardLogger(shardIndex));
                if (referenceShardIndex) {
                    THROW_ERROR_EXCEPTION_IF(
                        endpoints.front().Columns != metadata[*referenceShardIndex].front().Columns,
                        "ClickHouse shards %Qv and %Qv expose different ordered schemas",
                        Shards_[*referenceShardIndex].Name,
                        Shards_[shardIndex].Name);
                } else {
                    referenceShardIndex = shardIndex;
                }
            } catch (const std::exception& ex) {
                shardErrors.push_back(TError("Failed to validate ClickHouse shard metadata").With(ex));
            }
        }
        if (!shardErrors.empty()) {
            auto shardError = TError("Failed to validate ClickHouse endpoints").With(std::move(shardErrors));
            SetShardError(
                shardIndex,
                EClickHouseShardErrorPhase::Configuration,
                shardError);
            validationErrors.push_back(std::move(shardError));
        }
    }
    if (!validationErrors.empty()) {
        THROW_ERROR_EXCEPTION("Failed to validate configured ClickHouse endpoints")
            .With(std::move(validationErrors));
    }
    EndpointMetadata_ = std::move(metadata);
    TableColumns_ = EndpointMetadata_.front().front().Columns;
}

TFuture<void> TClickHouseWriter::Write(
    EWriteGuarantee guarantee,
    std::vector<TClickHouseShardWrite> shardWrites)
{
    auto promise = NewPromise<void>();
    Queue_.Enqueue(TWriteRequest{
        .Guarantee = guarantee,
        .ShardWrites = std::move(shardWrites),
        .Promise = promise,
    });
    return promise.ToFuture();
}

void TClickHouseWriter::Reconfigure(TDynamicCommonClickHouseSinkParametersPtr dynamicParameters)
{
    DynamicParameters_ = std::move(dynamicParameters);
}

TStringBuf GetClickHouseDedupWindowSettingName(bool asyncInsert)
{
    return asyncInsert
        ? "replicated_deduplication_window_seconds_for_async_inserts"
        : "replicated_deduplication_window_seconds";
}

std::optional<TDuration> TClickHouseWriter::QueryDedupWindow(int shardIndex, bool asyncInsert)
{
    auto query = Format("SELECT value FROM system.merge_tree_settings WHERE name = '%v'",
        GetClickHouseDedupWindowSettingName(asyncInsert));
    std::optional<i64> windowSeconds;
    Connections_[shardIndex].Client->Select(query, [&] (const clickhouse::Block& block) {
        if (block.GetRowCount() == 0) {
            return;
        }
        auto value = std::string(block[0]->As<clickhouse::ColumnString>()->At(0));
        i64 parsed = 0;
        if (TryFromString(value, parsed)) {
            windowSeconds = parsed;
        }
    });
    if (!windowSeconds || *windowSeconds == 0) {
        return std::nullopt;
    }
    return TDuration::Seconds(*windowSeconds);
}

void TClickHouseWriter::ValidateTarget(
    int shardIndex,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    const auto& shard = Shards_[shardIndex];
    auto& connection = Connections_[shardIndex];
    auto shardLogger = MakeShardLogger(shardIndex);
    YT_VERIFY(shardIndex < std::ssize(EndpointMetadata_));
    YT_VERIFY(!EndpointMetadata_[shardIndex].empty());
    const auto& engine = EndpointMetadata_[shardIndex].front().Engine;
    std::optional<TDuration> dedupWindow;
    if (IsBlockDeduplicatingEngine(engine)) {
        dedupWindow = QueryDedupWindow(shardIndex, dynamicParameters->AsyncInsert);
    }
    connection.TargetEngine = engine;
    connection.TargetDedupWindow = dedupWindow;
    if (IsBlockDeduplicatingEngine(*connection.TargetEngine)) {
        WarnIfDedupWindowBelowReplayHorizon(
            connection.TargetDedupWindow,
            dynamicParameters->ReplayHorizon,
            GetClickHouseDedupWindowSettingName(dynamicParameters->AsyncInsert),
            shard.Database,
            shard.Table,
            shardLogger);
    }
}

void TClickHouseWriter::ApplyConnectionDynamicParameters(
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    struct TPreviousConnectionState
    {
        std::optional<std::string> TargetEngine;
        std::optional<TDuration> TargetDedupWindow;
        bool ReconnectBeforeInsert = false;
    };

    const auto previousAppliedParameters = AppliedConnectionDynamicParameters_;
    std::vector<TPreviousConnectionState> previousConnectionStates;
    previousConnectionStates.reserve(Connections_.size());
    for (const auto& connection : Connections_) {
        previousConnectionStates.push_back({
            .TargetEngine = connection.TargetEngine,
            .TargetDedupWindow = connection.TargetDedupWindow,
            .ReconnectBeforeInsert = connection.ReconnectBeforeInsert,
        });
    }

    const bool recreateClient =
        !AppliedConnectionDynamicParameters_ ||
        AppliedConnectionDynamicParameters_->WriteTimeout != dynamicParameters->WriteTimeout;
    const bool replayHorizonChanged =
        !AppliedConnectionDynamicParameters_ ||
        AppliedConnectionDynamicParameters_->ReplayHorizon != dynamicParameters->ReplayHorizon;
    const bool asyncInsertChanged =
        !AppliedConnectionDynamicParameters_ ||
        AppliedConnectionDynamicParameters_->AsyncInsert != dynamicParameters->AsyncInsert;

    try {
        if (EndpointMetadata_.empty() && (!TestHooks_ || TestHooks_->QueryEndpointMetadata)) {
            IntrospectEndpoints(dynamicParameters);
        }

        std::vector<TError> configurationErrors;
        for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
            auto& connection = Connections_[shardIndex];
            try {
                if (TestHooks_) {
                    if (TestHooks_->ApplyConnectionDynamicParameters) {
                        TestHooks_->ApplyConnectionDynamicParameters(shardIndex, dynamicParameters);
                    }
                } else if (recreateClient) {
                    connection.Client = std::make_unique<clickhouse::Client>(
                        MakeClientOptions(*Parameters_, Shards_[shardIndex], dynamicParameters->WriteTimeout));
                    connection.ReconnectBeforeInsert = false;
                    ValidateTarget(shardIndex, dynamicParameters);
                } else if (
                    (replayHorizonChanged || asyncInsertChanged) &&
                    IsBlockDeduplicatingEngine(*connection.TargetEngine))
                {
                    if (asyncInsertChanged) {
                        connection.TargetDedupWindow = QueryDedupWindow(shardIndex, dynamicParameters->AsyncInsert);
                    }
                    WarnIfDedupWindowBelowReplayHorizon(
                        connection.TargetDedupWindow,
                        dynamicParameters->ReplayHorizon,
                        GetClickHouseDedupWindowSettingName(dynamicParameters->AsyncInsert),
                        Shards_[shardIndex].Database,
                        Shards_[shardIndex].Table,
                        MakeShardLogger(shardIndex));
                }
                ClearShardError(shardIndex, EClickHouseShardErrorPhase::Configuration);
            } catch (const std::exception& ex) {
                auto error = TError("Failed to configure ClickHouse connection").With(ex);
                SetShardError(
                    shardIndex,
                    EClickHouseShardErrorPhase::Configuration,
                    error);
                configurationErrors.push_back(std::move(error));
            }
        }
        if (!configurationErrors.empty()) {
            THROW_ERROR_EXCEPTION("Failed to configure ClickHouse connections")
                .With(std::move(configurationErrors));
        }
        AppliedConnectionDynamicParameters_ = dynamicParameters;
    } catch (...) {
        AppliedConnectionDynamicParameters_ = previousAppliedParameters;
        for (int shardIndex = 0; shardIndex < std::ssize(Connections_); ++shardIndex) {
            Connections_[shardIndex].TargetEngine = previousConnectionStates[shardIndex].TargetEngine;
            Connections_[shardIndex].TargetDedupWindow = previousConnectionStates[shardIndex].TargetDedupWindow;
            Connections_[shardIndex].ReconnectBeforeInsert = previousConnectionStates[shardIndex].ReconnectBeforeInsert;
        }
        throw;
    }
}

void TClickHouseWriter::Connect()
{
    auto dynamicParameters = DynamicParameters_.Acquire();
    ApplyConnectionDynamicParameters(dynamicParameters);
}

const std::vector<TClickHouseShard>& TClickHouseWriter::GetShards() const
{
    return Shards_;
}

std::vector<std::string> TClickHouseWriter::GetShardTargetIdentityFingerprints() const
{
    YT_VERIFY(EndpointMetadata_.size() == Shards_.size());
    std::vector<std::string> result;
    result.reserve(Shards_.size());
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        YT_VERIFY(!EndpointMetadata_[shardIndex].empty());
        result.push_back(BuildShardTargetIdentityFingerprint(
            Shards_[shardIndex],
            EndpointMetadata_[shardIndex].front().ReplicationIdentity));
    }
    return result;
}

const std::vector<TClickHouseTableColumn>& TClickHouseWriter::GetTableColumns() const
{
    return TableColumns_;
}

void TClickHouseWriter::SetResolvedColumns(std::vector<TResolvedColumn> columns)
{
    ResolvedColumns_ = std::move(columns);
}

void TClickHouseWriter::ResetConnection(int shardIndex)
{
    if (TestHooks_) {
        TestHooks_->ResetConnection(shardIndex);
    } else {
        // #Client::ResetConnection() reconnects to the same endpoint; failover to the next
        // replica of the shard requires #ResetConnectionEndpoint() instead.
        Connections_[shardIndex].Client->ResetConnectionEndpoint();
    }
}

void TClickHouseWriter::Insert(const TWriteRequest& request, int shardWriteIndex, bool asyncInsert)
{
    const auto& shardWrite = request.ShardWrites[shardWriteIndex];
    if (TestHooks_) {
        TestHooks_->Insert(shardWrite.ShardIndex, asyncInsert);
        return;
    }
    const auto& shard = Shards_[shardWrite.ShardIndex];
    auto header = BuildInsertHeader(
        shard.Database,
        shard.Table,
        ResolvedColumns_,
        asyncInsert,
        shardWrite.DedupToken);
    auto& client = Connections_[shardWrite.ShardIndex].Client;
    client->BeginInsert(header);
    client->SendInsertBlock(shardWrite.Block);
    client->EndInsert();
}

void TClickHouseWriter::Run(TWeakPtr<TRefCounted> owner)
{
    std::optional<TError> fatalError;
    while (owner.Lock()) {
        TWriteRequest request;
        try {
            request = WaitFor(Queue_.Dequeue()).ValueOrThrow();
        } catch (const std::exception&) {
            continue;
        }

        if (fatalError) {
            request.Promise.TrySet(*fatalError);
            continue;
        }

        const auto dynamicParameters = DynamicParameters_.Acquire();
        const auto requestOptions = MakeClickHouseRequestOptions(*dynamicParameters);
        bool configured = false;
        while (!configured && owner.Lock()) {
            try {
                ApplyConnectionDynamicParameters(dynamicParameters);
                configured = true;
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Failed to configure ClickHouse connection")
                    .With(ex);
                TDelayedExecutor::WaitForDuration(dynamicParameters->RetryBackoff);
            }
        }
        if (!configured) {
            request.Promise.TrySet(TError("ClickHouse writer stopped during configuration"));
            continue;
        }

        TClickHouseAttemptState attemptState;
        while (request.NextShardIndex < std::ssize(request.ShardWrites) && owner.Lock()) {
            const int shardIndex = request.ShardWrites[request.NextShardIndex].ShardIndex;
            try {
                if (Connections_[shardIndex].ReconnectBeforeInsert) {
                    ResetConnection(shardIndex);
                    Connections_[shardIndex].ReconnectBeforeInsert = false;
                }

                Insert(request, request.NextShardIndex, requestOptions.AsyncInsert);
                ClearShardError(shardIndex, EClickHouseShardErrorPhase::Writing);
                ++request.NextShardIndex;
                attemptState = {};
                continue;
            } catch (const std::exception& ex) {
                Connections_[shardIndex].ReconnectBeforeInsert = true;
                static constexpr auto Message = "Failed to insert batch into ClickHouse"_sb;
                auto error = TError(Message)
                    .With(ex);
                YT_TLOG_WARNING(Message)
                    .With(ex);
                SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, error);

                auto errorKind = ClassifyClickHouseError(ex);
                auto action = GetClickHouseFailureAction(
                    request.Guarantee,
                    /*insertStarted*/ true,
                    errorKind,
                    requestOptions,
                    &attemptState);
                if (action == EClickHouseFailureAction::Acknowledge) {
                    ++request.NextShardIndex;
                    attemptState = {};
                    continue;
                }
                if (action == EClickHouseFailureAction::Fail) {
                    static constexpr auto TerminalMessage = "Giving up insert into ClickHouse"_sb;
                    auto terminalError = TError(TerminalMessage)
                        .With("error_kind", errorKind)
                        .With(error);
                    YT_TLOG_ERROR(TerminalMessage)
                        .With("ErrorKind", errorKind)
                        .With(error);
                    SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, terminalError);
                    request.Promise.TrySet(terminalError);
                    if (request.Guarantee == EWriteGuarantee::ExactlyOnce) {
                        fatalError = terminalError;
                    }
                    break;
                }
                TDelayedExecutor::WaitForDuration(dynamicParameters->RetryBackoff);
            }
        }
        if (request.NextShardIndex >= std::ssize(request.ShardWrites)) {
            request.Promise.TrySet();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCommonClickHouseSink::TWriterSession
    : public TRefCounted
{
public:
    TWriterSession(
        TCommonClickHouseSinkParametersPtr parameters,
        TDynamicCommonClickHouseSinkParametersPtr dynamicParameters,
        std::vector<TClickHouseShard> shards,
        IStatusErrorStatePtr errorState,
        NLogging::TLogger logger)
        : ActionQueue(New<TActionQueue>("ClickHouseWriter"))
        , Writer(New<TClickHouseWriter>(
            std::move(parameters),
            std::move(dynamicParameters),
            std::move(shards),
            std::move(errorState),
            std::move(logger)))
    { }

    ~TWriterSession() override
    {
        if (Executor) {
            Executor.Cancel(TError("ClickHouse sink closed"));
        }
    }

    const TActionQueuePtr ActionQueue;
    const TClickHouseWriterPtr Writer;
    TFuture<void> Executor;
};

////////////////////////////////////////////////////////////////////////////////

TCommonClickHouseSink::TCommonClickHouseSink(
    TCommonClickHouseSinkParametersPtr parameters,
    TDynamicCommonClickHouseSinkParametersPtr dynamicParameters,
    IStatusProfilerPtr statusProfiler,
    std::vector<NTableClient::TTableSchemaPtr> streamSchemas,
    NLogging::TLogger logger)
    : Logger(MakeSinkLogger(logger, *parameters))
    , Parameters_(std::move(parameters))
    , DynamicParameters_(std::move(dynamicParameters))
    , ErrorState_(statusProfiler->ErrorState("/writing"))
    , StreamSchemas_(std::move(streamSchemas))
{
    // Validated here rather than at session startup: #EnsureSessionStartedWithRetry() retries
    // a failed startup forever, so a bad spec would never surface as a failure there.
    ValidateShardingKeyColumns(Parameters_->ShardingKeyColumns, StreamSchemas_);
}

// Defined out of line because #TWriterSession is incomplete in the header.
TCommonClickHouseSink::~TCommonClickHouseSink() = default;

TError TCommonClickHouseSink::TryStartSession()
{
    try {
        auto shards = ResolveShards(*Parameters_);

        std::vector<std::string> shardNames;
        for (const auto& shard : shards) {
            shardNames.push_back(shard.Name);
        }
        YT_TLOG_INFO("ClickHouse sink topology resolved")
            .With("Shards", shardNames)
            .With("ShardingKeyColumns", Parameters_->ShardingKeyColumns);

        // Connecting is deferred until the first write so #Init() cannot delay the first partition status.
        auto session = New<TWriterSession>(
            Parameters_,
            DynamicParameters_.Acquire(),
            std::move(shards),
            ErrorState_,
            Logger);

        WaitFor(BIND(&TClickHouseWriter::Connect, session->Writer)
                .AsyncVia(session->ActionQueue->GetInvoker())
                .Run())
            .ThrowOnError();

        std::vector<TResolvedColumn> resolved;
        std::vector<std::string> resolvedNames;
        for (const auto& schema : StreamSchemas_) {
            auto columns = ResolveColumns(session->Writer->GetTableColumns(), schema);
            std::vector<std::string> names;
            for (const auto& column : columns) {
                names.push_back(column.Name);
            }
            if (resolved.empty()) {
                resolved = std::move(columns);
                resolvedNames = std::move(names);
            } else if (names != resolvedNames) {
                THROW_ERROR_EXCEPTION("Input streams resolve to different ClickHouse column sets");
            }
        }
        session->Writer->SetResolvedColumns(resolved);
        BlockBuilder_.emplace(std::move(resolved));
        Router_.emplace(
            session->Writer->GetShards(),
            Parameters_->ShardingKeyColumns,
            StreamSchemas_);

        // #Connect() yields while the new session is not published, so reapply the latest parameters.
        session->Writer->Reconfigure(DynamicParameters_.Acquire());

        TWeakPtr<TRefCounted> weakSession = session;
        session->Executor = BIND(&TClickHouseWriter::Run, session->Writer, std::move(weakSession))
            .AsyncVia(session->ActionQueue->GetInvoker())
            .Run();
        Session_ = std::move(session);
        return {};
    } catch (const std::exception& ex) {
        return TError("Failed to initialize ClickHouse writer").With(ex);
    }
}

void TCommonClickHouseSink::EnsureSessionStarted()
{
    if (Session_) {
        return;
    }

    auto error = TryStartSession();
    if (!error.IsOK()) {
        ErrorState_->SetError(error);
        error.ThrowOnError();
    }
    ErrorState_->ClearError();
}

void TCommonClickHouseSink::EnsureSessionStartedWithRetry()
{
    if (Session_) {
        return;
    }

    while (true) {
        auto error = TryStartSession();
        if (error.IsOK()) {
            ErrorState_->ClearError();
            return;
        }
        ErrorState_->SetError(error);
        // Fiber cancellation is not a std::exception and must escape this retry loop.
        TDelayedExecutor::WaitForDuration(DynamicParameters_.Acquire()->RetryBackoff);
    }
}

TIntrusivePtr<TRefCounted> TCommonClickHouseSink::GetSessionLifetime() const
{
    return Session_;
}

std::vector<std::string> TCommonClickHouseSink::GetShardTargetIdentityFingerprints() const
{
    YT_VERIFY(Session_);
    return Session_->Writer->GetShardTargetIdentityFingerprints();
}

TFuture<void> TCommonClickHouseSink::WriteMessages(
    EWriteGuarantee guarantee,
    const std::optional<std::string>& batchDedupToken,
    const std::vector<TOutputMessageConstPtr>& messages)
{
    return Session_->Writer->Write(
        guarantee,
        BuildClickHouseShardWrites(*Router_, *BlockBuilder_, batchDedupToken, messages));
}

void TCommonClickHouseSink::Reconfigure(TDynamicCommonClickHouseSinkParametersPtr dynamicParameters)
{
    DynamicParameters_ = dynamicParameters;
    if (Session_) {
        Session_->Writer->Reconfigure(std::move(dynamicParameters));
    }
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseBatchingSinkBase::TClickHouseBatchingSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonClickHouseSink(
        GetParameters(),
        GetDynamicParameters(),
        GetContext()->StatusProfiler,
        CollectInputStreamSchemas(GetContext(), GetSpec()),
        TOrderedBatchingAsyncSinkBase::Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& /*dynamicContext*/) {
        TCommonClickHouseSink::Reconfigure(GetDynamicParameters());
    }));
}

void TClickHouseBatchingSinkBase::Init(IInitContextPtr initContext)
{
    TOrderedBatchingAsyncSinkBase::Init(initContext);
    initContext->WithPrefix("shard_topology")
        ->InitClient<TClickHouseShardTopologyState>(TopologyState_, "v0");
    ValidateShardTopologyUnchanged();
}

void TClickHouseBatchingSinkBase::ValidateShardTopologyUnchanged()
{
    auto fingerprint = BuildShardTopologyFingerprint(
        ResolveShards(*GetParameters()),
        GetParameters()->ShardingKeyColumns);
    const auto& persisted = TopologyState_->TopologyFingerprint;
    if (persisted && *persisted == fingerprint) {
        return;
    }
    const auto pending = GetPendingBatchBoundsSnapshot();
    // An unstamped state is a partition that never ran a sharding-aware binary. Letting it
    // through unconditionally would allow the very rollout that introduces |shard_hosts| to
    // replay unsuffixed-token batches under suffixed tokens, so only an unsharded-to-unsharded
    // first start may pass undrained.
    if (!pending.empty() && (persisted || fingerprint != UnshardedTopologyFingerprint)) {
        THROW_ERROR_EXCEPTION(
            "Refusing to start the ClickHouse sink: the shard topology changed while %v "
            "batch(es) are still undelivered; replaying them under the new topology would "
            "duplicate or drop rows. Restore the previous shard_hosts / sharding_key_columns, "
            "let the pipeline drain to completion, then apply the change",
            pending.size())
            .With("persisted_topology_fingerprint", persisted.value_or("<unstamped>"))
            .With("spec_topology_fingerprint", fingerprint)
            .With("oldest_undelivered_batch_bound", pending.front());
    }
    TopologyState_->TopologyFingerprint = fingerprint;
}

void TClickHouseBatchingSinkBase::ValidateShardTargetIdentityUnchanged()
{
    if (TargetIdentityValidated_) {
        return;
    }
    ValidateAndUpdateShardTargetIdentityFingerprints(
        TopologyState_.Get(),
        GetShardTargetIdentityFingerprints(),
        GetPendingBatchBoundsSnapshot(),
        !GetParameters()->ShardHosts.empty());
    TargetIdentityValidated_ = true;
}

void TClickHouseBatchingSinkBase::DoInit(const std::string& /*producerId*/)
{ }

void TClickHouseBatchingSinkBase::Distribute(
    const TOutputMessageConstPtr& message,
    TOnDistributedCallback onDistributed)
{
    EnsureSessionStarted();
    ValidateShardTargetIdentityUnchanged();
    TOrderedBatchingAsyncSinkBase::Distribute(message, std::move(onDistributed));
}

TFuture<void> TClickHouseBatchingSinkBase::DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 /*seqNo*/)
{
    if (messages.empty()) {
        return OKFuture;
    }
    return WriteMessages(EWriteGuarantee::ExactlyOnce, BuildDedupToken(messages), messages);
}

////////////////////////////////////////////////////////////////////////////////

TAtLeastOnceClickHouseSink::TAtLeastOnceClickHouseSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonClickHouseSink(
        GetParameters(),
        GetDynamicParameters(),
        GetContext()->StatusProfiler,
        CollectInputStreamSchemas(GetContext(), GetSpec()),
        TSyncSinkBase::Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& /*dynamicContext*/) {
        TCommonClickHouseSink::Reconfigure(GetDynamicParameters());
    }));
}

void TAtLeastOnceClickHouseSink::DoInit()
{ }

void TAtLeastOnceClickHouseSink::DoDistribute(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::deque<TOutputMessageConstPtr>& messages)
{
    if (messages.empty()) {
        return;
    }
    EnsureSessionStarted();
    std::vector<TOutputMessageConstPtr> batch(messages.begin(), messages.end());
    WaitFor(WriteMessages(EWriteGuarantee::AtLeastOnce, std::nullopt, batch))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TAtMostOnceClickHouseSink::TAtMostOnceClickHouseSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TDelegatingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , TCommonClickHouseSink(
        GetParameters(),
        GetDynamicParameters(),
        GetContext()->StatusProfiler,
        CollectInputStreamSchemas(GetContext(), GetSpec()),
        TDelegatingAsyncSinkBase::Logger)
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& /*dynamicContext*/) {
        TCommonClickHouseSink::Reconfigure(GetDynamicParameters());
    }));
}

TAtMostOnceClickHouseSink::~TAtMostOnceClickHouseSink()
{
    if (auto session = GetSessionLifetime()) {
        SuspendDestructionGuarded({std::move(session)});
    }
}

bool TAtMostOnceClickHouseSink::IsAtMostOnceStrategyEnabled() const
{
    const auto& strategy = GetParameters()->AtMostOnceStrategy;
    return strategy && strategy->Enabled;
}

void TAtMostOnceClickHouseSink::Distribute(
    const TOutputMessageConstPtr& message,
    TOnDistributedCallback onDistributed)
{
    if (IsAtMostOnceStrategyEnabled()) {
        EnsureSessionStartedWithRetry();
    } else {
        EnsureSessionStarted();
    }
    TDelegatingAsyncSinkBase::Distribute(message, std::move(onDistributed));
}

void TAtMostOnceClickHouseSink::DoInit(const std::string& /*producerId*/)
{ }

std::pair<TFuture<void>, ui64> TAtMostOnceClickHouseSink::DoDistribute(const TOutputMessageConstPtr& message, i64 /*seqNo*/)
{
    auto future = WriteMessages(EWriteGuarantee::AtMostOnce, std::nullopt, {message});
    return {std::move(future), message->ByteSize};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TClickHouseSinkController::GetReceiverChannelCount()
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
