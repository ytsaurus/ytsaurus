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

#include <netdb.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto MetadataProbeTimeout = TDuration::Seconds(5);

constexpr int UnknownTableErrorCode = 60;
constexpr int UnknownDatabaseErrorCode = 81;
constexpr int UnknownUserErrorCode = 192;
constexpr int WrongPasswordErrorCode = 193;
constexpr int RequiredPasswordErrorCode = 194;
constexpr int IpAddressNotAllowedErrorCode = 195;
constexpr int AccessDeniedErrorCode = 497;
constexpr int AuthenticationFailedErrorCode = 516;

constexpr TStringBuf CertificateVerificationFailureMessage = "Failed to verify SSL connection";
constexpr TStringBuf PeerClosedConnectionMessage = "closed";
constexpr TStringBuf GetaddrinfoErrorCategoryName = "getaddrinfoError";

bool IsDefinitiveClickHouseServerErrorCode(int code)
{
    switch (code) {
        case UnknownTableErrorCode:
        case UnknownDatabaseErrorCode:
        case UnknownUserErrorCode:
        case WrongPasswordErrorCode:
        case RequiredPasswordErrorCode:
        case IpAddressNotAllowedErrorCode:
        case AccessDeniedErrorCode:
        case AuthenticationFailedErrorCode:
            return true;

        default:
            return false;
    }
}

bool IsClickHouseTransportUnavailability(const std::system_error& error)
{
    if (TStringBuf(error.what()).starts_with(PeerClosedConnectionMessage)) {
        return false;
    }
    const auto& category = error.code().category();
    const int value = error.code().value();
    if (category == std::system_category() || category == std::generic_category()) {
        switch (value) {
            case ECONNREFUSED:
            case ECONNRESET:
            case ECONNABORTED:
            case ETIMEDOUT:
            case EHOSTUNREACH:
            case EHOSTDOWN:
            case ENETUNREACH:
            case ENETDOWN:
            case ENOTCONN:
            case EPIPE:
            case EAGAIN:
                return true;

            default:
                return false;
        }
    }
    if (TStringBuf(category.name()) == GetaddrinfoErrorCategoryName) {
        return value == EAI_AGAIN || value == EAI_NONAME;
    }
    return false;
}

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

class TNativeClickHouseConnection
    : public IClickHouseConnection
{
public:
    TNativeClickHouseConnection(const clickhouse::ClientOptions& options, const TClickHouseShard& shard)
        : Client_(options)
        , Host_(options.host)
        , Database_(shard.Database)
        , Table_(shard.Table)
    { }

    TClickHouseTargetMetadata QueryTargetMetadata() override
    {
        const auto database = QuoteLiteral(Database_);
        const auto table = QuoteLiteral(Table_);
        auto engines = QueryMetadataRows(Client_, Format("SELECT engine FROM system.tables WHERE database = %v AND name = %v", database, table), 1);
        if (engines.size() != 1 || engines.front().front().empty()) {
            ThrowIncompleteMetadata("Missing or ambiguous ClickHouse table metadata");
        }

        TClickHouseTargetMetadata result;
        result.Engine = engines.front().front();
        auto columns = QueryMetadataRows(Client_, Format("SELECT name, type, default_kind, default_expression FROM system.columns "
            "WHERE database = %v AND table = %v ORDER BY position",
            database,
            table),
            4);
        std::set<std::string> names;
        for (const auto& column : columns) {
            if (column[0].empty() || column[1].empty() || !names.insert(column[0]).second) {
                ThrowIncompleteMetadata("Missing or ambiguous ClickHouse column metadata");
            }
            result.Columns.push_back({column[0], column[1], column[2], column[3]});
        }
        if (result.Columns.empty()) {
            ThrowIncompleteMetadata("ClickHouse target has no columns");
        }

        if (result.Engine.starts_with("Replicated") && result.Engine.ends_with("MergeTree")) {
            auto replicas = QueryMetadataRows(Client_, Format("SELECT zookeeper_name, zookeeper_path, replica_name FROM system.replicas "
                "WHERE database = %v AND table = %v",
                database,
                table),
                3);
            if (replicas.size() != 1) {
                ThrowIncompleteMetadata("Missing or ambiguous ClickHouse replication metadata");
            }
            result.ReplicationIdentity = TClickHouseReplicationIdentity{replicas[0][0], replicas[0][1]};
            result.ReplicaName = replicas[0][2];
        }
        return result;
    }

    std::optional<TDuration> QueryDedupWindow(bool asyncInsert) override
    {
        auto query = Format("SELECT value FROM system.merge_tree_settings WHERE name = '%v'",
            GetClickHouseDedupWindowSettingName(asyncInsert));
        std::optional<i64> windowSeconds;
        Client_.Select(query, [&] (const clickhouse::Block& block) {
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

    void Insert(const TClickHouseShardWrite& shardWrite, bool asyncInsert, bool useInputFunction) override
    {
        auto header = BuildInsertHeader(
            Database_,
            Table_,
            shardWrite.Block,
            asyncInsert,
            shardWrite.DedupToken,
            useInputFunction);
        Client_.BeginInsert(header);
        Client_.SendInsertBlock(shardWrite.Block);
        Client_.EndInsert();
    }

private:
    clickhouse::Client Client_;
    const std::string Host_;
    const std::string Database_;
    const std::string Table_;

    [[noreturn]] void ThrowIncompleteMetadata(TStringBuf message) const
    {
        throw TClickHouseAdmissionException(
            EClickHouseAdmissionFailureKind::Incompatible,
            TError("%v for host %Qv", message, Host_));
    }
};

NTableClient::TTableSchemaPtr GetInputStreamSchema(
    const TSinkContextPtr& context,
    const TSinkSpecPtr& spec)
{
    return context->StreamSpecStorage->GetSchema(*spec->InputStreamIds.begin());
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

int SelectClickHouseCandidateStart(
    EClickHouseHostSelectionPolicy policy,
    int hostCount,
    int randomOffset)
{
    YT_VERIFY(hostCount > 0);

    switch (policy) {
        case EClickHouseHostSelectionPolicy::OrderedRoundRobin:
            return 0;

        case EClickHouseHostSelectionPolicy::RandomStart:
            YT_VERIFY(randomOffset >= 0 && randomOffset < hostCount);

            return randomOffset;
    }
    YT_ABORT();
}

clickhouse::ClientOptions MakeClientOptions(
    const TCommonClickHouseSinkParameters& parameters,
    const TClickHouseShard& shard,
    const std::string& host,
    TDuration socketTimeout)
{
    auto timeout = std::chrono::milliseconds(socketTimeout.MilliSeconds());
    auto options = clickhouse::ClientOptions()
        .SetHost(host)
        .SetPort(shard.Port)
        .SetUser(parameters.User)
        .SetDefaultDatabase(shard.Database)
        .SetCompressionMethod(ToCompressionMethod(parameters.Codec))
        .SetConnectionSendTimeout(timeout)
        .SetConnectionRecvTimeout(timeout);
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

EClickHouseAdmissionFailureKind ClassifyClickHouseAdmissionError(const std::exception& ex)
{
    if (const auto* admissionError = dynamic_cast<const TClickHouseAdmissionException*>(&ex)) {
        return admissionError->GetKind();
    }
    if (const auto* serverError = dynamic_cast<const clickhouse::ServerException*>(&ex)) {
        return IsDefinitiveClickHouseServerErrorCode(serverError->GetCode())
            ? EClickHouseAdmissionFailureKind::Incompatible
            : EClickHouseAdmissionFailureKind::Indeterminate;
    }
    if (dynamic_cast<const clickhouse::OpenSSLError*>(&ex)) {
        return TStringBuf(ex.what()).Contains(CertificateVerificationFailureMessage)
            ? EClickHouseAdmissionFailureKind::Incompatible
            : EClickHouseAdmissionFailureKind::Indeterminate;
    }
    if (const auto* systemError = dynamic_cast<const std::system_error*>(&ex)) {
        return IsClickHouseTransportUnavailability(*systemError)
            ? EClickHouseAdmissionFailureKind::Unavailable
            : EClickHouseAdmissionFailureKind::Indeterminate;
    }
    return EClickHouseAdmissionFailureKind::Indeterminate;
}

TClickHouseAdmissionException::TClickHouseAdmissionException(EClickHouseAdmissionFailureKind kind, TError error)
    : Kind_(kind)
{
    Error() = std::move(error);
}

EClickHouseAdmissionFailureKind TClickHouseAdmissionException::GetKind() const
{
    return Kind_;
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
    EClickHouseErrorKind errorKind,
    const TClickHouseRequestOptions& requestOptions,
    TClickHouseAttemptState* attemptState)
{
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
    const clickhouse::Block& block,
    bool asyncInsert,
    const std::optional<std::string>& dedupToken,
    bool useInputFunction)
{
    TStringBuilder builder;
    builder.AppendFormat("INSERT INTO %v.%v (",
        QuoteIdentifier(database),
        QuoteIdentifier(table));
    for (const auto& column : block) {
        if (column.ColumnIndex() > 0) {
            builder.AppendChar(',');
        }
        builder.AppendString(QuoteIdentifier(column.Name()));
    }
    builder.AppendChar(')');

    std::vector<std::string> settings;
    if (dedupToken) {
        settings.push_back("insert_deduplication_token=" + QuoteLiteral(*dedupToken));
    }
    if (useInputFunction) {
        settings.push_back("async_insert=0");
    } else if (asyncInsert) {
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

    if (useInputFunction) {
        TStringBuilder structure;
        for (const auto& column : block) {
            if (column.ColumnIndex() > 0) {
                structure.AppendChar(',');
            }
            structure.AppendFormat("%v %v", QuoteIdentifier(column.Name()), column.Type()->GetName());
        }
        builder.AppendFormat(" SELECT * FROM input(%v) FORMAT Native", QuoteLiteral(structure.Flush()));
    } else {
        builder.AppendString(" VALUES");
    }

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

void ValidateClickHouseEndpointMetadata(
    const TClickHouseShard& shard,
    const std::string& host,
    const TClickHouseTargetMetadata& endpoint,
    const TClickHouseTargetMetadata* reference,
    const NLogging::TLogger& logger)
{
    if (endpoint.Engine.empty() || endpoint.Columns.empty()) {
        THROW_ERROR_EXCEPTION("ClickHouse endpoint %Qv of shard %Qv returned incomplete table metadata",
            host,
            shard.Name);
    }
    if (endpoint.Engine.starts_with("Replicated") && endpoint.Engine.ends_with("MergeTree")) {
        if (!endpoint.ReplicationIdentity ||
            endpoint.ReplicationIdentity->ZookeeperName.empty() ||
            endpoint.ReplicationIdentity->ZookeeperPath.empty() ||
            endpoint.ReplicaName.empty())
        {
            THROW_ERROR_EXCEPTION("ClickHouse endpoint %Qv of shard %Qv returned incomplete replication identity",
                host,
                shard.Name);
        }
    }
    if (!reference) {
        ValidateTargetEngine(
            endpoint.Engine,
            shard.Database,
            shard.Table,
            shard.Hosts.size() > 1,
            logger);
        return;
    }
    if (endpoint.Engine != reference->Engine || endpoint.Columns != reference->Columns) {
        THROW_ERROR_EXCEPTION("ClickHouse endpoint %Qv of shard %Qv exposes different table metadata",
            host,
            shard.Name)
            .With("engine", endpoint.Engine)
            .With("reference_engine", reference->Engine);
    }
    if (endpoint.ReplicationIdentity != reference->ReplicationIdentity) {
        THROW_ERROR_EXCEPTION("ClickHouse endpoint %Qv of shard %Qv belongs to a different replicated table",
            host,
            shard.Name);
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
    , CandidateTraversals_(Shards_.size())
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

bool TClickHouseWriter::UsesAsyncInsert(const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters) const
{
    return dynamicParameters->AsyncInsert && !RequiresInputFunction_;
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

std::unique_ptr<IClickHouseConnection> TClickHouseWriter::CreateConnection(
    int shardIndex,
    int hostIndex,
    EClickHouseConnectionPurpose purpose,
    TDuration writeTimeout)
{
    const auto& shard = Shards_[shardIndex];
    const auto& host = shard.Hosts[hostIndex];
    const auto socketTimeout = purpose == EClickHouseConnectionPurpose::Write
        ? writeTimeout
        : MetadataProbeTimeout;
    if (TestHooks_) {
        return TestHooks_->CreateConnection(shardIndex, host, purpose, socketTimeout);
    }

    auto options = MakeClientOptions(*Parameters_, shard, host, socketTimeout);
    if (purpose == EClickHouseConnectionPurpose::Write) {
        return std::make_unique<TNativeClickHouseConnection>(options, shard);
    }
    options.SetConnectionConnectTimeout(std::chrono::milliseconds(MetadataProbeTimeout.MilliSeconds()));
    options.SetSendRetries(0);
    options.SetRetryTimeout(std::chrono::seconds(0));
    return std::make_unique<TNativeClickHouseConnection>(options, shard);
}

TError TClickHouseWriter::MakeEndpointError(
    TError error,
    int shardIndex,
    int hostIndex,
    EClickHouseAdmissionPhase phase,
    EClickHouseAdmissionFailureKind kind,
    const std::exception& ex) const
{
    const auto& shard = Shards_[shardIndex];
    return std::move(error)
        .With("shard", shard.Name)
        .With("shard_index", shardIndex)
        .With("configured_host", shard.Hosts[hostIndex])
        .With("database", shard.Database)
        .With("table", shard.Table)
        .With("phase", phase)
        .With("failure_kind", kind)
        .With(ex);
}

void TClickHouseWriter::IntrospectEndpoints()
{
    struct TDeferredEndpoint
    {
        int ShardIndex;
        TError Error;
    };

    std::vector<TClickHouseTargetMetadata> references(Shards_.size());
    std::vector<TDeferredEndpoint> deferredEndpoints;
    std::vector<TError> validationErrors;
    auto worstFailureKind = EClickHouseAdmissionFailureKind::Unavailable;
    std::optional<int> referenceShardIndex;
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        const auto& shard = Shards_[shardIndex];
        const auto shardLogger = MakeShardLogger(shardIndex);
        auto& reference = references[shardIndex];
        std::vector<TError> unavailableErrors;
        std::vector<TError> rejectionErrors;
        auto shardFailureKind = EClickHouseAdmissionFailureKind::Unavailable;
        for (int hostIndex = 0; hostIndex < std::ssize(shard.Hosts); ++hostIndex) {
            const auto& host = shard.Hosts[hostIndex];
            auto phase = EClickHouseAdmissionPhase::Connect;
            try {
                auto connection = CreateConnection(shardIndex, hostIndex, EClickHouseConnectionPurpose::Probe, /*writeTimeout*/ {});
                phase = EClickHouseAdmissionPhase::QueryMetadata;
                auto metadata = connection->QueryTargetMetadata();
                phase = EClickHouseAdmissionPhase::Validate;
                ValidateClickHouseEndpointMetadata(
                    shard,
                    host,
                    metadata,
                    reference.Engine.empty() ? nullptr : &reference,
                    shardLogger);
                if (reference.Engine.empty()) {
                    reference = std::move(metadata);
                }
            } catch (const std::exception& ex) {
                const auto kind = phase == EClickHouseAdmissionPhase::Validate
                    ? EClickHouseAdmissionFailureKind::Incompatible
                    : ClassifyClickHouseAdmissionError(ex);
                auto error = MakeEndpointError(TError("Failed to introspect ClickHouse endpoint"), shardIndex, hostIndex, phase, kind, ex);
                if (kind == EClickHouseAdmissionFailureKind::Unavailable) {
                    unavailableErrors.push_back(std::move(error));
                } else {
                    shardFailureKind = std::max(shardFailureKind, kind);
                    rejectionErrors.push_back(std::move(error));
                }
            }
        }
        if (!reference.Engine.empty()) {
            if (!referenceShardIndex) {
                referenceShardIndex = shardIndex;
            } else if (reference.Columns != references[*referenceShardIndex].Columns) {
                shardFailureKind = EClickHouseAdmissionFailureKind::Incompatible;
                rejectionErrors.push_back(TError(
                    "ClickHouse shards %Qv and %Qv expose different ordered schemas",
                    Shards_[*referenceShardIndex].Name,
                    shard.Name));
            }
        }

        std::optional<TError> shardError;
        if (!rejectionErrors.empty()) {
            rejectionErrors.insert(rejectionErrors.end(), unavailableErrors.begin(), unavailableErrors.end());
            shardError = TError("Failed to validate ClickHouse endpoints").With(std::move(rejectionErrors));
        } else if (reference.Engine.empty()) {
            shardError = TError("No available ClickHouse replica in shard %Qv", shard.Name)
                .With(std::move(unavailableErrors));
        } else {
            for (auto& error : unavailableErrors) {
                deferredEndpoints.push_back({shardIndex, std::move(error)});
            }
        }
        if (shardError) {
            worstFailureKind = std::max(worstFailureKind, shardFailureKind);
            SetShardError(shardIndex, EClickHouseShardErrorPhase::Configuration, *shardError);
            validationErrors.push_back(std::move(*shardError));
        }
    }
    if (!validationErrors.empty()) {
        throw TClickHouseAdmissionException(
            worstFailureKind,
            TError("Failed to validate configured ClickHouse endpoints").With(std::move(validationErrors)));
    }

    ShardReferences_ = std::move(references);
    for (const auto& deferred : deferredEndpoints) {
        YT_TLOG_WARNING("ClickHouse replica is unavailable; the shard starts with its remaining replicas")
            .With("Shard", Shards_[deferred.ShardIndex].Name)
            .With(deferred.Error);
    }
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        ClearShardError(shardIndex, EClickHouseShardErrorPhase::Configuration);
    }
}

int TClickHouseWriter::SelectPolicyCandidateStart(int shardIndex) const
{
    const int hostCount = std::ssize(Shards_[shardIndex].Hosts);
    int randomOffset = 0;
    if (Parameters_->HostSelectionPolicy == EClickHouseHostSelectionPolicy::RandomStart && hostCount > 1) {
        randomOffset = TestHooks_ && TestHooks_->SelectRandomStartOffset
            ? TestHooks_->SelectRandomStartOffset(hostCount)
            : static_cast<int>(RandomNumber<ui64>(hostCount));
    }
    return SelectClickHouseCandidateStart(Parameters_->HostSelectionPolicy, hostCount, randomOffset);
}

int TClickHouseWriter::SelectFailoverCandidateStart(int shardIndex) const
{
    const auto& hostIndex = Connections_[shardIndex].HostIndex;
    if (!hostIndex) {
        return SelectPolicyCandidateStart(shardIndex);
    }
    return (*hostIndex + 1) % std::ssize(Shards_[shardIndex].Hosts);
}

void TClickHouseWriter::BeginCandidateSequence(int shardIndex, bool failover)
{
    auto& traversal = CandidateTraversals_[shardIndex];
    if (traversal.InSequence) {
        return;
    }
    traversal = TCandidateTraversal{
        .InSequence = true,
        .NextHostIndex = failover
            ? SelectFailoverCandidateStart(shardIndex)
            : SelectPolicyCandidateStart(shardIndex),
    };
}

void TClickHouseWriter::EndCandidateSequence(int shardIndex)
{
    CandidateTraversals_[shardIndex] = {};
}

void TClickHouseWriter::ResetCandidateSequences()
{
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        EndCandidateSequence(shardIndex);
    }
}

TClickHouseWriter::TShardConnection TClickHouseWriter::AdmitCandidate(
    int shardIndex,
    int hostIndex,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    const auto& shard = Shards_[shardIndex];
    const auto shardLogger = MakeShardLogger(shardIndex);
    const bool asyncInsert = UsesAsyncInsert(dynamicParameters);
    TShardConnection result{
        .HostIndex = hostIndex,
        .Generation = ++LastConnectionGeneration_,
    };
    auto phase = EClickHouseAdmissionPhase::Connect;
    try {
        result.Connection = CreateConnection(shardIndex, hostIndex, EClickHouseConnectionPurpose::Write, dynamicParameters->WriteTimeout);
        phase = EClickHouseAdmissionPhase::QueryMetadata;
        auto metadata = result.Connection->QueryTargetMetadata();
        phase = EClickHouseAdmissionPhase::Validate;
        ValidateClickHouseEndpointMetadata(shard, shard.Hosts[hostIndex], metadata, &ShardReferences_[shardIndex], shardLogger);
        result.TargetEngine = metadata.Engine;
        if (IsBlockDeduplicatingEngine(metadata.Engine)) {
            phase = EClickHouseAdmissionPhase::QueryDedupWindow;
            result.TargetDedupWindow = result.Connection->QueryDedupWindow(asyncInsert);
        }
    } catch (const std::exception& ex) {
        const auto kind = phase == EClickHouseAdmissionPhase::Validate
            ? EClickHouseAdmissionFailureKind::Incompatible
            : ClassifyClickHouseAdmissionError(ex);
        throw TClickHouseAdmissionException(
            kind,
            MakeEndpointError(TError("Failed to admit ClickHouse replica connection"), shardIndex, hostIndex, phase, kind, ex)
                .With("connection_generation", result.Generation));
    }

    if (IsBlockDeduplicatingEngine(*result.TargetEngine)) {
        WarnIfDedupWindowBelowReplayHorizon(
            result.TargetDedupWindow,
            dynamicParameters->ReplayHorizon,
            GetClickHouseDedupWindowSettingName(asyncInsert),
            shard.Database,
            shard.Table,
            shardLogger);
    }

    YT_TLOG_DEBUG("ClickHouse replica connection admitted")
        .With("Shard", shard.Name)
        .With("Host", shard.Hosts[hostIndex])
        .With("ConnectionGeneration", result.Generation);

    return result;
}

TClickHouseWriter::TShardConnection TClickHouseWriter::AdmitNextCandidate(
    int shardIndex,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    const auto& shard = Shards_[shardIndex];
    const int hostCount = std::ssize(shard.Hosts);
    auto& traversal = CandidateTraversals_[shardIndex];

    YT_VERIFY(traversal.InSequence);

    if (traversal.VisitedHostCount == hostCount) {
        traversal.VisitedHostCount = 0;
        traversal.UnavailableHostErrors.clear();
        traversal.IndeterminateHosts.clear();
    }
    while (traversal.VisitedHostCount < hostCount) {
        const int hostIndex = traversal.NextHostIndex;
        traversal.NextHostIndex = (hostIndex + 1) % hostCount;
        ++traversal.VisitedHostCount;
        try {
            auto connection = AdmitCandidate(shardIndex, hostIndex, dynamicParameters);
            traversal.NextHostIndex = hostIndex;
            traversal.VisitedHostCount = 0;
            traversal.UnavailableHostErrors.clear();
            traversal.IndeterminateHosts.clear();
            return connection;
        } catch (const TClickHouseAdmissionException& ex) {
            YT_TLOG_WARNING("ClickHouse replica connection was not admitted")
                .With("Shard", shard.Name)
                .With(ex.Error());

            switch (ex.GetKind()) {
                case EClickHouseAdmissionFailureKind::Unavailable:
                    traversal.UnavailableHostErrors.push_back(ex.Error());
                    break;

                case EClickHouseAdmissionFailureKind::Indeterminate:
                    traversal.IndeterminateHosts.push_back(shard.Hosts[hostIndex]);
                    throw;

                case EClickHouseAdmissionFailureKind::Incompatible:
                    throw;
            }
        }
    }

    auto error = traversal.IndeterminateHosts.empty()
        ? TError("No available ClickHouse replica in shard %Qv", shard.Name)
        : TError("No ClickHouse replica in shard %Qv was admitted; the traversal included indeterminate failures", shard.Name)
            .With("indeterminate_hosts", traversal.IndeterminateHosts);
    error = std::move(error)
        .With("shard_index", shardIndex)
        .With("database", shard.Database)
        .With("table", shard.Table)
        .With(std::move(traversal.UnavailableHostErrors));
    traversal.VisitedHostCount = 0;
    traversal.UnavailableHostErrors.clear();
    traversal.IndeterminateHosts.clear();
    throw TClickHouseAdmissionException(EClickHouseAdmissionFailureKind::Unavailable, std::move(error));
}

void TClickHouseWriter::ReadmitConnection(
    int shardIndex,
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    BeginCandidateSequence(shardIndex, /*failover*/ true);
    Connections_[shardIndex] = AdmitNextCandidate(shardIndex, dynamicParameters);
    EndCandidateSequence(shardIndex);
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

void TClickHouseWriter::ApplyConnectionDynamicParameters(
    const TDynamicCommonClickHouseSinkParametersPtr& dynamicParameters)
{
    const bool recreateConnections =
        !AppliedConnectionDynamicParameters_ ||
        AppliedConnectionDynamicParameters_->WriteTimeout != dynamicParameters->WriteTimeout;
    const bool replayHorizonChanged =
        !AppliedConnectionDynamicParameters_ ||
        AppliedConnectionDynamicParameters_->ReplayHorizon != dynamicParameters->ReplayHorizon;
    const bool asyncInsert = UsesAsyncInsert(dynamicParameters);
    const bool asyncInsertChanged =
        !AppliedConnectionDynamicParameters_ ||
        (!RequiresInputFunction_ && AppliedConnectionDynamicParameters_->AsyncInsert != asyncInsert);

    std::vector<std::optional<TShardConnection>> stagedConnections(Shards_.size());
    std::vector<std::optional<std::optional<TDuration>>> stagedDedupWindows(Shards_.size());
    std::vector<TError> configurationErrors;
    auto worstFailureKind = EClickHouseAdmissionFailureKind::Unavailable;
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        auto& connection = Connections_[shardIndex];
        try {
            if (recreateConnections || !connection.Connection || connection.RequiresAdmission) {
                BeginCandidateSequence(shardIndex, /*failover*/ connection.RequiresAdmission);
                stagedConnections[shardIndex] = AdmitNextCandidate(shardIndex, dynamicParameters);
            } else if (
                (replayHorizonChanged || asyncInsertChanged) &&
                IsBlockDeduplicatingEngine(*connection.TargetEngine))
            {
                auto dedupWindow = connection.TargetDedupWindow;
                if (asyncInsertChanged) {
                    try {
                        dedupWindow = connection.Connection->QueryDedupWindow(asyncInsert);
                    } catch (const std::exception& ex) {
                        connection.RequiresAdmission = true;
                        const auto kind = ClassifyClickHouseAdmissionError(ex);
                        throw TClickHouseAdmissionException(
                            kind,
                            MakeEndpointError(
                                TError("Failed to query ClickHouse dedup window"),
                                shardIndex,
                                *connection.HostIndex,
                                EClickHouseAdmissionPhase::QueryDedupWindow,
                                kind,
                                ex));
                    }
                }
                WarnIfDedupWindowBelowReplayHorizon(
                    dedupWindow,
                    dynamicParameters->ReplayHorizon,
                    GetClickHouseDedupWindowSettingName(asyncInsert),
                    Shards_[shardIndex].Database,
                    Shards_[shardIndex].Table,
                    MakeShardLogger(shardIndex));
                stagedDedupWindows[shardIndex] = dedupWindow;
            }
            ClearShardError(shardIndex, EClickHouseShardErrorPhase::Configuration);
        } catch (const TClickHouseAdmissionException& ex) {
            worstFailureKind = std::max(worstFailureKind, ex.GetKind());
            auto error = TError("Failed to configure ClickHouse connection").With(ex.Error());
            SetShardError(
                shardIndex,
                EClickHouseShardErrorPhase::Configuration,
                error);
            configurationErrors.push_back(std::move(error));
        }
    }
    if (!configurationErrors.empty()) {
        throw TClickHouseAdmissionException(
            worstFailureKind,
            TError("Failed to configure ClickHouse connections").With(std::move(configurationErrors)));
    }

    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        if (stagedConnections[shardIndex]) {
            Connections_[shardIndex] = std::move(*stagedConnections[shardIndex]);
            EndCandidateSequence(shardIndex);
        } else if (stagedDedupWindows[shardIndex]) {
            Connections_[shardIndex].TargetDedupWindow = *stagedDedupWindows[shardIndex];
        }
    }
    AppliedConnectionDynamicParameters_ = dynamicParameters;
}

std::vector<TResolvedColumn> TClickHouseWriter::Connect(const NTableClient::TTableSchemaPtr& streamSchema)
{
    auto dynamicParameters = DynamicParameters_.Acquire();
    if (ShardReferences_.empty()) {
        IntrospectEndpoints();
    }

    auto resolved = ResolveColumns(ShardReferences_.front().Columns, streamSchema);
    RequiresInputFunction_ = std::ranges::any_of(resolved, [] (const auto& column) {
        return column.Type.RequiresInputFunction;
    });
    ApplyConnectionDynamicParameters(dynamicParameters);
    return resolved;
}

const std::vector<TClickHouseShard>& TClickHouseWriter::GetShards() const
{
    return Shards_;
}

std::vector<std::string> TClickHouseWriter::GetShardTargetIdentityFingerprints() const
{
    YT_VERIFY(ShardReferences_.size() == Shards_.size());

    std::vector<std::string> result;
    result.reserve(Shards_.size());
    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
        result.push_back(BuildShardTargetIdentityFingerprint(
            Shards_[shardIndex],
            ShardReferences_[shardIndex].ReplicationIdentity));
    }
    return result;
}

void TClickHouseWriter::Insert(const TWriteRequest& request, int shardWriteIndex, bool asyncInsert)
{
    const auto& shardWrite = request.ShardWrites[shardWriteIndex];
    Connections_[shardWrite.ShardIndex].Connection->Insert(shardWrite, asyncInsert, RequiresInputFunction_);
}

std::optional<TError> TClickHouseWriter::GetAdmissionTerminalError(
    const TClickHouseAdmissionException& ex,
    const TClickHouseRequestOptions& requestOptions,
    TClickHouseAttemptState* attemptState) const
{
    switch (ex.GetKind()) {
        case EClickHouseAdmissionFailureKind::Unavailable:
            return std::nullopt;

        case EClickHouseAdmissionFailureKind::Indeterminate:
            if (GetClickHouseFailureAction(EClickHouseErrorKind::Unclassified, requestOptions, attemptState) ==
                EClickHouseFailureAction::Retry)
            {
                return std::nullopt;
            }
            return TError("Giving up ClickHouse connection admission")
                .With("failure_kind", ex.GetKind())
                .With(ex.Error());

        case EClickHouseAdmissionFailureKind::Incompatible:
            return TError("ClickHouse replica is incompatible with the target table")
                .With("failure_kind", ex.GetKind())
                .With(ex.Error());
    }
    YT_ABORT();
}

void TClickHouseWriter::Run(TWeakPtr<TRefCounted> owner)
{
    std::optional<TError> fatalError;
    auto failRequest = [&] (TWriteRequest& request, const TError& error, bool sessionTerminal) {
        request.Promise.TrySet(error);
        if (sessionTerminal || request.Guarantee == EWriteGuarantee::ExactlyOnce) {
            fatalError = error;
        }
        ResetCandidateSequences();
    };

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

        auto dynamicParameters = DynamicParameters_.Acquire();
        const auto configurationRequestOptions = MakeClickHouseRequestOptions(*dynamicParameters);
        TClickHouseAttemptState attemptState;
        bool configured = false;
        while (!configured && !request.Promise.IsSet() && owner.Lock()) {
            dynamicParameters = DynamicParameters_.Acquire();
            try {
                if (RequiresInputFunction_ && dynamicParameters->AsyncInsert) {
                    auto error = TError("ClickHouse aggregate string arrays require async_insert=false")
                        .With("database", Parameters_->Database)
                        .With("table", Parameters_->Table);
                    for (int shardIndex = 0; shardIndex < std::ssize(Shards_); ++shardIndex) {
                        SetShardError(shardIndex, EClickHouseShardErrorPhase::Configuration, error);
                    }
                    error.ThrowOnError();
                }

                ApplyConnectionDynamicParameters(dynamicParameters);
                configured = true;
            } catch (const TClickHouseAdmissionException& ex) {
                YT_TLOG_WARNING("Failed to configure ClickHouse connection")
                    .With("FailureKind", ex.GetKind())
                    .With(ex.Error());

                if (auto terminalError = GetAdmissionTerminalError(ex, configurationRequestOptions, &attemptState)) {
                    YT_TLOG_ERROR("Failing ClickHouse write request after connection admission failure")
                        .With(*terminalError);

                    failRequest(request, *terminalError, ex.GetKind() == EClickHouseAdmissionFailureKind::Incompatible);
                    break;
                }
                TDelayedExecutor::WaitForDuration(dynamicParameters->RetryBackoff);
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

        const auto requestOptions = MakeClickHouseRequestOptions(*dynamicParameters);
        while (request.NextShardIndex < std::ssize(request.ShardWrites) && owner.Lock()) {
            const int shardIndex = request.ShardWrites[request.NextShardIndex].ShardIndex;
            if (Connections_[shardIndex].RequiresAdmission) {
                try {
                    ReadmitConnection(shardIndex, dynamicParameters);
                } catch (const TClickHouseAdmissionException& ex) {
                    static constexpr auto Message = "Failed to reconnect to ClickHouse shard"_sb;

                    YT_TLOG_WARNING(Message)
                        .With("FailureKind", ex.GetKind())
                        .With(ex.Error());

                    SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, TError(Message).With(ex.Error()));
                    if (auto terminalError = GetAdmissionTerminalError(ex, requestOptions, &attemptState)) {
                        YT_TLOG_ERROR("Failing ClickHouse write request after connection admission failure")
                            .With(*terminalError);

                        SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, *terminalError);
                        failRequest(request, *terminalError, ex.GetKind() == EClickHouseAdmissionFailureKind::Incompatible);
                        break;
                    }
                    TDelayedExecutor::WaitForDuration(dynamicParameters->RetryBackoff);
                    continue;
                }
            }

            try {
                Insert(request, request.NextShardIndex, requestOptions.AsyncInsert);
                ClearShardError(shardIndex, EClickHouseShardErrorPhase::Writing);
                ++request.NextShardIndex;
                attemptState = {};
                continue;
            } catch (const std::exception& ex) {
                Connections_[shardIndex].RequiresAdmission = true;
                static constexpr auto Message = "Failed to insert batch into ClickHouse"_sb;
                auto error = TError(Message)
                    .With(ex);
                YT_TLOG_WARNING(Message)
                    .With(ex);
                SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, error);

                auto errorKind = ClassifyClickHouseError(ex);
                auto action = GetClickHouseFailureAction(
                    errorKind,
                    requestOptions,
                    &attemptState);
                if (action == EClickHouseFailureAction::Fail) {
                    static constexpr auto TerminalMessage = "Giving up insert into ClickHouse"_sb;
                    auto terminalError = TError(TerminalMessage)
                        .With("error_kind", errorKind)
                        .With(error);
                    YT_TLOG_ERROR(TerminalMessage)
                        .With("ErrorKind", errorKind)
                        .With(error);
                    SetShardError(shardIndex, EClickHouseShardErrorPhase::Writing, terminalError);
                    failRequest(request, terminalError, /*sessionTerminal*/ false);
                    break;
                }
                TDelayedExecutor::WaitForDuration(dynamicParameters->RetryBackoff);
            }
        }
        if (request.NextShardIndex >= std::ssize(request.ShardWrites)) {
            request.Promise.TrySet();
        } else {
            request.Promise.TrySet(TError("ClickHouse writer stopped before the request completed"));
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
    NTableClient::TTableSchemaPtr streamSchema,
    NLogging::TLogger logger)
    : Logger(MakeSinkLogger(logger, *parameters))
    , Parameters_(std::move(parameters))
    , DynamicParameters_(std::move(dynamicParameters))
    , ErrorState_(statusProfiler->ErrorState("/writing"))
    , StreamSchema_(std::move(streamSchema))
{
    ValidateShardingKeyColumns(Parameters_->ShardingKeyColumns, {StreamSchema_});
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

        auto resolved = WaitFor(BIND(&TClickHouseWriter::Connect, session->Writer, StreamSchema_)
                .AsyncVia(session->ActionQueue->GetInvoker())
                .Run())
            .ValueOrThrow();
        BlockBuilder_.emplace(std::move(resolved));
        Router_.emplace(
            session->Writer->GetShards(),
            Parameters_->ShardingKeyColumns,
            std::vector{StreamSchema_});

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
        GetInputStreamSchema(GetContext(), GetSpec()),
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
        GetInputStreamSchema(GetContext(), GetSpec()),
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

std::optional<i64> TClickHouseSinkController::GetReceiverChannelCount()
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
