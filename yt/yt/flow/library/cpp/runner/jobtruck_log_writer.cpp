#include "jobtruck_log_writer.h"

#include <yt/yt/flow/library/cpp/jobtruck/metadata.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/log_writer_detail.h>
#include <yt/yt/core/logging/log_writer_factory.h>
#include <yt/yt/core/logging/system_log_event_provider.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <util/stream/output.h>

#include <deque>
#include <iterator>
#include <limits>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const TLogger, Logger, "Logging");

constexpr TStringBuf SourceId = "itflow_v0:controller_leader";
constexpr TStringBuf LogName = "controller";
constexpr TStringBuf FilePath = "/logs/controller.log";
constexpr ui64 TimestampIndexBitCount = 20;
constexpr ui64 MaxTimestampIndex = (1ULL << TimestampIndexBitCount) - 1;
constexpr int BufferLimit = 10'000;
constexpr auto WarningPeriod = TDuration::Minutes(1);
constexpr auto MetadataRefreshPeriod = TDuration::Minutes(10);

////////////////////////////////////////////////////////////////////////////////

class TTimestampGenerator
{
public:
    ui64 Next(TInstant now)
    {
        ui64 nowMilliseconds = now.MilliSeconds();
        if (nowMilliseconds > Milliseconds_) {
            Milliseconds_ = nowMilliseconds;
            Index_ = 0;
        } else if (Index_ < MaxTimestampIndex) {
            ++Index_;
        } else {
            ++Milliseconds_;
            Index_ = 0;
        }

        return (Milliseconds_ << TimestampIndexBitCount) + Index_;
    }

private:
    ui64 Milliseconds_ = 0;
    ui64 Index_ = 0;
};

TYsonString CreateSourceMeta(const TNodeInfoPtr& nodeInfo)
{
    auto sourceMeta = New<NJobtruck::TSourceMeta>();
    sourceMeta->Kind = "yt_flow_controller";
    sourceMeta->DisplayName = "Flow controller leader";
    sourceMeta->NodeInfo = ConvertToNode(nodeInfo);
    return ConvertToYsonString(sourceMeta, EYsonFormat::Binary);
}

TYsonString CreateLogConfig()
{
    auto logConfig = New<NJobtruck::TLogConfig>();
    logConfig->LogLevel = "INFO";
    return ConvertToYsonString(logConfig, EYsonFormat::Binary);
}

////////////////////////////////////////////////////////////////////////////////

struct TJobtruckLogWriterConfig
    : public TLogWriterConfig
{
    static constexpr TStringBuf WriterType = "jobtruck";

    TRichYPath SourcePath;

    REGISTER_YSON_STRUCT(TJobtruckLogWriterConfig);

    static void Register(TRegistrar registrar);
};

using TJobtruckLogWriterConfigPtr = TIntrusivePtr<TJobtruckLogWriterConfig>;

////////////////////////////////////////////////////////////////////////////////

struct TJobtruckLogWriterContext
    : public TRefCounted
{
    const IClientPtr Client;
    const std::string InstanceId;
    const i64 CreationTime;
    const TYsonString SourceMeta;
    const IStatusErrorStatePtr ErrorState;

    TJobtruckLogWriterContext(IClientPtr client, TNodeInfoPtr nodeInfo, IStatusErrorStatePtr errorState)
        : Client(std::move(client))
        , InstanceId(ToString(nodeInfo->IncarnationId))
        , CreationTime(static_cast<i64>(TInstant::Now().MicroSeconds()))
        , SourceMeta(CreateSourceMeta(nodeInfo))
        , ErrorState(std::move(errorState))
    { }
};

struct TJobtruckLogWriterGlobalState final
{
    TAtomicIntrusivePtr<TJobtruckLogWriterContext> Context;

    static TIntrusivePtr<TJobtruckLogWriterGlobalState> Get()
    {
        return LeakyRefCountedSingleton<TJobtruckLogWriterGlobalState>();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSourcesMetaColumns
{
    const TNameTablePtr NameTable = New<TNameTable>();
    const int SourceId = NameTable->RegisterName("source_id");
    const int InstanceId = NameTable->RegisterName("instance_id");
    const int Meta = NameTable->RegisterName("meta");
};

struct TLogsMetaColumns
{
    const TNameTablePtr NameTable = New<TNameTable>();
    const int SourceId = NameTable->RegisterName("source_id");
    const int InstanceId = NameTable->RegisterName("instance_id");
    const int LogName = NameTable->RegisterName("log_name");
    const int FilePath = NameTable->RegisterName("file_path");
    const int LogConfig = NameTable->RegisterName("log_config");
    const int CreationTime = NameTable->RegisterName("creation_time");
};

struct TLogsColumns
{
    const TNameTablePtr NameTable = New<TNameTable>();
    const int SourceId = NameTable->RegisterName("source_id");
    const int InstanceId = NameTable->RegisterName("instance_id");
    const int LogName = NameTable->RegisterName("log_name");
    const int FilePath = NameTable->RegisterName("file_path");
    const int Timestamp = NameTable->RegisterName("ts");
    const int Line = NameTable->RegisterName("line");
};

bool ShouldLogWarning(TInstant* lastWarningTime)
{
    auto now = TInstant::Now();
    if (*lastWarningTime && now - *lastWarningTime < WarningPeriod) {
        return false;
    }
    *lastWarningTime = now;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TJobtruckLogWriterState
    : public TRefCounted
{
    struct TBufferedRow
    {
        ui64 Timestamp = 0;
        std::string Line;
    };

public:
    explicit TJobtruckLogWriterState(TRichYPath sourcePath)
        : SourcePath_(std::move(sourcePath))
        , SourcesMetaPath_(YPathJoin(SourcePath_.GetPath(), SourcesMetaTableName))
        , LogsMetaPath_(YPathJoin(SourcePath_.GetPath(), LogsMetaTableName))
        , LogsPath_(YPathJoin(SourcePath_.GetPath(), LogsTableName))
        , LogConfig_(CreateLogConfig())
        , DroppedRowsCounter_(TProfiler("/logging/jobtruck").WithTag("reason", "buffer_full").Counter("/rows_dropped"))
    { }

    const TRichYPath& GetSourcePath() const
    {
        return SourcePath_;
    }

    void Write(const void* buf, size_t size)
    {
        const char* data = static_cast<const char*>(buf);
        if (size > 0 && data[size - 1] == '\n') {
            --size;
        }

        if (std::ssize(Rows_) == BufferLimit) {
            Rows_.pop_front();
            ++DroppedRowCount_;
            DroppedRowsCounter_.Increment();
        }

        Rows_.push_back({
            .Timestamp = TimestampGenerator_.Next(TInstant::Now()),
            .Line = std::string(data, size),
        });
    }

    void Flush()
    {
        if (FlushInProgress_ || TInstant::Now() < NextFlushAttemptTime_) {
            return;
        }

        auto context = TJobtruckLogWriterGlobalState::Get()->Context.Acquire();
        if (!context) {
            if (!Rows_.empty() && ShouldLogWarning(&LastContextWarningTime_)) {
                YT_TLOG_WARNING("Cannot write jobtruck log to YT because context is not provided yet")
                    .With("SourcePath", SourcePath_)
                    .With("PendingRows", Rows_.size())
                    .With("DroppedRows", DroppedRowCount_);
            }
            return;
        }

        if (Rows_.empty()) {
            return;
        }

        FlushInProgress_ = true;
        auto flushGuard = Finally([this] {
            FlushInProgress_ = false;
        });

        std::deque<TBufferedRow> batch;
        batch.swap(Rows_);
        try {
            if (!FormatVersionVerified_) {
                VerifyFormatVersion(context->Client);
                FormatVersionVerified_ = true;
            }

            WriteRows(context, batch);
            FlushBackoff_.Restart();
            NextFlushAttemptTime_ = TInstant::Zero();
            context->ErrorState->ClearError();
        } catch (const std::exception& ex) {
            batch.insert(batch.end(), std::make_move_iterator(Rows_.begin()), std::make_move_iterator(Rows_.end()));
            Rows_.swap(batch);
            while (std::ssize(Rows_) > BufferLimit) {
                Rows_.pop_front();
                ++DroppedRowCount_;
                DroppedRowsCounter_.Increment();
            }
            FlushBackoff_.Next();
            NextFlushAttemptTime_ = TInstant::Now() + FlushBackoff_.GetBackoff();
            context->ErrorState->SetError(TError("Failed to write jobtruck log rows")
                    .With("source_path", SourcePath_)
                    .With(ex));
            if (ShouldLogWarning(&LastDataWarningTime_)) {
                YT_TLOG_WARNING("Failed to write jobtruck log rows")
                    .With("SourcePath", SourcePath_)
                    .With("PendingRows", Rows_.size())
                    .With("DroppedRows", DroppedRowCount_)
                    .With(ex);
            }
        }
    }

private:
    void VerifyFormatVersion(const IClientPtr& client)
    {
        auto formatVersionPath = Format("%v/@%v", SourcePath_.GetPath(), NJobtruck::LogsFormatVersionAttribute);
        int formatVersion = ConvertTo<int>(WaitFor(client->GetNode(formatVersionPath))
                .ValueOrThrow());
        THROW_ERROR_EXCEPTION_IF(
            formatVersion != NJobtruck::CurrentLogsFormatVersion,
            "Unsupported jobtruck log format version %v",
            formatVersion);
    }

    void WriteRows(
        const TIntrusivePtr<TJobtruckLogWriterContext>& context,
        const std::deque<TBufferedRow>& batch)
    {
        bool writeMetadata = MetadataContext_ != context ||
            TInstant::Now() - LastMetadataRefreshTime_ >= MetadataRefreshPeriod;
        TUnversionedRowsBuilder sourcesMetaRows;
        if (writeMetadata) {
            TUnversionedRowBuilder row;
            row.AddValue(MakeUnversionedStringValue(SourceId, SourcesMetaColumns_.SourceId));
            row.AddValue(MakeUnversionedStringValue(context->InstanceId, SourcesMetaColumns_.InstanceId));
            row.AddValue(MakeUnversionedAnyValue(context->SourceMeta.AsStringBuf(), SourcesMetaColumns_.Meta));
            sourcesMetaRows.AddRow(row.GetRow());
        }

        TUnversionedRowsBuilder logsMetaRows;
        if (writeMetadata) {
            TUnversionedRowBuilder row;
            row.AddValue(MakeUnversionedStringValue(SourceId, LogsMetaColumns_.SourceId));
            row.AddValue(MakeUnversionedStringValue(context->InstanceId, LogsMetaColumns_.InstanceId));
            row.AddValue(MakeUnversionedStringValue(LogName, LogsMetaColumns_.LogName));
            row.AddValue(MakeUnversionedStringValue(FilePath, LogsMetaColumns_.FilePath));
            row.AddValue(MakeUnversionedAnyValue(LogConfig_.AsStringBuf(), LogsMetaColumns_.LogConfig));
            row.AddValue(MakeUnversionedInt64Value(context->CreationTime, LogsMetaColumns_.CreationTime));
            logsMetaRows.AddRow(row.GetRow());
        }

        TUnversionedRowsBuilder logRows;
        for (const auto& bufferedRow : batch) {
            TUnversionedRowBuilder row;
            row.AddValue(MakeUnversionedStringValue(SourceId, LogsColumns_.SourceId));
            row.AddValue(MakeUnversionedStringValue(context->InstanceId, LogsColumns_.InstanceId));
            row.AddValue(MakeUnversionedStringValue(LogName, LogsColumns_.LogName));
            row.AddValue(MakeUnversionedStringValue(FilePath, LogsColumns_.FilePath));
            row.AddValue(MakeUnversionedUint64Value(bufferedRow.Timestamp, LogsColumns_.Timestamp));
            row.AddValue(MakeUnversionedStringValue(bufferedRow.Line, LogsColumns_.Line));
            logRows.AddRow(row.GetRow());
        }

        auto transaction = WaitFor(context->Client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        if (writeMetadata) {
            transaction->WriteRows(SourcesMetaPath_, SourcesMetaColumns_.NameTable, sourcesMetaRows.Build());
            transaction->WriteRows(LogsMetaPath_, LogsMetaColumns_.NameTable, logsMetaRows.Build());
        }
        transaction->WriteRows(LogsPath_, LogsColumns_.NameTable, logRows.Build());
        WaitFor(transaction->Commit())
            .ThrowOnError();
        if (writeMetadata) {
            MetadataContext_ = context;
            LastMetadataRefreshTime_ = TInstant::Now();
        }
    }

private:
    const TRichYPath SourcePath_;
    const TYPath SourcesMetaPath_;
    const TYPath LogsMetaPath_;
    const TYPath LogsPath_;

    const TSourcesMetaColumns SourcesMetaColumns_;
    const TLogsMetaColumns LogsMetaColumns_;
    const TLogsColumns LogsColumns_;
    const TYsonString LogConfig_;
    const TCounter DroppedRowsCounter_;

    TTimestampGenerator TimestampGenerator_;
    std::deque<TBufferedRow> Rows_;
    i64 DroppedRowCount_ = 0;
    bool FlushInProgress_ = false;
    bool FormatVersionVerified_ = false;
    TIntrusivePtr<TJobtruckLogWriterContext> MetadataContext_;
    TInstant LastMetadataRefreshTime_;

    TBackoffStrategy FlushBackoff_{TExponentialBackoffOptions{
        .InvocationCount = std::numeric_limits<int>::max(),
        .MaxBackoff = TDuration::Seconds(30),
    }};
    TInstant NextFlushAttemptTime_;

    TInstant LastContextWarningTime_;
    TInstant LastDataWarningTime_;
};

////////////////////////////////////////////////////////////////////////////////

using TJobtruckLogWriterStatePtr = TIntrusivePtr<TJobtruckLogWriterState>;

struct TJobtruckLogWriterStateHolder final
{
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock);
    TJobtruckLogWriterStatePtr State;

    static TIntrusivePtr<TJobtruckLogWriterStateHolder> Get()
    {
        return LeakyRefCountedSingleton<TJobtruckLogWriterStateHolder>();
    }
};

TJobtruckLogWriterStatePtr GetOrCreateJobtruckLogWriterState(TRichYPath sourcePath)
{
    auto holder = TJobtruckLogWriterStateHolder::Get();
    auto guard = Guard(holder->Lock);
    if (!holder->State) {
        holder->State = New<TJobtruckLogWriterState>(std::move(sourcePath));
    } else {
        const auto& currentSourcePath = holder->State->GetSourcePath();
        YT_VERIFY(currentSourcePath.GetPath() == sourcePath.GetPath());
        YT_VERIFY(currentSourcePath.GetCluster() == sourcePath.GetCluster());
    }
    return holder->State;
}

class TJobtruckWriterStream
    : public IOutputStream
{
public:
    explicit TJobtruckWriterStream(TRichYPath sourcePath)
        : State_(GetOrCreateJobtruckLogWriterState(std::move(sourcePath)))
    { }

private:
    void DoWrite(const void* buf, size_t size) override
    {
        State_->Write(buf, size);
    }

    void DoFlush() override
    {
        State_->Flush();
    }

private:
    const TJobtruckLogWriterStatePtr State_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobtruckLogWriter
    : public TStreamLogWriterBase
{
public:
    TJobtruckLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
        std::string name,
        const TJobtruckLogWriterConfigPtr& config)
        : TStreamLogWriterBase(std::move(formatter), std::move(systemEventProvider), std::move(name), config)
        , Stream_(std::make_unique<TJobtruckWriterStream>(config->SourcePath))
    { }

private:
    IOutputStream* GetOutputStream() const noexcept override
    {
        return Stream_.get();
    }

    void OnException(const std::exception& ex) override
    {
        YT_TLOG_ERROR("Jobtruck log writer failed")
            .With(ex);
    }

private:
    std::unique_ptr<IOutputStream> Stream_;
};

class TJobtruckLogWriterFactory
    : public ILogWriterFactory
{
public:
    void ValidateConfig(const IMapNodePtr& configNode) override
    {
        ParseConfig(configNode);
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        std::string name,
        const IMapNodePtr& configNode,
        ILogWriterHost*) noexcept override
    {
        auto config = ParseConfig(configNode);
        auto eventProvider = CreateDefaultSystemLogEventProvider(config);
        return New<TJobtruckLogWriter>(
            std::move(formatter),
            std::move(eventProvider),
            std::move(name),
            std::move(config));
    }

private:
    static TJobtruckLogWriterConfigPtr ParseConfig(const IMapNodePtr& configNode)
    {
        return ConvertTo<TJobtruckLogWriterConfigPtr>(configNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TJobtruckLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("source_path", &TThis::SourcePath);

    registrar.Preprocessor([] (TThis* config) {
        config->Type = TThis::WriterType;
    });

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(!config->SourcePath.GetCluster(), "Source cluster must be specified");
    });
}

void RegisterJobtruckLogWriterFactory()
{
    TLogManager::Get()->RegisterWriterFactory(
        std::string(TJobtruckLogWriterConfig::WriterType),
        LeakyRefCountedSingleton<TJobtruckLogWriterFactory>());
}

IMapNodePtr GetJobtruckLogWriterConfig(TRichYPath pipelinePath)
{
    auto writerConfig = New<TJobtruckLogWriterConfig>();
    pipelinePath.SetPath(YPathJoin(pipelinePath.GetPath(), LogsDirectoryName));
    writerConfig->SourcePath = std::move(pipelinePath);
    return ConvertTo<IMapNodePtr>(writerConfig);
}

void SetJobtruckLogWriterContext(IClientPtr client, TNodeInfoPtr nodeInfo, IStatusErrorStatePtr errorState)
{
    auto state = TJobtruckLogWriterGlobalState::Get();
    if (!client || !nodeInfo) {
        state->Context.Store(nullptr);
        return;
    }
    state->Context.Store(New<TJobtruckLogWriterContext>(std::move(client), std::move(nodeInfo), std::move(errorState)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
