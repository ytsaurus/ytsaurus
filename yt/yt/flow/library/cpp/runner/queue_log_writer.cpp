#include "queue_log_writer.h"

#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/logging/formatter.h>
#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/log_writer_detail.h>
#include <yt/yt/core/logging/log_writer_factory.h>
#include <yt/yt/core/logging/system_log_event_provider.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <util/stream/output.h>
#include <util/string/strip.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NTableClient;
using namespace NYPath;

namespace {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const TLogger, Logger, "Logging");

////////////////////////////////////////////////////////////////////////////////

static const auto QueueLogSchema = New<TTableSchema>(
    std::vector<TColumnSchema>{
        TColumnSchema("host", EValueType::String),
        TColumnSchema("data", EValueType::String),
        TColumnSchema("codec", EValueType::String),
    },
    /*strict*/ true);

////////////////////////////////////////////////////////////////////////////////

class TQueueLogWriterConfig
    : public NLogging::TLogWriterConfig
{
public:
    static constexpr TStringBuf WriterType = "queue";

    NYPath::TRichYPath QueuePath;

    REGISTER_YSON_STRUCT(TQueueLogWriterConfig);

    static void Register(TRegistrar registrar);
};

using TQueueLogWriterConfigPtr = TIntrusivePtr<TQueueLogWriterConfig>;

////////////////////////////////////////////////////////////////////////////////

struct TQueueLogWriterGlobalState final
{
    TAtomicIntrusivePtr<IClient> Client;

    static TIntrusivePtr<TQueueLogWriterGlobalState> Get()
    {
        return LeakyRefCountedSingleton<TQueueLogWriterGlobalState>();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueueWriterStream
    : public IOutputStream
{
public:
    static constexpr size_t BufferLimit = 10000;

public:
    explicit TQueueWriterStream(const TQueueLogWriterConfigPtr& config)
        : QueuePath_(config->QueuePath)
        , NameTable_(TNameTable::FromSchema(*QueueLogSchema))
        , DataColumnId_(NameTable_->GetIdOrThrow("data"))
        , HostColumnId_(NameTable_->GetIdOrThrow("host"))
    { }

    const TRichYPath& QueuePath() const
    {
        return QueuePath_;
    }

    void Truncate()
    {
        if (LogRows_.size() > BufferLimit) {
            LogRows_.clear();
        }
    }

private:
    void DoWrite(const void* buf, size_t size) override
    {
        const char* buffer = static_cast<const char*>(buf);
        Strip(buffer, size);
        LogRows_.emplace_back(buffer, size);
    }

    void DoFlush() override
    {
        auto client = TQueueLogWriterGlobalState::Get()->Client.Acquire();
        if (!client) {
            if (!LogRows_.empty()) {
                YT_TLOG_WARNING("Can not write log to YT because YT client is not provided yet")
                    .With("Table", QueuePath_)
                    .With("PendingRows", LogRows_.size());
            }
            return;
        }

        TUnversionedRowsBuilder rowsBuilder;
        auto rowBuffer = New<TRowBuffer>();

        for (const auto& logRow : LogRows_) {
            TUnversionedRowBuilder rowBuilder;
            rowBuilder.AddValue(ToUnversionedValue(NNet::GetLocalHostNameRaw(), rowBuffer, HostColumnId_));
            rowBuilder.AddValue(ToUnversionedValue(logRow, rowBuffer, DataColumnId_));

            rowsBuilder.AddRow(rowBuilder.GetRow());
        }

        auto transaction = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(QueuePath_.GetPath(), NameTable_, rowsBuilder.Build());

        WaitFor(transaction->Commit())
            .ThrowOnError();

        LogRows_.clear();
    }

private:
    const TRichYPath QueuePath_;
    const TNameTablePtr NameTable_;

    const int DataColumnId_;
    const int HostColumnId_;

    std::vector<std::string> LogRows_;
};

class TQueueLogWriter
    : public TStreamLogWriterBase
{
public:
    TQueueLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
        std::string name,
        const TQueueLogWriterConfigPtr& config)
        : TStreamLogWriterBase(std::move(formatter), std::move(systemEventProvider), std::move(name), config)
        , Stream_(std::make_unique<TQueueWriterStream>(config))
    { }

private:
    IOutputStream* GetOutputStream() const noexcept override
    {
        return Stream_.get();
    }

    void OnException(const std::exception& ex) override
    {
        YT_TLOG_ERROR("Queue write failed")
            .With("Queue", Stream_->QueuePath(), "%Qv")
            .With(ex);

        Stream_->Truncate();
    }

private:
    std::unique_ptr<TQueueWriterStream> Stream_;
};

class TQueueLogWriterFactory
    : public ILogWriterFactory
{
public:
    void ValidateConfig(const NYTree::IMapNodePtr& configNode) override
    {
        ParseConfig(configNode);
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        std::string name,
        const NYTree::IMapNodePtr& configNode,
        ILogWriterHost*) noexcept override
    {
        auto config = ParseConfig(configNode);
        auto eventProvider = CreateDefaultSystemLogEventProvider(config);
        return New<TQueueLogWriter>(
            std::move(formatter),
            std::move(eventProvider),
            std::move(name),
            std::move(config));
    }

private:
    static TQueueLogWriterConfigPtr ParseConfig(const NYTree::IMapNodePtr& configNode)
    {
        return ConvertTo<TQueueLogWriterConfigPtr>(configNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

void TQueueLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath);

    registrar.Preprocessor([] (TThis* config) {
        config->Type = TThis::WriterType;
    });

    registrar.Postprocessor([] (TQueueLogWriterConfig* config) {
        THROW_ERROR_EXCEPTION_IF(!config->QueuePath.GetCluster().has_value(), "Queue cluster must be specified");
    });
}

////////////////////////////////////////////////////////////////////////////////

void RegisterQueueLogWriterFactory()
{
    NLogging::TLogManager::Get()->RegisterWriterFactory(
        std::string(TQueueLogWriterConfig::WriterType),
        LeakyRefCountedSingleton<TQueueLogWriterFactory>());
}

NYTree::IMapNodePtr GetQueueLogWriterConfig(NYPath::TRichYPath pipelinePath)
{
    auto writerConfig = New<TQueueLogWriterConfig>();
    writerConfig->QueuePath = NYPath::YPathJoin(pipelinePath.GetPath(), ControllerLogsTableName);
    writerConfig->QueuePath.SetCluster(pipelinePath.GetCluster().value());
    return ConvertTo<NYTree::IMapNodePtr>(writerConfig);
}

void SetQueueLogWriterClient(NApi::IClientPtr client)
{
    TQueueLogWriterGlobalState::Get()->Client.Store(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
