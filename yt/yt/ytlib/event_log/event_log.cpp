#include "event_log.h"

#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/mpsc_stack.h>

namespace NYT::NEventLog {

using namespace NApi;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

TFluentLogEventConsumer::TFluentLogEventConsumer(const NLogging::TLogger* logger)
    : Logger_(logger)
    , State_(New<TState>(EYsonFormat::Binary, EYsonType::MapFragment))
{
    YT_VERIFY(Logger_);
}

void TFluentLogEventConsumer::OnMyBeginMap()
{
    Forward(State_->GetConsumer(), /*onFinished*/ {}, EYsonType::MapFragment);
}

void TFluentLogEventConsumer::OnMyEndMap()
{
    LogStructuredEvent(*Logger_, State_->GetValue(), NLogging::ELogLevel::Info);
}

////////////////////////////////////////////////////////////////////////////////

class TEventLogValueConsumer
    : public IValueConsumer
{
public:
    TEventLogValueConsumer(
        TNameTablePtr nameTable,
        TCallback<void(TUnversionedOwningRow)> addRow)
        : NameTable_(std::move(nameTable))
        , AddRow_(std::move(addRow))
    { }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    void OnBeginRow() override
    {
        YT_VERIFY(!std::exchange(BuildingRow_, true));
        ContextSwitchGuard_.emplace();
    }

    void OnValue(const TUnversionedValue& value) override
    {
        YT_VERIFY(BuildingRow_);

        Builder_.AddValue(value);
    }

    void OnEndRow() override
    {
        YT_VERIFY(std::exchange(BuildingRow_, false));
        ContextSwitchGuard_.reset();

        AddRow_(Builder_.FinishRow());
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

private:
    const TNameTablePtr NameTable_;
    const TCallback<void(TUnversionedOwningRow)> AddRow_;

    const TTableSchemaPtr Schema_ = New<TTableSchema>();

    bool BuildingRow_ = false;
    std::optional<TForbidContextSwitchGuard> ContextSwitchGuard_;

    TUnversionedOwningRowBuilder Builder_;
};

class TEventLogTableConsumer
    : public TTableConsumer
{
public:
    explicit TEventLogTableConsumer(std::unique_ptr<TEventLogValueConsumer> valueConsumer)
        : TTableConsumer(TYsonConverterConfig(), valueConsumer.get())
        , ValueConsumer_(std::move(valueConsumer))
    { }

private:
    std::unique_ptr<TEventLogValueConsumer> ValueConsumer_;
};

class TEventLogWriter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TEventLogManagerConfigPtr config,
        IInvokerPtr invoker,
        NTableClient::IUnversionedWriterPtr writer)
        : Config_(std::move(config))
        , EventLogWriter_(std::move(writer))
    {
        YT_VERIFY(EventLogWriter_.Get());

        Enabled_.store(Config_->Enable);

        PendingRowsFlushExecutor_ = New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TImpl::OnPendingEventLogRowsFlush, Unretained(this)),
            Config_->PendingRowsFlushPeriod);
        PendingRowsFlushExecutor_->Start();
    }

    std::unique_ptr<IYsonConsumer> CreateConsumer()
    {
        auto valueConsumer = std::make_unique<TEventLogValueConsumer>(
            EventLogWriter_->GetNameTable(),
            BIND(&TImpl::AddRow, MakeStrong(this)));
        return std::make_unique<TEventLogTableConsumer>(std::move(valueConsumer));
    }

    TEventLogManagerConfigPtr GetConfig() const
    {
        return Config_;
    }

    void UpdateConfig(const TEventLogManagerConfigPtr& config)
    {
        Config_ = config;
        Enabled_.store(Config_->Enable);
        PendingRowsFlushExecutor_->SetPeriod(Config_->PendingRowsFlushPeriod);
    }

    TFuture<void> Close()
    {
        return BIND(&TEventLogWriter::TImpl::DoClose, Unretained(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

private:
    TEventLogManagerConfigPtr Config_;

    NTableClient::IUnversionedWriterPtr EventLogWriter_;

    TMpscStack<TUnversionedOwningRow> PendingEventLogRows_;
    TPeriodicExecutorPtr PendingRowsFlushExecutor_;

    std::atomic<bool> Enabled_{false};

    void AddRow(TUnversionedOwningRow row)
    {
        if (Enabled_) {
            PendingEventLogRows_.Enqueue(std::move(row));
        }
    }

    void OnPendingEventLogRowsFlush()
    {
        auto owningRows = PendingEventLogRows_.DequeueAll();
        if (!owningRows.empty()) {
            std::vector<TUnversionedRow> rows(owningRows.begin(), owningRows.end());
            EventLogWriter_->Write(rows);
        }
    }

    void DoClose()
    {
        // NB(mrkastep): Since we want all rows added before Close to be flushed,
        // we should schedule our own flush and wait until it is completed.
        PendingRowsFlushExecutor_->ScheduleOutOfBand();
        WaitFor(PendingRowsFlushExecutor_->GetExecutedEvent())
            .ThrowOnError();
        WaitFor(PendingRowsFlushExecutor_->Stop())
            .ThrowOnError();
        WaitFor(EventLogWriter_->Close())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEventLogWriter::TEventLogWriter(
    TEventLogManagerConfigPtr config,
    IInvokerPtr invoker,
    NTableClient::IUnversionedWriterPtr writer)
    : Impl_(New<TImpl>(std::move(config), std::move(invoker), std::move(writer)))
{ }

TEventLogWriter::~TEventLogWriter()
{ }

std::unique_ptr<IYsonConsumer> TEventLogWriter::CreateConsumer()
{
    return Impl_->CreateConsumer();
}

TEventLogManagerConfigPtr TEventLogWriter::GetConfig() const
{
    return Impl_->GetConfig();
}

void TEventLogWriter::UpdateConfig(const TEventLogManagerConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

TFuture<void> TEventLogWriter::Close()
{
    return Impl_->Close();
}

////////////////////////////////////////////////////////////////////////////////

TEventLogWriterPtr CreateStaticTableEventLogWriter(
    TEventLogManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker)
{
    auto nameTable = New<TNameTable>();
    auto options = New<NTableClient::TTableWriterOptions>();
    options->EnableValidationOptions();

    auto eventLogWriter = CreateSchemalessBufferedTableWriter(
        config,
        options,
        client,
        nameTable,
        config->Path);

    return New<TEventLogWriter>(std::move(config), std::move(invoker), std::move(eventLogWriter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog

