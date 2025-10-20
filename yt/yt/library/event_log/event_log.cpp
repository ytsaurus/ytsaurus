#include "event_log.h"
#include "config.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/mpsc_stack.h>

namespace NYT::NEventLog {

using namespace NApi;
using namespace NYson;
using namespace NTableClient;
using namespace NChunkClient;
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

TFluentLogEvent::TFluentLogEvent(std::unique_ptr<NYson::IYsonConsumer> consumer)
    : TBase(consumer.get())
    , Consumer_(std::move(consumer))
{
    Consumer_->OnBeginMap();
}

TFluentLogEvent::~TFluentLogEvent()
{
    if (Consumer_) {
        Consumer_->OnEndMap();
    }
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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

class TEventLogWriter
    : public IEventLogWriter
{
public:
    TEventLogWriter(
        TEventLogManagerConfigPtr config,
        IInvokerPtr invoker,
        NTableClient::IUnversionedWriterPtr writer,
        const NLogging::TLogger& logger)
        : Logger(logger)
        , Config_(config)
        , EventLogWriter_(std::move(writer))
    {
        YT_VERIFY(EventLogWriter_.Get());

        Enabled_.store(config->Enable);

        PendingRowsFlushExecutor_ = New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TEventLogWriter::OnPendingEventLogRowsFlush, Unretained(this)),
            config->PendingRowsFlushPeriod);
        PendingRowsFlushExecutor_->Start();
    }

    std::unique_ptr<IYsonConsumer> CreateConsumer() override
    {
        auto valueConsumer = std::make_unique<TEventLogValueConsumer>(
            EventLogWriter_->GetNameTable(),
            BIND(&TEventLogWriter::AddRow, MakeStrong(this)));
        return std::make_unique<TEventLogTableConsumer>(std::move(valueConsumer));
    }

    TEventLogManagerConfigPtr GetConfig() const override
    {
        return Config_.Acquire();
    }

    void UpdateConfig(const TEventLogManagerConfigPtr& config) override
    {
        Config_.Store(config);
        Enabled_.store(config->Enable);
        PendingRowsFlushExecutor_->SetPeriod(config->PendingRowsFlushPeriod);
    }

    TFuture<void> Close() override
    {
        return BIND(&TEventLogWriter::DoClose, Unretained(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

private:
    const NLogging::TLogger Logger;

    TAtomicIntrusivePtr<TEventLogManagerConfig> Config_;

    NTableClient::IUnversionedWriterPtr EventLogWriter_;
    TFuture<void> EventLogWriterReadyEvent_;

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
        if (EventLogWriterReadyEvent_) {
            if (!EventLogWriterReadyEvent_.IsSet()) {
                return;
            }

            EventLogWriterReadyEvent_.Get().ThrowOnError();
            EventLogWriterReadyEvent_ = {};
        }

        auto owningRows = PendingEventLogRows_.DequeueAll();
        if (owningRows.empty()) {
            return;
        }

        std::vector<TUnversionedRow> rows(owningRows.begin(), owningRows.end());
        bool writerReady = true;

        // TODO(eshcherbin): Add better error handling? Retries with exponential backoff?
        try {
            writerReady = EventLogWriter_->Write(rows);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Could not write to event log");
        }

        if (!writerReady) {
            EventLogWriterReadyEvent_ = EventLogWriter_->GetReadyEvent();
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

IEventLogWriterPtr CreateEventLogWriter(
    TEventLogManagerConfigPtr config,
    IInvokerPtr invoker,
    NTableClient::IUnversionedWriterPtr logWriter,
    const NLogging::TLogger& logger)
{
    return New<TEventLogWriter>(std::move(config), std::move(invoker), std::move(logWriter), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
