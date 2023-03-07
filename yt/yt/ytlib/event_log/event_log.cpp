#include "event_log.h"

#include <yt/client/table_client/table_consumer.h>
#include <yt/client/table_client/unversioned_writer.h>
#include <yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/client/table_client/value_consumer.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/lock_free.h>

namespace NYT::NEventLog {

using namespace NApi;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFluentLogEventConsumer::TFluentLogEventConsumer(IYsonConsumer* tableConsumer, const NLogging::TLogger* logger)
    : Logger_(logger)
    , TableConsumer_(tableConsumer)
{
    YT_VERIFY(tableConsumer || logger);
    if (Logger_) {
        State_ = New<TState>(EYsonFormat::Binary, EYsonType::MapFragment);
    }
}

void TFluentLogEventConsumer::OnMyBeginMap()
{
    std::vector<IYsonConsumer*> consumers;

    if (TableConsumer_) {
        TableConsumer_->OnBeginMap();
        consumers.push_back(TableConsumer_);
    }

    if (Logger_) {
        YT_VERIFY(State_);
        consumers.push_back(State_->GetConsumer());
    }

    Forward(std::move(consumers), nullptr, EYsonType::MapFragment);
}

void TFluentLogEventConsumer::OnMyEndMap()
{
    if (TableConsumer_) {
        TableConsumer_->OnEndMap();
    }
    if (Logger_) {
        LogStructuredEvent(*Logger_, State_->GetValue(), NLogging::ELogLevel::Info);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TEventLogValueConsumer
    : public IValueConsumer
{
public:
    explicit TEventLogValueConsumer(
        const TNameTablePtr& nameTable,
        TCallback<void(TUnversionedOwningRow)> addRow)
        : NameTable_(nameTable)
        , AddRow_(addRow)
    { }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    virtual void OnBeginRow() override
    { }

    virtual void OnValue(const TUnversionedValue& value) override
    {
        Builder_.AddValue(value);
    }

    virtual void OnEndRow() override
    {
        AddRow_(Builder_.FinishRow());
    }

    virtual const TTableSchema& GetSchema() const override
    {
        static TTableSchema schema;
        return schema;
    }

private:
    TNameTablePtr NameTable_;
    TCallback<void(TUnversionedOwningRow)> AddRow_;

    TUnversionedOwningRowBuilder Builder_;
};

class TEventLogTableConsumer
    : public TTableConsumer
{
public:
    explicit TEventLogTableConsumer(std::unique_ptr<TEventLogValueConsumer> valueConsumer)
        : TTableConsumer(NFormats::EComplexTypeMode::Named, valueConsumer.get())
        , ValueConsumer_(std::move(valueConsumer))
    { }

private:
    std::unique_ptr<TEventLogValueConsumer> ValueConsumer_;
};

class TEventLogWriter::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl(const TEventLogManagerConfigPtr& config, const NNative::IClientPtr& client, const IInvokerPtr& invoker)
        : Config_(config)
        , Client_(client)
    {
        YT_VERIFY(Config_->Path);

        auto nameTable = New<TNameTable>();
        auto options = New<NTableClient::TTableWriterOptions>();
        options->EnableValidationOptions();

        EventLogWriter_ = CreateSchemalessBufferedTableWriter(
            Config_,
            options,
            Client_,
            nameTable,
            Config_->Path);

        PendingRowsFlushExecutor_ = New<TPeriodicExecutor>(
            invoker,
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

    void UpdateConfig(const TEventLogManagerConfigPtr& config)
    {
        Config_ = config;
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
    NApi::NNative::IClientPtr Client_;

    NTableClient::IUnversionedWriterPtr EventLogWriter_;

    TMultipleProducerSingleConsumerLockFreeStack<TUnversionedOwningRow> PendingEventLogRows_;
    TPeriodicExecutorPtr PendingRowsFlushExecutor_;

    void AddRow(TUnversionedOwningRow row)
    {
        PendingEventLogRows_.Enqueue(std::move(row));
    }

    void OnPendingEventLogRowsFlush()
    {
        auto owningRows = PendingEventLogRows_.DequeueAll();
        std::vector<TUnversionedRow> rows(owningRows.begin(), owningRows.end());
        EventLogWriter_->Write(rows);
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
    const TEventLogManagerConfigPtr& config,
    const NApi::NNative::IClientPtr& client,
    const IInvokerPtr& invoker)
    : Impl_(New<TImpl>(config, client, invoker))
{ }

TEventLogWriter::~TEventLogWriter()
{ }

std::unique_ptr<IYsonConsumer> TEventLogWriter::CreateConsumer()
{
    return Impl_->CreateConsumer();
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

} // namespace NYT::NEventLog

