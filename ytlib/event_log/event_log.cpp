#include "event_log.h"

#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/ytlib/table_client/schemaless_writer.h>
#include <yt/ytlib/table_client/schemaless_buffered_table_writer.h>
#include <yt/ytlib/table_client/value_consumer.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/lock_free.h>

namespace NYT {
namespace NEventLog {

using namespace NApi;
using namespace NYson;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFluentEventLogger::TFluentEventLogger()
    : Consumer_(nullptr)
    , Counter_(0)
{ }

TFluentEventLogger::~TFluentEventLogger()
{
    YCHECK(!Consumer_);
}

TFluentLogEvent TFluentEventLogger::LogEventFluently(IYsonConsumer* consumer)
{
    YCHECK(consumer);
    YCHECK(!Consumer_);
    Consumer_ = consumer;
    return TFluentLogEvent(this);
}

void TFluentEventLogger::Acquire()
{
    if (++Counter_ == 1) {
        Consumer_->OnBeginMap();
    }
}

void TFluentEventLogger::Release()
{
    if (--Counter_ == 0) {
        Consumer_->OnEndMap();
        Consumer_ = nullptr;
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
        : TTableConsumer(valueConsumer.get())
        , ValueConsumer_(std::move(valueConsumer))
    { }

private:
    std::unique_ptr<TEventLogValueConsumer> ValueConsumer_;
};

class TEventLogWriter::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl(const TEventLogConfigPtr& config, const INativeClientPtr& client, const IInvokerPtr& invoker)
        : Config_(config)
        , Client_(client)
    {
        YCHECK(Config_->Path);

        auto nameTable = New<TNameTable>();
        auto options = New<NTableClient::TTableWriterOptions>();
        options->EnableValidationOptions();

        EventLogWriter_ = CreateSchemalessBufferedTableWriter(
            Config_,
            options,
            Client_,
            nameTable,
            Config_->Path);

        // Open is always synchronous for buffered writer.
        YCHECK(EventLogWriter_->Open().IsSet());

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
        auto tableConsumer = std::make_unique<TEventLogTableConsumer>(std::move(valueConsumer));
        return std::move(tableConsumer);
    }

    void UpdateConfig(const TEventLogConfigPtr& config)
    {
        Config_ = config;
        PendingRowsFlushExecutor_->SetPeriod(Config_->PendingRowsFlushPeriod);
    }

private:
    TEventLogConfigPtr Config_;
    NApi::INativeClientPtr Client_;

    NTableClient::ISchemalessWriterPtr EventLogWriter_;

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
};

////////////////////////////////////////////////////////////////////////////////

TEventLogWriter::TEventLogWriter(
    const TEventLogConfigPtr& config,
    const NApi::INativeClientPtr& client,
    const IInvokerPtr& invoker)
    : Impl_(New<TImpl>(config, client, invoker))
{ }

std::unique_ptr<IYsonConsumer> TEventLogWriter::CreateConsumer()
{
    return Impl_->CreateConsumer();
}

void TEventLogWriter::UpdateConfig(const TEventLogConfigPtr& config)
{
    Impl_->UpdateConfig(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEventLog
} // namespace NYT

