#include "event_log.h"

#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/client/table_client/schemaless_dynamic_table_writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/null_consumer.h>

#include <util/string/cast.h>
#include <util/string/escape.h>

namespace NYT::NOrm::NServer::NMaster {

using namespace NYT::NApi;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTransactionClient;

using namespace NLogging;

using namespace NYPath;
using namespace NYTree;

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const NYT::NLogging::TLogger Logger("EventLogger");

////////////////////////////////////////////////////////////////////////////////

class TLogFriendlyYsonWriter
    : public TYsonConsumerBase
    , public virtual IFlushableYsonConsumer
    , private TNonCopyable
{
    static constexpr char ItemSeparatorSymbol = ',';
    static constexpr char KeyValueSeparatorSymbol = ':';
    static constexpr char BeginMapSymbol = '(';
    static constexpr char EndMapSymbol = ')';

public:
    explicit TLogFriendlyYsonWriter(IOutputStream* stream)
        : Stream_(stream)
    {
        YT_ASSERT(Stream_);
    }

    void OnStringScalar(TStringBuf value) override
    {
        StartNode();
        WriteStringScalar(value);
    }

    void OnInt64Scalar(i64 value) override
    {
        StartNode();
        Stream_->Write(ToString(value));
    }

    void OnUint64Scalar(ui64 value) override
    {
        StartNode();
        Stream_->Write(ToString(value));
        Stream_->Write("u");
    }

    void OnDoubleScalar(double value) override
    {
        StartNode();
        Stream_->Write(FloatToString(value));
    }

    void OnBooleanScalar(bool value) override
    {
        StartNode();
        Stream_->Write(value ? TStringBuf("%true") : TStringBuf("%false"));
    }

    void OnEntity() override
    {
        StartNode();
        Stream_->Write(NYson::NDetail::EntitySymbol);
    }

    void OnBeginList() override
    {
        StartNode(false);
        BeginCollection(NYson::NDetail::BeginListSymbol);
    }

    void OnListItem() override
    {
    }

    void OnEndList() override
    {
        EndCollection(NYson::NDetail::EndListSymbol);
    }

    void OnBeginMap() override
    {
        StartNode(false);
        BeginCollection(BeginMapSymbol);
    }

    void OnKeyedItem(TStringBuf key) override
    {
        StartNode(false);
        WriteStringWithEscaping(UnderscoreCaseToCamelCase(key));

        Stream_->Write(KeyValueSeparatorSymbol);
        Stream_->Write(' ');
    }

    void OnEndMap() override
    {
        EndCollection(EndMapSymbol);
    }

    void OnBeginAttributes() override
    {
        BeginCollection(NYson::NDetail::BeginAttributesSymbol);
    }

    void OnEndAttributes() override
    {
        EndCollection(NYson::NDetail::EndAttributesSymbol);
    }

    using IYsonConsumer::OnRaw;
    void OnRaw(TStringBuf yson, EYsonType type = EYsonType::Node) override
    {
        if (type == EYsonType::Node) {
            StartNode();
        }
        Stream_->Write(yson);
    }

    void Flush() override
    { }

protected:
    IOutputStream* const Stream_;

    bool NeedComma_ = false;
    int Depth_ = 0;

    void StartNode(bool needComma = true)
    {
        std::swap(NeedComma_, needComma);
        if (!needComma) {
            return;
        }
        Stream_->Write(ItemSeparatorSymbol);
        Stream_->Write(' ');
    }

    void BeginCollection(char ch)
    {
        ++Depth_;
        Stream_->Write(ch);
    }

    void EndCollection(char ch)
    {
        --Depth_;
        Stream_->Write(ch);
    }

    void WriteStringScalar(TStringBuf value)
    {
        Stream_->Write('"');
        WriteStringWithEscaping(value);
        Stream_->Write('"');
    }

    void WriteStringWithEscaping(TStringBuf value)
    {
        Stream_->Write(EscapeC(value.data(), value.length()));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFluentLogEventWrapper::TLogState
{
public:
    TLogState() : Writer(&Output)
    { }

    TLogState(TLogState&&) = delete;
    TLogState(const TLogState&) = delete;

    TLogState& operator=(TLogState&&) = delete;
    TLogState& operator=(const TLogState&) = delete;

    TString& Str()
    {
        return Output.Str();
    }

    IYsonConsumer* GetConsumer()
    {
        return &Writer;
    }

private:
    TStringStream Output;
    TLogFriendlyYsonWriter Writer;
};

////////////////////////////////////////////////////////////////////////////////

class TMultiplexingYsonConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    TMultiplexingYsonConsumer(std::vector<IYsonConsumer*> consumers)
    {
        Forward(std::move(consumers), nullptr, EYsonType::MapFragment);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFluentLogEventWrapper::TFluentLogEventWrapper(
    std::unique_ptr<NYson::IYsonConsumer> stateMultiplexingConsumer,
    std::unique_ptr<TLogState> logState,
    TFluentStatePtr fluentState,
    ELogLevel logLevel,
    TString description,
    TConstEventLoggerPtr eventLogger)
    : TFluentMap(BuildYsonMapFragmentFluently(stateMultiplexingConsumer.get()))
    , StateMultiplexingConsumerHolder_(std::move(stateMultiplexingConsumer))
    , LogState_(std::move(logState))
    , FluentState_(std::move(fluentState))
    , LogLevel_(std::move(logLevel))
    , Description_(std::move(description))
    , EventLogger_(std::move(eventLogger))
{ }

TFluentLogEventWrapper::~TFluentLogEventWrapper()
{
    try {
        if (LogState_) {
            const auto& textPayload = LogState_->Str();
            if (textPayload.empty()) {
                YT_LOG_EVENT(EventLogger_->Logger_, LogLevel_, "%v", Description_);
            } else {
                YT_LOG_EVENT(EventLogger_->Logger_,
                    LogLevel_,
                    "%v (%v)",
                    Description_,
                    textPayload);
            }
        }
    } catch (...) {
        YT_LOG_EVENT(EventLogger_->Logger_, ELogLevel::Error, "EventLog text log write failed");
    }

    if (EventLogger_->Writer_->GetConfig()->Enable && FluentState_) {
        try {
            auto timestamp = EventLogger_->TransactionManager_->GenerateTimestampBuffered();
            auto datetime = TInstant::Now();
            BuildYsonFluently(EventLogger_->TableConsumer_.get())
                .BeginMap()
                    .Item("timestamp").Value(timestamp)
                    .Item("datetime").Value(datetime.ToString())
                    .Item("source").Value(EventLogger_->Logger_.GetCategory()->Name)
                    .Item("host").Value(EventLogger_->HostFqdn_)
                    .Item("level").Value(LogLevel_)
                    .Item("description").Value(Description_)
                    .Item("payload").BeginMap()
                        .Items(FluentState_->GetValue())
                        .OptionalItem("tag", EventLogger_->Logger_.GetTag())
                    .EndMap()
                .EndMap();
        } catch (...) {
            YT_LOG_EVENT(EventLogger_->Logger_, ELogLevel::Error, "EventLog table write failed");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TEventLogger::TEventLogger(
    TLogger logger,
    TString hostFqdn,
    NObjects::TTransactionManagerPtr transactionManager,
    NEventLog::IEventLogWriterPtr writer)
    : Logger_(std::move(logger))
    , HostFqdn_(std::move(hostFqdn))
    , TransactionManager_(std::move(transactionManager))
    , Writer_(std::move(writer))
    , TableConsumer_(Writer_->CreateConsumer())
{ }

TFluentLogEventWrapper TEventLogger::LogFluently(
    ELogLevel logLevel,
    TString description,
    TEventLogOptions options) const
{
    std::unique_ptr<TFluentLogEventWrapper::TLogState> logState = nullptr;
    TFluentLogEventWrapper::TFluentStatePtr fluentState = nullptr;

    std::vector<IYsonConsumer*> consumers;
    if (options.WriteTextLog) {
        logState = std::make_unique<TFluentLogEventWrapper::TLogState>();
        consumers.push_back(logState->GetConsumer());
    }
    if (options.WriteTableLog) {
        fluentState = New<TFluentLogEventWrapper::TFluentState>(
            EYsonFormat::Text,
            EYsonType::MapFragment);
        consumers.push_back(fluentState->GetConsumer());
    }

    if (consumers.empty()) {
        consumers.push_back(GetNullYsonConsumer());
    }

    // TODO(rogday): Replace TLogState with TYsonStringBuilder and
    // Create TFluentYsonStringBuilder and use it instead of TFluentYsonWriterState
    // Then, cache this multiplexing consumer in TFlsSlot
    auto stateMultiplexingConsumer = std::make_unique<TMultiplexingYsonConsumer>(std::move(consumers));

    return TFluentLogEventWrapper(
        std::move(stateMultiplexingConsumer),
        std::move(logState),
        std::move(fluentState),
        std::move(logLevel),
        std::move(description),
        MakeStrong(this));
}

TEventLoggerPtr TEventLogger::WithTag(TStringBuf tag) const
{
    return New<TEventLogger>(
        Logger_.WithTag(tag.data()),
        HostFqdn_,
        TransactionManager_,
        Writer_);
}

DEFINE_REFCOUNTED_TYPE(TEventLogger);

////////////////////////////////////////////////////////////////////////////////

NEventLog::TEventLogWriterPtr CreateDynamicTableEventLogWriter(
    TEventLogManagerConfigPtr config,
    NYPath::TYPath eventLogPath,
    IClientPtr client,
    IInvokerPtr invoker)
{
    auto eventLogWriter = CreateSchemalessDynamicTableWriter(std::move(eventLogPath), std::move(client));
    return New<NEventLog::TEventLogWriter>(
        config->AsYTEventLogManagerConfig(),
        std::move(invoker),
        std::move(eventLogWriter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
