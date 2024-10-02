#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/ytlib/event_log/event_log.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TEventLogger;

class TFluentLogEventWrapper
    : public NYTree::TFluentMap
{
    friend TEventLogger;

    class TLogState;

    using TFluentState = NYTree::TFluentYsonWriterState;
    using TFluentStatePtr = TIntrusivePtr<TFluentState>;

    using TConstEventLoggerPtr = TIntrusivePtr<const TEventLogger>;

public:
    ~TFluentLogEventWrapper();

private:
    TFluentLogEventWrapper(
        std::unique_ptr<NYson::IYsonConsumer> stateMultiplexingConsumer,
        std::unique_ptr<TLogState> logState,
        TFluentStatePtr fluentState,
        NLogging::ELogLevel logLevel,
        TString description,
        TConstEventLoggerPtr eventLogger);

    const std::unique_ptr<NYson::IYsonConsumer> StateMultiplexingConsumerHolder_;

    const std::unique_ptr<TLogState> LogState_;
    const TFluentStatePtr FluentState_;

    const NLogging::ELogLevel LogLevel_;
    const TString Description_;

    const TConstEventLoggerPtr EventLogger_;
};

////////////////////////////////////////////////////////////////////////////////

struct TEventLogOptions
{
    bool WriteTextLog = true;
    bool WriteTableLog = true;
};

////////////////////////////////////////////////////////////////////////////////

class TEventLogger
    : public TRefCounted
{
    friend TFluentLogEventWrapper;

public:
    TEventLogger(
        NLogging::TLogger logger,
        TString hostFqdn,
        NObjects::TTransactionManagerPtr transactionManager,
        NEventLog::IEventLogWriterPtr writer);

    TFluentLogEventWrapper LogFluently(
        NLogging::ELogLevel logLevel,
        TString description,
        TEventLogOptions options = {}) const;
    TEventLoggerPtr WithTag(TStringBuf tag) const;

private:
    const NLogging::TLogger Logger_;
    const TString HostFqdn_;
    const NObjects::TTransactionManagerPtr TransactionManager_;
    const NEventLog::IEventLogWriterPtr Writer_;
    const std::unique_ptr<NYson::IYsonConsumer> TableConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

NEventLog::TEventLogWriterPtr CreateDynamicTableEventLogWriter(
    TEventLogManagerConfigPtr config,
    NYPath::TYPath eventLogPath,
    NYT::NApi::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
