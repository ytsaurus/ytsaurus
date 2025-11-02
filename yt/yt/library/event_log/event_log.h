#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

class TFluentLogEventConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    explicit TFluentLogEventConsumer(const NLogging::TLogger* logger);

protected:
    void OnMyBeginMap() override;

    void OnMyEndMap() override;

private:
    const NLogging::TLogger* Logger_;

    using TState = NYTree::TFluentYsonWriterState;
    using TStatePtr = TIntrusivePtr<NYTree::TFluentYsonWriterState>;

    TStatePtr State_;
};

////////////////////////////////////////////////////////////////////////////////

class TFluentLogEvent
    : public NYTree::TFluentYsonBuilder::TFluentMapFragmentBase<NYTree::TFluentYsonVoid, TFluentLogEvent&&>
{
public:
    using TThis = TFluentLogEvent;
    using TBase = NYTree::TFluentYsonBuilder::TFluentMapFragmentBase<NYTree::TFluentYsonVoid, TThis&&>;

    explicit TFluentLogEvent(std::unique_ptr<NYson::IYsonConsumer> consumer);

    TFluentLogEvent(TFluentLogEvent&& other) = default;
    TFluentLogEvent(const TFluentLogEvent& other) = delete;

    ~TFluentLogEvent();

    TFluentLogEvent& operator=(TFluentLogEvent&& other) = default;
    TFluentLogEvent& operator=(const TFluentLogEvent& other) = delete;

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

struct IEventLogWriter
    : public TRefCounted
{
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() = 0;

    virtual TEventLogManagerConfigPtr GetConfig() const = 0;
    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) = 0;

    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IEventLogWriter)

////////////////////////////////////////////////////////////////////////////////

IEventLogWriterPtr CreateEventLogWriter(
    TEventLogManagerConfigPtr config,
    IInvokerPtr invoker,
    NTableClient::IUnversionedWriterPtr logWriter,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
