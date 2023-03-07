#pragma once

#include "config.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

#include <atomic>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

class TFluentLogEventConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    TFluentLogEventConsumer(IYsonConsumer* tableConsumer, const NLogging::TLogger* logger);

protected:
    void OnMyBeginMap();

    void OnMyEndMap();

private:
    using TState = NYTree::TFluentYsonWriterState;
    using TStatePtr = TIntrusivePtr<NYTree::TFluentYsonWriterState>;

    TStatePtr State_;
    const NLogging::TLogger* Logger_;

    IYsonConsumer* const TableConsumer_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl;

typedef TFluentLogEventImpl<NYTree::TFluentYsonVoid> TFluentLogEvent;

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TFluentLogEventImpl, TParent, NYTree::TFluentMap>
{
public:
    typedef TFluentLogEventImpl TThis;
    typedef NYTree::TFluentYsonBuilder::TFluentFragmentBase<NEventLog::TFluentLogEventImpl, TParent, NYTree::TFluentMap> TBase;

    TFluentLogEventImpl(std::unique_ptr<NYson::IYsonConsumer> consumer);

    TFluentLogEventImpl(TFluentLogEventImpl&& other) = default;

    ~TFluentLogEventImpl();

    TFluentLogEventImpl& operator = (TFluentLogEventImpl&& other) = default;

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(TStringBuf key);

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

class IEventLogWriter
    : public TIntrinsicRefCounted
{
public:
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() = 0;

    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) = 0;

    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IEventLogWriter);

////////////////////////////////////////////////////////////////////////////////

class TEventLogWriter
    : public IEventLogWriter
{
public:
    TEventLogWriter(
        const TEventLogManagerConfigPtr& config,
        const NApi::NNative::IClientPtr& client,
        const IInvokerPtr& invoker);

    ~TEventLogWriter();

    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() override;

    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) override;

    TFuture<void> Close() override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog

#define EVENT_LOG_INL_H_
#include "event_log-inl.h"
#undef EVENT_LOG_INL_H_
