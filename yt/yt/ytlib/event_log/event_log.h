#pragma once

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <atomic>

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

template <class TParent>
class TFluentLogEventImpl;

using TFluentLogEvent = TFluentLogEventImpl<NYTree::TFluentYsonVoid>;

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TFluentLogEventImpl, TParent, NYTree::TFluentMap>
{
public:
    using TThis = TFluentLogEventImpl;
    using TBase = NYTree::TFluentYsonBuilder::TFluentFragmentBase<NEventLog::TFluentLogEventImpl, TParent, NYTree::TFluentMap>;

    TFluentLogEventImpl(std::unique_ptr<NYson::IYsonConsumer> consumer);

    TFluentLogEventImpl(TFluentLogEventImpl&& other) = default;

    ~TFluentLogEventImpl();

    TFluentLogEventImpl& operator = (TFluentLogEventImpl&& other) = default;

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(TStringBuf key);
    TThis& Items(const NYson::TYsonString& items);

private:
    std::unique_ptr<NYson::IYsonConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

class IEventLogWriter
    : public TRefCounted
{
public:
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() = 0;

    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) = 0;

    virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IEventLogWriter)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Hide implementation.
class TEventLogWriter
    : public IEventLogWriter
{
public:
    TEventLogWriter(
        TEventLogManagerConfigPtr config,
        IInvokerPtr invoker,
        NTableClient::IUnversionedWriterPtr writer);

    ~TEventLogWriter();

    std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() override;

    void UpdateConfig(const TEventLogManagerConfigPtr& config) override;

    TFuture<void> Close() override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

using TEventLogWriterPtr = TIntrusivePtr<TEventLogWriter>;

////////////////////////////////////////////////////////////////////////////////

TEventLogWriterPtr CreateStaticTableEventLogWriter(
    TEventLogManagerConfigPtr config,
    NApi::NNative::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog

#define EVENT_LOG_INL_H_
#include "event_log-inl.h"
#undef EVENT_LOG_INL_H_
