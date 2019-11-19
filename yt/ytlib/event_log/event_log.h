#pragma once

#include "config.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

#include <atomic>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl;

typedef TFluentLogEventImpl<NYTree::TFluentYsonVoid> TFluentLogEvent;

////////////////////////////////////////////////////////////////////////////////

class TFluentEventLogger
{
public:
    ~TFluentEventLogger();

    TFluentLogEvent LogEventFluently(NYson::IYsonConsumer* consumer);

private:
    template <class TParent>
    friend class TFluentLogEventImpl;

    NYson::IYsonConsumer* Consumer_ = nullptr;
    std::atomic<int> Counter_ = {0};

    void Acquire();
    void Release();
};

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TFluentLogEventImpl, TParent, NYTree::TFluentMap>
{
public:
    typedef TFluentLogEventImpl TThis;
    typedef NYTree::TFluentYsonBuilder::TFluentFragmentBase<NEventLog::TFluentLogEventImpl, TParent, NYTree::TFluentMap> TBase;

    explicit TFluentLogEventImpl(TFluentEventLogger* logger);
    explicit TFluentLogEventImpl(NYson::IYsonConsumer* consumer);

    TFluentLogEventImpl(TFluentLogEventImpl&& other);
    TFluentLogEventImpl(const TFluentLogEventImpl& other);

    ~TFluentLogEventImpl();

    TFluentLogEventImpl& operator = (TFluentLogEventImpl&& other) = delete;
    TFluentLogEventImpl& operator = (const TFluentLogEventImpl& other) = delete;

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(TStringBuf key);

private:
    TFluentEventLogger* Logger_;

    void Acquire();
    void Release();
};

////////////////////////////////////////////////////////////////////////////////

class IEventLogWriter
    : public TIntrinsicRefCounted
{
public:
    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() = 0;

    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) = 0;
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

    virtual std::unique_ptr<NYson::IYsonConsumer> CreateConsumer() override;

    virtual void UpdateConfig(const TEventLogManagerConfigPtr& config) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog

#define EVENT_LOG_INL_H_
#include "event_log-inl.h"
#undef EVENT_LOG_INL_H_
