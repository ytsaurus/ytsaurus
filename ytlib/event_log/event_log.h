#pragma once

#include "config.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/core/misc/enum.h>

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

class TEventLogWriter
    : public TIntrinsicRefCounted
{
public:
    TEventLogWriter(
        const TEventLogConfigPtr& config,
        const NApi::NNative::IClientPtr& client,
        const IInvokerPtr& invoker);

    std::unique_ptr<NYson::IYsonConsumer> CreateConsumer();

    void UpdateConfig(const TEventLogConfigPtr& config);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEventLogWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog

#define EVENT_LOG_INL_H_
#include "event_log-inl.h"
#undef EVENT_LOG_INL_H_
