#pragma once

#include "public.h"

#include <core/misc/enum.h>

#include <core/ytree/fluent.h>

#include <atomic>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogEventType,
    (SchedulerStarted)
    (MasterConnected)
    (MasterDisconnected)
    (JobStarted)
    (JobCompleted)
    (JobFailed)
    (JobAborted)
    (OperationStarted)
    (OperationCompleted)
    (OperationFailed)
    (OperationAborted)
    (FairShareInfo)
);

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl;

typedef TFluentLogEventImpl<NYTree::TFluentYsonVoid> TFluentLogEvent;

////////////////////////////////////////////////////////////////////////////////

class TFluentEventLogger
{
public:
    TFluentEventLogger();
    ~TFluentEventLogger();

    TFluentLogEvent LogEventFluently(NYson::IYsonConsumer* consumer);

private:
    template <class TParent>
    friend class TFluentLogEventImpl;

    NYson::IYsonConsumer* Consumer_;
    std::atomic<int> Counter_;


    void Acquire();
    void Release();

};

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TFluentLogEventImpl, TParent>
{
public:
    typedef TFluentLogEventImpl TThis;
    typedef NYTree::TFluentYsonBuilder::TFluentFragmentBase<NScheduler::TFluentLogEventImpl, TParent> TBase;

    explicit TFluentLogEventImpl(TFluentEventLogger* logger)
        : TBase(logger->Consumer_)
        , Logger_(logger)
    {
        Acquire();
    }

    explicit TFluentLogEventImpl(NYson::IYsonConsumer* consumer)
        : TBase(consumer)
        , Logger_(nullptr)
    { }

    TFluentLogEventImpl(TFluentLogEventImpl&& other)
        : TBase(other.Consumer, other.Parent)
        , Logger_(other.Logger_)
    {
        other.Logger_ = nullptr;
    }

    TFluentLogEventImpl(const TFluentLogEventImpl& other)
        : TBase(other.Consumer, other.Parent)
        , Logger_(other.Logger_)
    {
        Acquire();
    }

    ~TFluentLogEventImpl()
    {
        Release();
    }

    TFluentLogEventImpl& operator = (TFluentLogEventImpl&& other) = delete;
    TFluentLogEventImpl& operator = (const TFluentLogEventImpl& other) = delete;

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(const TStringBuf& key)
    {
        this->Consumer->OnKeyedItem(key);
        return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
    }

private:
    TFluentEventLogger* Logger_;


    void Acquire()
    {
        if (this->Logger_) {
            this->Logger_->Acquire();
        }
    }

    void Release()
    {
        if (this->Logger_) {
            this->Logger_->Release();
            this->Logger_ = nullptr;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IEventLogHost
{
public:
    virtual ~IEventLogHost()
    { }

    virtual TFluentLogEvent LogEventFluently(ELogEventType eventType) = 0;
};

class TEventLogHostBase
    : public virtual IEventLogHost
{
public:
    virtual TFluentLogEvent LogEventFluently(ELogEventType eventType) override;

protected:
    virtual NYson::IYsonConsumer* GetEventLogConsumer() = 0;

private:
    TFluentEventLogger EventLogger_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
