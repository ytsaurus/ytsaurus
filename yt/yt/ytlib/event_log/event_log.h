#pragma once

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <atomic>

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, EventLogWriterLogger, "EventLogWriter");

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

    TFluentLogEvent(std::unique_ptr<NYson::IYsonConsumer> consumer);

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

    TEventLogManagerConfigPtr GetConfig() const override;

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
    IInvokerPtr invoker,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
