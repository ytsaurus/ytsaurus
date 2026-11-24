#pragma once

#include "public.h"

#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TYdbSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbSinkControllerParameters);

    TYdbSinkController(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() override;
    void Commit() final;
    std::optional<i64> GetReceiverChannelCount() override;

protected:
    const TYdbSinkControllerEnginePtr Engine_;
};

DEFINE_REFCOUNTED_TYPE(TYdbSinkController);

////////////////////////////////////////////////////////////////////////////////

class TYdbSink
    : public TOrderedAsyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbSinkParameters);
    using TSinkController = TYdbSinkController;

    TYdbSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    const TYdbSinkEnginePtr Engine_;

    void DoInit(const std::string& producerId) final;
    TFuture<void> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) final;
};

DEFINE_REFCOUNTED_TYPE(TYdbSink);

////////////////////////////////////////////////////////////////////////////////

class TYdbFramingSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbFramingSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbFramingSinkParameters);
    using TSinkController = TYdbSinkController;

    TYdbFramingSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    const TYdbSinkEnginePtr Engine_;

    void DoInit(const std::string& producerId) final;
    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) final;
};

DEFINE_REFCOUNTED_TYPE(TYdbFramingSink);

////////////////////////////////////////////////////////////////////////////////

class TYdbAtLeastOnceSink
    : public TSyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbAtLeastOnceSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbAtLeastOnceSinkParameters);
    using TSinkController = TYdbSinkController;

    TYdbAtLeastOnceSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    const TYdbSinkEnginePtr Engine_;

    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;
};

DEFINE_REFCOUNTED_TYPE(TYdbAtLeastOnceSink);

////////////////////////////////////////////////////////////////////////////////

class TYdbAtLeastOnceFramingSink
    : public TSyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbAtLeastOnceFramingSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbAtLeastOnceFramingSinkParameters);
    using TSinkController = TYdbSinkController;

    TYdbAtLeastOnceFramingSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    const TYdbSinkEnginePtr Engine_;
    std::shared_ptr<const TYdbBatchingOptions> Batching_;

    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;
};

DEFINE_REFCOUNTED_TYPE(TYdbAtLeastOnceFramingSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
