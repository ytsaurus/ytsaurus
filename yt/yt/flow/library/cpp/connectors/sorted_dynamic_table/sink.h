#pragma once

#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/logging/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NSortedDynamicTable {

////////////////////////////////////////////////////////////////////////////////

struct TInfoControllerState
    : public NYTree::TYsonStruct
{
    std::optional<int> CachedPartitionCount;

    REGISTER_YSON_STRUCT(TInfoControllerState);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_TYPE(TInfoControllerState);

////////////////////////////////////////////////////////////////////////////////

class TSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSinkControllerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicSinkControllerParameters);

    TSinkController(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;
    std::optional<i64> GetReceiverChannelCount() override;

private:
    void TryUpdatePartitionCount();

private:
    const NApi::IClientPtr Client_;
    const IStatusErrorStatePtr UpdatePartitionCountErrorState_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StateLock_);
    TMutableStateClient<TInfoControllerState> State_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
};

DECLARE_REFCOUNTED_TYPE(TSinkController);

////////////////////////////////////////////////////////////////////////////////

class TSyncSink
    : public TSyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TSyncSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicSyncSinkParameters);

    using TSinkController = TSinkController;

    TSyncSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

protected:
    const NLogging::TLogger Logger;

private:
    const NTableClient::TNameTablePtr NameTable_;

private:
    void DoInit() final;
    void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) final;
};

DECLARE_REFCOUNTED_TYPE(TSyncSink);

////////////////////////////////////////////////////////////////////////////////

class TAsyncSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TAsyncSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicAsyncSinkParameters);

    using TSinkController = TSinkController;

    TAsyncSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

private:
    const NLogging::TLogger Logger;
    const NApi::IClientPtr Client_;
    const NTableClient::TNameTablePtr NameTable_;
    const IStatusErrorStatePtr WriteErrorState_;
    const NConcurrency::TAsyncSemaphorePtr WriteSemaphore_;

private:
    void DoInit(const std::string& producerId) final;
    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) final;

    bool TryWriteBatch(const std::vector<TOutputMessageConstPtr>& messages);
};

DECLARE_REFCOUNTED_TYPE(TAsyncSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable
