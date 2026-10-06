#pragma once

#include "public.h"

#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TAsyncAtMostOnceSinkState
    : public NYTree::TYsonStruct
{
    TMessageId MaxPersistedMessageId;

    REGISTER_YSON_STRUCT(TAsyncAtMostOnceSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAsyncAtMostOnceSinkState);

////////////////////////////////////////////////////////////////////////////////

struct TAtMostOnceStrategyParameters
    : public NYTree::TYsonStruct
{
    bool Enabled{};

    REGISTER_YSON_STRUCT(TAtMostOnceStrategyParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAtMostOnceStrategyParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAtMostOnceStrategyDynamicParameters
    : public NYTree::TYsonStruct
{
    TDuration SuspendDestructionDuration;
    NYTree::TSize TotalQueueBytesLimit;

    REGISTER_YSON_STRUCT(TAtMostOnceStrategyDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAtMostOnceStrategyDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

class TAsyncAtMostOnceSinkBase
    : public TSinkBase
{
public:
    TAsyncAtMostOnceSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

    void Reconfigure(TAtMostOnceStrategyDynamicParametersPtr parameters);

    void SuspendDestructionGuarded(std::vector<TIntrusivePtr<TRefCounted>> prevent);

protected:
    const NLogging::TLogger Logger;

private:
    struct TRequest
    {
        TOutputMessageConstPtr Message;
        i64 SeqNo = 0;
    };

    TMutableStateClient<TAsyncAtMostOnceSinkState> State_;
    std::string ProducerId_;
    i64 LastDistributedSeqNo_ = 0;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::deque<TRequest> RegisteredRequests_;
    NConcurrency::TAsyncSemaphorePtr QueueSizeSemaphore_;
    TAtMostOnceStrategyDynamicParametersPtr DynamicParameters_;

private:
    virtual void DoInit(const std::string& producerId) = 0;
    virtual std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
