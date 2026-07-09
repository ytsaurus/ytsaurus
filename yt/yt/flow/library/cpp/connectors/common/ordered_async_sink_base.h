#pragma once

#include "public.h"

#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedAsyncSinkState
    : public NYTree::TYsonStruct
{
    std::string ProducerId;
    TMessageId MaxPersistedMessageId;
    i64 MaxPersistedSeqNo{};

    REGISTER_YSON_STRUCT(TOrderedAsyncSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOrderedAsyncSinkState);

////////////////////////////////////////////////////////////////////////////////

class TOrderedAsyncSinkBase
    : public TSinkBase
{
public:
    TOrderedAsyncSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

protected:
    const NLogging::TLogger Logger;

private:
    struct TRequest
    {
        TOutputMessageConstPtr Message;
        i64 SeqNo;
        TOnDistributedCallback Callback;
    };

    TMutableStateClient<TOrderedAsyncSinkState> State_;
    TMessageId LastDistributedMessageId_ = {};
    i64 LastDistributedSeqNo_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TRequest> RegisteredRequests_;
    std::deque<TRequest> DistributedRequests_;
    std::deque<TRequest> PersistedRequests_;

private:
    virtual void DoInit(const std::string& producerId) = 0;
    virtual TFuture<void> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
