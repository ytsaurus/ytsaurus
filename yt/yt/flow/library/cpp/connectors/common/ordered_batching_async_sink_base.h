#pragma once

#include "public.h"

#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TOrderedBatchingAsyncSinkState
    : public NYTree::TYsonStruct
{
    std::string ProducerId;
    TMessageId MaxPersistedMessageId;
    i64 MaxPersistedSeqNo{};
    std::deque<TMessageId> BatchBounds;

    REGISTER_YSON_STRUCT(TOrderedBatchingAsyncSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOrderedBatchingAsyncSinkState);

////////////////////////////////////////////////////////////////////////////////

class TOrderedBatchingAsyncSinkBase
    : public TSinkBase
{
private:
    struct TExtendedDynamicParameters
        : public virtual TSinkBase::TDynamicParameters
    {
        i64 MaxRowsPerBatch{};
        i64 MaxBytesPerBatch{};

        REGISTER_YSON_STRUCT(TExtendedDynamicParameters);

        static void Register(TRegistrar registrar);
    };

public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TExtendedDynamicParameters);

    TOrderedBatchingAsyncSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

protected:
    const NLogging::TLogger Logger;

private:
    using TBatch = std::vector<TOutputMessageConstPtr>;

    struct TRequest
    {
        i64 SeqNo = 0;
        TBatch Batch = {};
        i64 ByteSize = 0;
        std::vector<TOnDistributedCallback> Callbacks;
    };

    TMutableStateClient<TOrderedBatchingAsyncSinkState> State_;
    TMessageId LastMessageId_ = {};
    i64 LastSeqNo_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TMessageId> OldBounds_;
    TRequest Request_;

    std::deque<TRequest> RegisteredRequests_;
    std::deque<TRequest> DistributedRequests_;
    std::deque<TRequest> PersistedRequests_;

private:
    void NextBatch();
    void CheckBatch();
    void FlushBatch();

private:
    virtual void DoInit(const std::string& producerId) = 0;
    virtual TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
