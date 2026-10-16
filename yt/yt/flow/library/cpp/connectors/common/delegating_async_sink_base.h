#pragma once

#include "public.h"

#include "async_at_most_once_sink_base.h"
#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/state.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TDelegatingAsyncSinkParameters
    : public TSinkBase::TParameters
{
    TAtMostOnceStrategyParametersPtr AtMostOnceStrategy;

    REGISTER_YSON_STRUCT(TDelegatingAsyncSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDelegatingAsyncSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDelegatingAsyncSinkDynamicParameters
    : public TSinkBase::TDynamicParameters
{
    TAtMostOnceStrategyDynamicParametersPtr AtMostOnceStrategy;

    REGISTER_YSON_STRUCT(TDelegatingAsyncSinkDynamicParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDelegatingAsyncSinkDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

class TDelegatingAsyncSinkBase
    : public TSinkBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TDelegatingAsyncSinkParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDelegatingAsyncSinkDynamicParameters);

    using TThis = TDelegatingAsyncSinkBase;

    TDelegatingAsyncSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    ~TDelegatingAsyncSinkBase() override;

    void Init(IInitContextPtr initContext) override;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) override;
    void Commit() override;

protected:
    virtual void DoInit(const std::string& producerId) = 0;
    virtual std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) = 0;

    void SuspendDestructionGuarded(std::vector<TIntrusivePtr<TRefCounted>> prevent);

private:
    void ReconfigureAtMostOnce(const TDynamicSinkContextPtr& dynamicContext);

    TIntrusivePtr<ISink> Delegate_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
