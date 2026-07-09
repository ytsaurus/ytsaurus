#pragma once

#include "public.h"

#include "sink_base.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TSyncSinkState
    : public NYTree::TYsonStruct
{
    TMessageId MaxPersistedMessageId;

    REGISTER_YSON_STRUCT(TSyncSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSyncSinkState);

////////////////////////////////////////////////////////////////////////////////

class TSyncSinkBase
    : public TSinkBase
{
public:
    TSyncSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) final;
    void Sync(NApi::IDynamicTableTransactionPtr transaction) final;
    void Commit() final;

protected:
    const NLogging::TLogger Logger;

private:
    TMutableStateClient<TSyncSinkState> State_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::deque<TOutputMessageConstPtr> RegisteredMessages_;

private:
    virtual void DoInit() = 0;
    virtual void DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
