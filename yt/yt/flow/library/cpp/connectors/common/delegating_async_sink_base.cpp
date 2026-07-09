#include "delegating_async_sink_base.h"
#include "ordered_async_sink_base.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Class derived from TBase.
//! Forward DoInit and DoDistribute calls to outer class.
template <class TBase>
class TCallbackAdapter
    : public TBase
{
public:
    using TInitCallback = TCallback<void(const std::string&)>;
    using TDistributeCallback = TCallback<std::pair<TFuture<void>, ui64>(const TOutputMessageConstPtr&, i64)>;

    TCallbackAdapter(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext,
        TInitCallback initCallback,
        TDistributeCallback distributeCallback)
        : TBase(std::move(context), std::move(dynamicContext))
        , InitCallback_(std::move(initCallback))
        , DistributeCallback_(std::move(distributeCallback))
    { }

    void DoInit(const std::string& producerId) override
    {
        InitCallback_(producerId);
    }

private:
    TInitCallback InitCallback_;

protected:
    TDistributeCallback DistributeCallback_;
};

////////////////////////////////////////////////////////////////////////////////

class TOrderedCallbackAdapter : public TCallbackAdapter<TOrderedAsyncSinkBase>
{
public:
    using TCallbackAdapter::TCallbackAdapter;

    TFuture<void> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) override
    {
        return DistributeCallback_(message, seqNo).first;
    }
};

DEFINE_REFCOUNTED_TYPE(TOrderedCallbackAdapter);

////////////////////////////////////////////////////////////////////////////////

class TAtMostOnceCallbackAdapter : public TCallbackAdapter<TAsyncAtMostOnceSinkBase>
{
public:
    using TCallbackAdapter::TCallbackAdapter;

    std::pair<TFuture<void>, ui64> DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo) override
    {
        return DistributeCallback_(message, seqNo);
    }
};

DEFINE_REFCOUNTED_TYPE(TAtMostOnceCallbackAdapter);

////////////////////////////////////////////////////////////////////////////////

void TDelegatingAsyncSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("at_most_once_strategy", &TThis::AtMostOnceStrategy)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TDelegatingAsyncSinkDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("at_most_once_strategy", &TThis::AtMostOnceStrategy)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TDelegatingAsyncSinkBase::TDelegatingAsyncSinkBase(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(context, dynamicContext)
{
    auto initCb = BIND(&TThis::DoInit, MakeWeak(this));
    auto distributeCb = BIND(ThrowOnDestroyed(&TThis::DoDistribute), MakeWeak(this));

    if (GetParameters()->AtMostOnceStrategy && GetParameters()->AtMostOnceStrategy->Enabled) {
        Delegate_ = New<TAtMostOnceCallbackAdapter>(
            GetContext(),
            GetDynamicContext(),
            std::move(initCb),
            std::move(distributeCb));
        // Should be moved to constructor.
        ReconfigureAtMostOnce(GetDynamicContext());
        SubscribeReconfigured(BIND(&TThis::ReconfigureAtMostOnce, MakeWeak(this)));
    } else {
        Delegate_ = New<TOrderedCallbackAdapter>(
            GetContext(),
            GetDynamicContext(),
            std::move(initCb),
            std::move(distributeCb));
    }
}

TDelegatingAsyncSinkBase::~TDelegatingAsyncSinkBase() = default;

void TDelegatingAsyncSinkBase::Init(IInitContextPtr initContext)
{
    Delegate_->Init(initContext);
}

void TDelegatingAsyncSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    Delegate_->Distribute(message, std::move(onDistributed));
}

void TDelegatingAsyncSinkBase::Sync(NApi::IDynamicTableTransactionPtr transaction)
{
    Delegate_->Sync(transaction);
}

void TDelegatingAsyncSinkBase::Commit()
{
    Delegate_->Commit();
}

void TDelegatingAsyncSinkBase::ReconfigureAtMostOnce(const TDynamicSinkContextPtr& /*dynamicContext*/)
{
    if (auto atMostOnce = DynamicPointerCast<TAtMostOnceCallbackAdapter>(Delegate_)) {
        atMostOnce->Reconfigure(GetDynamicParameters()->AtMostOnceStrategy);
    }
}

void TDelegatingAsyncSinkBase::SuspendDestructionGuarded(std::vector<TIntrusivePtr<TRefCounted>> prevent)
{
    if (auto atMostOnce = DynamicPointerCast<TAtMostOnceCallbackAdapter>(Delegate_)) {
        atMostOnce->SuspendDestructionGuarded(std::move(prevent));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
