#include "bigrt_execution_context.h"

#include "composite_bigrt_writer.h" // IWYU pragma: keep (for proper working of TIntrusivePtr class)
#include "profiling.h"
#include "table_poller.h"

#include <ads/bsyeti/libs/ytex/transaction_keeper/transaction_keeper.h>
#include <bigrt/lib/processing/state_manager/base/fwd.h>
#include <yt/cpp/roren/library/timers/timers.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TBigRtExecutionContext
    : public IBigRtExecutionContext
{
public:
    TBigRtExecutionContext(TBigRtExecutionContextArgs args)
        : Args_(std::move(args))
    { }

    const TString& GetInputTag() const override
    {
        return Args_.InputTag;
    }

    ui64 GetShard() const override
    {
        return Args_.Shard;
    }

    const NBigRT::TStatelessShardProcessor::TRowMeta& GetRowMeta() const override
    {
        return *RowMeta_;
    }

    void SetRowMeta(const NBigRT::TStatelessShardProcessor::TRowMeta& rowMeta)
    {
        RowMeta_ = &rowMeta;
    }

    NYT::NProfiling::TProfiler GetProfiler() const override
    {
        return Args_.Profiler;
    }

    std::shared_ptr<TTimers> GetTimers()
    {
        return Args_.Timers;
    }

    void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy) override
    {
        auto g = Guard(Lock_);
        auto result = TimersMap_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(timer.GetKey()),
            std::forward_as_tuple(timer, policy)
        );
        if (!result.second) {
            TimersMap_.at(timer.GetKey()) = std::tuple(timer, policy);
        }
    }

    void DeleteTimer(const TTimer::TKey& key) override
    {
        SetTimer(
            TTimer{key.GetKey(), key.GetTimerId(), key.GetCallbackId(), 0, {}},
            TTimer::EMergePolicy::REPLACE
        );
    }

    NYT::NApi::IClientPtr ResolveYtClient(TStringBuf cluster)
    {
        return Args_.TransactionKeeper->GetClient(cluster);
    }

    TTablePollerPtr GetTablePoller()
    {
        return Args_.TablePoller;
    }

    const TCompositeBigRtWriterPtr& GetWriter()
    {
        return Args_.Writer;
    }

    const NPrivate::TStateManagerRegistryPtr& GetStateManagerRegistry()
    {
        return Args_.StateManagerRegistry;
    }

    const TString& GetMainCluster()
    {
        return Args_.MainCluster;
    }

    void AddTransactionWriter(NBigRT::TWriterCallback callback)
    {
        auto g = Guard(Lock_);
        TransactionWriterList_.push_back(std::move(callback));
    }

    void AddOnCommitCallback(NPrivate::TOnCommitCallback callback)
    {
        auto g = Guard(Lock_);
        OnCommitCallbackList_.push_back(std::move(callback));
    }

    std::vector<NBigRT::TWriterCallback> GetTransactionWriterList()
    {
        auto g = Guard(Lock_);
        return std::move(TransactionWriterList_);
    }

    TTimers::TTimersHashMap GetTimersHashMap()
    {
        auto g = Guard(Lock_);
        return std::move(TimersMap_);
    }

    bool IsTimerChanged(const TTimer::TKey& key) const noexcept
    {
        auto g = Guard(Lock_);
        return TimersMap_.contains(key);
    }

    std::vector<NPrivate::TOnCommitCallback> GetOnCommitCallbackList()
    {
        auto g = Guard(Lock_);
        return std::move(OnCommitCallbackList_);
    }

    void ThrottlerReportProcessed(ui64 processed)
    {
        if (Args_.ThrottlerReport) {
            *Args_.ThrottlerReport += processed;
        }
    }

private:
    const TBigRtExecutionContextArgs Args_;

    TMutex Lock_;
    TVector<NBigRT::TWriterCallback> TransactionWriterList_;
    TVector<TOnCommitCallback> OnCommitCallbackList_;
    TTimers::TTimersHashMap TimersMap_;
    const NBigRT::TStatelessShardProcessor::TRowMeta* RowMeta_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IBigRtExecutionContext> CreateDummyBigRtExecutionContext()
{
    return MakeIntrusive<TBigRtExecutionContext>(TBigRtExecutionContextArgs{});
}

////////////////////////////////////////////////////////////////////////////////

static TBigRtExecutionContext* CastContext(const IBigRtExecutionContextPtr& context)
{
    return VerifyDynamicCast<TBigRtExecutionContext*>(context.Get());
}

NYT::NApi::IClientPtr TBigRtExecutionContextOps::ResolveYtClient(const IBigRtExecutionContextPtr& context, TStringBuf cluster)
{
    return CastContext(context)->ResolveYtClient(cluster);
}

::TIntrusivePtr<TTablePoller> TBigRtExecutionContextOps::GetTablePoller(const IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetTablePoller();
}

const TCompositeBigRtWriterPtr& TBigRtExecutionContextOps::GetWriter(const IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetWriter();
}

NBigRT::TBaseStateManagerPtr TBigRtExecutionContextOps::ResolveStateManager(const IBigRtExecutionContextPtr& context, TString id)
{
    auto stateManager = CastContext(context)->GetStateManagerRegistry();
    YT_VERIFY(stateManager);
    return stateManager->GetBase(id);
}

const TString& TBigRtExecutionContextOps::GetMainCluster(const IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetMainCluster();
}

void TBigRtExecutionContextOps::AddTransactionWriter(const IBigRtExecutionContextPtr& context, NBigRT::TWriterCallback callback)
{
    return CastContext(context)->AddTransactionWriter(callback);
}

void TBigRtExecutionContextOps::AddOnCommitCallback(const IBigRtExecutionContextPtr& context, TOnCommitCallback callback)
{
    return CastContext(context)->AddOnCommitCallback(callback);
}

std::vector<NBigRT::TWriterCallback> TBigRtExecutionContextOps::GetTransactionWriterList(IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetTransactionWriterList();
}

std::shared_ptr<NPrivate::TTimers> TBigRtExecutionContextOps::GetTimers(IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetTimers();
}

TTimers::TTimersHashMap TBigRtExecutionContextOps::GetTimersHashMap(const IBigRtExecutionContextPtr& executionContext)
{
    return CastContext(executionContext)->GetTimersHashMap();
}

bool TBigRtExecutionContextOps::IsTimerChanged(const IBigRtExecutionContextPtr& executionContext, const TTimer::TKey& key)
{
    return CastContext(executionContext)->IsTimerChanged(key);
}

std::vector<TOnCommitCallback> TBigRtExecutionContextOps::GetOnCommitCallbackList(const IBigRtExecutionContextPtr& context)
{
    return CastContext(context)->GetOnCommitCallbackList();
}

void TBigRtExecutionContextOps::ThrottlerReportProcessed(const IBigRtExecutionContextPtr& context, ui64 processed)
{
    CastContext(context)->ThrottlerReportProcessed(processed);
}

void TBigRtExecutionContextOps::SetRowMeta(const IBigRtExecutionContextPtr& context, const NBigRT::TStatelessShardProcessor::TRowMeta& rowMeta)
{
    return CastContext(context)->SetRowMeta(rowMeta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

////////////////////////////////////////////////////////////////////////////////

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(const TBigRtExecutionContextArgs& args)
{
    return MakeIntrusive<NPrivate::TBigRtExecutionContext>(args);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
