#include "bigrt_execution_context.h"

#include "composite_bigrt_writer.h" // IWYU pragma: keep (for proper working of TIntrusivePtr class)
#include "profiling.h"
#include "table_poller.h"

#include <ads/bsyeti/libs/ytex/transaction_keeper/transaction_keeper.h>
#include <bigrt/lib/processing/state_manager/base/fwd.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TBigRtExecutionContext
    : public IBigRtExecutionContext
{
public:
    TBigRtExecutionContext(TBigRtExecutionContextArgs args)
        : Args_(std::move(args))
    { }

    ui64 GetShard() const override
    {
        return Args_.Shard;
    }

    NYT::NProfiling::TProfiler GetProfiler() const override
    {
        return Args_.Profiler;
    }

private:
    NYT::NApi::IClientPtr ResolveYtClient(TStringBuf cluster) override
    {
        return Args_.TransactionKeeper->GetClient(cluster);
    }

    TTablePollerPtr GetTablePoller() override
    {
        return Args_.TablePoller;
    }

    const TCompositeBigRtWriterPtr& GetWriter() override
    {
        return Args_.Writer;
    }

    const NPrivate::TStateManagerRegistryPtr& GetStateManagerRegistry() override
    {
        return Args_.StateManagerRegistry;
    }

    const TString& GetMainCluster() override
    {
        return Args_.MainCluster;
    }

    void AddTransactionWriter(NBigRT::TWriterCallback callback) override
    {
        auto g = Guard(Lock_);
        TransactionWriterList.push_back(std::move(callback));
    }

    void AddOnCommitCallback(NPrivate::TOnCommitCallback callback) override
    {
        auto g = Guard(Lock_);
        OnCommitCallbackList.push_back(std::move(callback));
    }

    std::vector<NBigRT::TWriterCallback> GetTransactionWriterList() override
    {
        auto g = Guard(Lock_);
        return TransactionWriterList;
    }

    std::vector<NPrivate::TOnCommitCallback> GetOnCommitCallbackList() override
    {
        auto g = Guard(Lock_);
        return OnCommitCallbackList;
    }

private:
    const TBigRtExecutionContextArgs Args_;

    TMutex Lock_;
    std::vector<NBigRT::TWriterCallback> TransactionWriterList;
    std::vector<TOnCommitCallback> OnCommitCallbackList;
};

////////////////////////////////////////////////////////////////////////////////

NYT::NApi::IClientPtr ResolveYtClient(const ::TIntrusivePtr<IBigRtExecutionContext>& context, TStringBuf cluster)
{
    return context->ResolveYtClient(cluster);
}

::TIntrusivePtr<TTablePoller> GetTablePoller(const ::TIntrusivePtr<IBigRtExecutionContext>& context)
{
    return context->GetTablePoller();
}

const NPrivate::TCompositeBigRtWriterPtr& GetWriter(const ::TIntrusivePtr<IBigRtExecutionContext>& context)
{
    return context->GetWriter();
}

TIntrusivePtr<IBigRtExecutionContext> CreateDummyBigRtExecutionContext()
{
    return MakeIntrusive<NPrivate::TBigRtExecutionContext>(TBigRtExecutionContextArgs{});
}

NBigRT::TBaseStateManagerPtr ResolveStateManager(const TIntrusivePtr<IBigRtExecutionContext>& context, TString id)
{
    auto stateManager = context->GetStateManagerRegistry();
    YT_VERIFY(stateManager);
    return stateManager->GetBase(id);
}

const TString& GetMainCluster(const IBigRtExecutionContextPtr& context)
{
    return context->GetMainCluster();
}

void AddTransactionWriter(const IBigRtExecutionContextPtr& context, NBigRT::TWriterCallback callback)
{
    return context->AddTransactionWriter(callback);
}

void AddOnCommitCallback(const IBigRtExecutionContextPtr& context, TOnCommitCallback callback)
{
    return context->AddOnCommitCallback(callback);
}

std::vector<NBigRT::TWriterCallback> GetTransactionWriterList(IBigRtExecutionContextPtr& context)
{
    return context->GetTransactionWriterList();
}

std::vector<TOnCommitCallback> GetOnCommitCallbackList(const IBigRtExecutionContextPtr& context)
{
    return context->GetOnCommitCallbackList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

////////////////////////////////////////////////////////////////////////////////

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(
    const NYTEx::ITransactionKeeperPtr& transactionKeeper,
    ui64 shard,
    const TTablePollerPtr& tablePoller,
    const NSFStats::TSolomonContext& solomonContext,
    const NPrivate::TCompositeBigRtWriterPtr& writer)
{
    auto args = TBigRtExecutionContextArgs{
        .TransactionKeeper = transactionKeeper,
        .Shard = shard,
        .TablePoller = tablePoller,
        .Profiler = NPrivate::CreateSolomonContextProfiler(solomonContext),
        .Writer = writer,
    };
    return MakeIntrusive<NPrivate::TBigRtExecutionContext>(std::move(args));
}

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(TBigRtExecutionContextArgs args)
{
    return MakeIntrusive<NPrivate::TBigRtExecutionContext>(std::move(args));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
