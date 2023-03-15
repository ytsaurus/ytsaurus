#pragma once

#include "fwd.h"
#include "composite_bigrt_writer.h" // IWYU pragma: keep (Required for TIntrusivePtr)

#include "stateful_impl/state_manager_registry.h"

#include <yt/cpp/roren/interface/execution_context.h>

#include <ads/bsyeti/libs/ytex/transaction_keeper/public.h>

#include <library/cpp/safe_stats/safe_stats.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

NYT::NApi::IClientPtr ResolveYtClient(const ::TIntrusivePtr<IBigRtExecutionContext>& executionContext, TStringBuf cluster);
::TIntrusivePtr<TTablePoller> GetTablePoller(const ::TIntrusivePtr<IBigRtExecutionContext>& executionContext);
const NPrivate::TCompositeBigRtWriterPtr& GetWriter(const ::TIntrusivePtr<IBigRtExecutionContext>&);
NBigRT::TBaseStateManagerPtr ResolveStateManager(const TIntrusivePtr<IBigRtExecutionContext>& context, TString id);
const TString& GetMainCluster(const IBigRtExecutionContextPtr& context);

void AddTransactionWriter(const IBigRtExecutionContextPtr& context, NBigRT::TWriterCallback callback);
void AddOnCommitCallback(const IBigRtExecutionContextPtr& context, TOnCommitCallback callback);

std::vector<NBigRT::TWriterCallback> GetTransactionWriterList(IBigRtExecutionContextPtr& context);
std::vector<TOnCommitCallback> GetOnCommitCallbackList(const IBigRtExecutionContextPtr& context);

}

////////////////////////////////////////////////////////////////////////////////

class IBigRtExecutionContext
    : public IExecutionContext
{
public:
    TString GetExecutorName() const final
    {
        return "bigrt";
    }

    virtual ui64 GetShard() const = 0;

private:
    virtual NYT::NApi::IClientPtr ResolveYtClient(TStringBuf cluster) = 0;
    virtual ::TIntrusivePtr<TTablePoller> GetTablePoller() = 0;
    virtual const NPrivate::TCompositeBigRtWriterPtr& GetWriter() = 0;
    virtual const NPrivate::TStateManagerRegistryPtr& GetStateManagerRegistry() = 0;
    virtual const TString& GetMainCluster() = 0;

    virtual void AddTransactionWriter(NBigRT::TWriterCallback callback) = 0;
    virtual void AddOnCommitCallback(NPrivate::TOnCommitCallback callback) = 0;

    virtual std::vector<NBigRT::TWriterCallback> GetTransactionWriterList() = 0;
    virtual std::vector<NPrivate::TOnCommitCallback> GetOnCommitCallbackList() = 0;

private:
    friend NYT::NApi::IClientPtr NPrivate::ResolveYtClient(const ::TIntrusivePtr<IBigRtExecutionContext>&, TStringBuf cluster);
    friend ::TIntrusivePtr<TTablePoller> NPrivate::GetTablePoller(const ::TIntrusivePtr<IBigRtExecutionContext>&);
    friend const NPrivate::TCompositeBigRtWriterPtr& NPrivate::GetWriter(const ::TIntrusivePtr<IBigRtExecutionContext>&);
    friend NBigRT::TBaseStateManagerPtr NPrivate::ResolveStateManager(const TIntrusivePtr<IBigRtExecutionContext>& context, TString id);
    friend const TString& NPrivate::GetMainCluster(const IBigRtExecutionContextPtr& context);

    friend void NPrivate::AddTransactionWriter(const IBigRtExecutionContextPtr& context, NBigRT::TWriterCallback callback);
    friend void NPrivate::AddOnCommitCallback(const IBigRtExecutionContextPtr& context, NPrivate::TOnCommitCallback callback);

    friend std::vector<NBigRT::TWriterCallback> NPrivate::GetTransactionWriterList(IBigRtExecutionContextPtr& context);
    friend std::vector<NPrivate::TOnCommitCallback> NPrivate::GetOnCommitCallbackList(const IBigRtExecutionContextPtr& context);
};

struct TBigRtExecutionContextArgs
{
    NYTEx::ITransactionKeeperPtr TransactionKeeper;
    ui64 Shard = 0;
    TTablePollerPtr TablePoller;
    NYT::NProfiling::TProfiler Profiler;
    NPrivate::TCompositeBigRtWriterPtr Writer;
    NPrivate::TStateManagerRegistryPtr StateManagerRegistry;
    TString MainCluster;
};

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(TBigRtExecutionContextArgs args);

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(
    const NYTEx::ITransactionKeeperPtr& transactionKeeper,
    ui64 shard,
    const TTablePollerPtr& tablePoller,
    const NSFStats::TSolomonContext& solomonContext,
    const NPrivate::TCompositeBigRtWriterPtr& writer = nullptr);

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Create dummy execution context
///
/// Useful for tests that doesn't use execution context.
TIntrusivePtr<IBigRtExecutionContext> CreateDummyBigRtExecutionContext();

struct TProcessorCommitContext
{
    bool Success = false;
    NYT::NApi::TTransactionCommitResult TransactionCommitResult = {};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
