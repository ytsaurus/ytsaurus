#pragma once

#include "fwd.h"
#include "composite_bigrt_writer.h" // IWYU pragma: keep (Required for TIntrusivePtr)

#include "stateful_impl/state_manager_registry.h"

#include <bigrt/lib/processing/shard_processor/stateless/processor.h>  // NBigRT::TStatelessShardProcessor::TRowWithMeta

#include <yt/cpp/roren/interface/execution_context.h>
#include <yt/cpp/roren/library/timers/timers.h>

#include <ads/bsyeti/libs/ytex/transaction_keeper/public.h>

#include <library/cpp/safe_stats/safe_stats.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IBigRtExecutionContext
    : public IExecutionContext
{
public:
    TString GetExecutorName() const final
    {
        return "bigrt";
    }

    virtual const TString& GetInputTag() const = 0;
    virtual ui64 GetShard() const = 0;
    virtual const NBigRT::TStatelessShardProcessor::TRowMeta& GetRowMeta() const = 0;
};

struct TBigRtExecutionContextArgs
{
    TString InputTag;
    ui64 Shard = 0;
    std::shared_ptr<NPrivate::TTimers> Timers;
    NYTEx::ITransactionKeeperPtr TransactionKeeper;
    TTablePollerPtr TablePoller;
    NYT::NProfiling::TProfiler Profiler;
    NPrivate::TCompositeBigRtWriterPtr Writer;
    NPrivate::TStateManagerRegistryPtr StateManagerRegistry;
    TString MainCluster;
    std::shared_ptr<std::atomic<ui64>> ThrottlerReport;
};

TIntrusivePtr<IBigRtExecutionContext> CreateBigRtExecutionContext(const TBigRtExecutionContextArgs& args);

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


///
/// @brief Helper class to access hidden state of IBigRtExecutionContext
///
/// MUST NOT be used from user code, can only be used from roren code.
class TBigRtExecutionContextOps
{
public:
    TBigRtExecutionContextOps() = delete;

public:
    static NYT::NApi::IClientPtr ResolveYtClient(const IBigRtExecutionContextPtr& executionContext, TStringBuf cluster);
    static ::TIntrusivePtr<TTablePoller> GetTablePoller(const IBigRtExecutionContextPtr& executionContext);
    static const NPrivate::TCompositeBigRtWriterPtr& GetWriter(const IBigRtExecutionContextPtr&);
    static NBigRT::TBaseStateManagerPtr ResolveStateManager(const IBigRtExecutionContextPtr& context, TString id);
    static const TString& GetMainCluster(const IBigRtExecutionContextPtr& context);

    static void AddTransactionWriter(const IBigRtExecutionContextPtr& context, NBigRT::TWriterCallback callback);
    static void AddOnCommitCallback(const IBigRtExecutionContextPtr& context, TOnCommitCallback callback);

    static std::vector<NBigRT::TWriterCallback> GetTransactionWriterList(IBigRtExecutionContextPtr& context);
    static std::shared_ptr<NPrivate::TTimers> GetTimers(IBigRtExecutionContextPtr& context);
    static TTimers::TTimersHashMap GetTimersHashMap(const IBigRtExecutionContextPtr& executionContext);
    static bool IsTimerChanged(const IBigRtExecutionContextPtr& executionContext, const TTimer::TKey& key);

    static std::vector<TOnCommitCallback> GetOnCommitCallbackList(const IBigRtExecutionContextPtr& context);

    static void ThrottlerReportProcessed(const IBigRtExecutionContextPtr& context, ui64 processed);

    static void SetRowMeta(const IBigRtExecutionContextPtr& context, const NBigRT::TStatelessShardProcessor::TRowMeta& rowMeta);
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
