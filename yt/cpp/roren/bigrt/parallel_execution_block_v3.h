#pragma once

#include "fwd.h"
#include "async_execution_block_v3.h"
#include "bigrt_execution_context.h"  // TBigRtExecutionContextArgs

#include <yt/cpp/roren/library/cpu_account_invoker/cpu_account_invoker.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TParallelExecutionBlockV3;

////////////////////////////////////////////////////////////////////////////////

class IParallelExecutionBlockV3Input
    : public TThrRefBase
{
public:
    virtual void AsyncMessageBatchDo(NBigRT::TMessageBatch messageBatch);
    virtual void AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data);
    virtual void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta);
    virtual void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers);
};  // class IParallelExecutionBlockV3Input

using IParallelExecutionBlockV3InputPtr = TIntrusivePtr<IParallelExecutionBlockV3Input>;

////////////////////////////////////////////////////////////////////////////////

class IParallelExecutionBlockV3
    : public IParallelExecutionBlockV3Input
{
public:
    virtual void SetOutputs(std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> outputs) = 0;
    virtual void StartEpoch(const TBigRtExecutionContextArgs& contextArgs) = 0;
    [[nodiscard]] virtual NYT::TFuture<void> AsyncStartBundle() = 0;
    [[nodiscard]] virtual NYT::TFuture<void> AsyncFinishBundle() = 0;
    [[nodiscard]] virtual NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture) = 0;
    [[nodiscard]] virtual THashMap<TString, TCreateBaseStateManagerFunction> GetStateManagerFunctions() const noexcept = 0;
};

using IParallelExecutionBlockV3Ptr = TIntrusivePtr<IParallelExecutionBlockV3>;

////////////////////////////////////////////////////////////////////////////////

class TParallelExecutionBlockV3ProxyOutput
    : public IRowWithMetaOutput
{
public:
    using TRowMeta = NBigRT::TStatelessShardProcessor::TRowMeta;
    using TRowWithMeta = NBigRT::TStatelessShardProcessor::TRowWithMeta;
    using TRowWithMetaV = NBigRT::TStatelessShardProcessor::TRowWithMetaV;

    TParallelExecutionBlockV3ProxyOutput(TParallelExecutionBlockV3& executionBlock, const TRowVtable& vtable, ssize_t outputIndex);
    void OnStartBundle(const IBigRtExecutionContextPtr& executionContext) override;
    void OnRow() override;
    void OnTimer() override;
    void OnFinishBundle() override;

    void AddRaw(const void* rows, ssize_t count) override;
    void Close() override;

private:
    std::reference_wrapper<TParallelExecutionBlockV3> ExecutionBlock_;
    TRowVtable Vtable_;
    TRowVtable KeyVtable_;
    ssize_t OutputIndex_ = 0;
    IBigRtExecutionContextPtr ExecutionContext_;
    int RowSeqNo = -1;
};  // class TParallelExecutionBlockV3ProxyOutput

////////////////////////////////////////////////////////////////////////////////

class TParallelExecutionBlockV3BufferedOutput
    : public IRowWithMetaOutput
{
public:
    using TRowMeta = NBigRT::TStatelessShardProcessor::TRowMeta;
    using TRowWithMeta = NBigRT::TStatelessShardProcessor::TRowWithMeta;
    using TRowWithMetaV = NBigRT::TStatelessShardProcessor::TRowWithMetaV;
    using TContainer = std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV>;

    TParallelExecutionBlockV3BufferedOutput(const TRowVtable& vtable, std::deque<TRowWithMetaV>& result);
    void OnStartBundle(const IBigRtExecutionContextPtr& executionContext) override;
    void OnRow() override;
    void OnTimer() override;
    void OnFinishBundle() override;

    void AddRaw(const void* rows, ssize_t count) override;
    void Close() override;

private:
    std::atomic<bool> Finished_ = true;
    TRowVtable Vtable_;
    TRowVtable KeyVtable_;
    IBigRtExecutionContextPtr ExecutionContext_;
    int RowSeqNo = -1;
    std::reference_wrapper<TContainer> Result_;
};  // class TParallelExecutionBlockV3BufferedOutput

using TParallelExecutionBlockV3BufferedOutputPtr = TIntrusivePtr<TParallelExecutionBlockV3BufferedOutput>;

////////////////////////////////////////////////////////////////////////////////

class TParallelExecutionBlockV3
    : public IParallelExecutionBlockV3
{
public:
    TParallelExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
    void SetOutputs(std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> foreignOutputs) override;
    void StartEpoch(const TBigRtExecutionContextArgs& contextArgs) override;
    [[nodiscard]] NYT::TFuture<void> AsyncStartBundle() override;
    [[nodiscard]] NYT::TFuture<void> AsyncFinishBundle() override;
    [[nodiscard]] NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseFinishFuture) override;
    [[nodiscard]] THashMap<TString, TCreateBaseStateManagerFunction> GetStateManagerFunctions() const noexcept override;

protected:
    virtual void GatherWrites();
    virtual std::vector<IRowWithMetaOutputPtr> MakeOutputs(ssize_t executionBlockIndex) = 0;

protected:
    NYT::IInvokerPtr Invoker_;
    std::vector<IBigRtExecutionContextPtr> Contexts_;
    std::vector<IAsyncExecutionBlockV3InputPtr> Inputs_;
    std::vector<std::vector<IRowWithMetaOutputPtr>> Outputs_;
    std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> ForeignOutputs_;
    std::vector<TAsyncExecutionBlockV3Ptr> AsyncExecutionBlocks_;

    std::vector<NBigRT::TWriterCallback> TransactionWriterList_;
    std::vector<NPrivate::TOnCommitCallback> OnCommitCallbackList_;
    NPrivate::TTimers::TTimersHashMap TimersUpdates_;

    friend class TParallelExecutionBlockV3ProxyOutput;  //TODO: remove
};  // class TParallelExecutionBlockV3

////////////////////////////////////////////////////////////////////////////////

IParallelExecutionBlockV3Ptr MakeParallelByNoneExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
IParallelExecutionBlockV3Ptr MakeParallelByGreedyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
IParallelExecutionBlockV3Ptr MakeParallelByPartExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);
IParallelExecutionBlockV3Ptr MakeParallelByKeyExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks);

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
