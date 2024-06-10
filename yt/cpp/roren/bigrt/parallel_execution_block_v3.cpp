#include "parallel_execution_block_v3.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

void IParallelExecutionBlockV3Input::AsyncMessageBatchDo(NBigRT::TMessageBatch messageBatch)
{
    Y_UNUSED(messageBatch);
    Y_ABORT("Not implemented");
}

void IParallelExecutionBlockV3Input::AsyncBatchDo(std::deque<NBigRT::TStatelessShardProcessor::TRowWithMetaV> data)
{
    Y_UNUSED(data);
    Y_ABORT("Not implemented");
}

void IParallelExecutionBlockV3Input::AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta rowWithMeta)
{
    Y_UNUSED(rowWithMeta);
    Y_ABORT("Not implemented");
}

void IParallelExecutionBlockV3Input::AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers)
{
    Y_UNUSED(callbackId, readyTimers);
    Y_ABORT("Not implemented");
}

////////////////////////////////////////////////////////////////////////////////

TParallelExecutionBlockV3ProxyOutput::TParallelExecutionBlockV3ProxyOutput(TParallelExecutionBlockV3& executionBlock, const TRowVtable& vtable, ssize_t outputIndex)
    : ExecutionBlock_(executionBlock)
    , Vtable_(vtable)
    , OutputIndex_(outputIndex)
{
    if (IsKv(Vtable_)) {
        KeyVtable_ = Vtable_.KeyVtableFactory();
        Y_ABORT_IF(KeyVtable_.Hash == nullptr);
    }
}

void TParallelExecutionBlockV3ProxyOutput::OnStartBundle(const IBigRtExecutionContextPtr& executionContext)
{
    ExecutionContext_ = executionContext;
}

void TParallelExecutionBlockV3ProxyOutput::OnRow()
{
    RowSeqNo = 0;
}

void TParallelExecutionBlockV3ProxyOutput::OnTimer()
{
    RowSeqNo = 0;
}

void TParallelExecutionBlockV3ProxyOutput::OnFinishBundle()
{
}

void TParallelExecutionBlockV3ProxyOutput::AddRaw(const void* rows, ssize_t count)
{
    for (const char* p = reinterpret_cast<const char*>(rows); count > 0; --count, p += Vtable_.DataSize) {
        TRowVtable::TUniquePtr uniquePtr = Vtable_.CopyToUniquePtr(p);
        const size_t keyHash = KeyVtable_.Hash? KeyVtable_.Hash(GetKeyOfKv(Vtable_, p)) : 0;
        TRowMeta meta(ExecutionContext_->GetRowMeta());
        //TODO: set RowSeqNo
        ++RowSeqNo;
        TRowWithMeta rowWithMeta(std::move(uniquePtr), Vtable_.TypeHash, Vtable_.DataSize, std::move(meta), keyHash);
        Y_ABORT_IF(ExecutionBlock_.get().ForeignOutputs_.size() != 1);  //TODO: support for multiple outputs
        Y_ABORT_IF(ExecutionBlock_.get().ForeignOutputs_[OutputIndex_].size() != 1);
        ExecutionBlock_.get().ForeignOutputs_[OutputIndex_][0]->AsyncDo(std::move(rowWithMeta));
    }
}

void TParallelExecutionBlockV3ProxyOutput::Close()
{
    RowSeqNo = -1;
    ExecutionContext_ = {};
}

////////////////////////////////////////////////////////////////////////////////

TParallelExecutionBlockV3BufferedOutput::TParallelExecutionBlockV3BufferedOutput(const TRowVtable& vtable, std::deque<TRowWithMetaV>& result)
    : Vtable_(vtable)
    , Result_(result)
{
    if (IsKv(Vtable_)) {
        KeyVtable_ = Vtable_.KeyVtableFactory();
        Y_ABORT_IF(KeyVtable_.Hash == nullptr);
    }
}

void TParallelExecutionBlockV3BufferedOutput::OnStartBundle(const IBigRtExecutionContextPtr& executionContext)
{
    ExecutionContext_ = executionContext;
    Finished_ = false;
    RowSeqNo = 0;
}

void TParallelExecutionBlockV3BufferedOutput::AddRaw(const void* rows, ssize_t count)
{
    Y_ABORT_IF(Finished_);
    for (const char* p = reinterpret_cast<const char*>(rows); count > 0; --count, p += Vtable_.DataSize) {
        TRowVtable::TUniquePtr uniquePtr = Vtable_.CopyToUniquePtr(p);
        const size_t keyHash = KeyVtable_.Hash? KeyVtable_.Hash(GetKeyOfKv(Vtable_, p)) : 0;
        TRowMeta meta(ExecutionContext_->GetRowMeta());
        //TODO: set RowSeqNo
        ++RowSeqNo;
        TRowWithMeta rowWithMeta(std::move(uniquePtr), Vtable_.TypeHash, Vtable_.DataSize, std::move(meta), keyHash);
        Result_.get().back().push_back(std::move(rowWithMeta));
    }
}

void TParallelExecutionBlockV3BufferedOutput::OnRow()
{
    Result_.get().push_back({});
    RowSeqNo = 0;
}

void TParallelExecutionBlockV3BufferedOutput::OnTimer()
{
    Y_ABORT("Timers are not allowed in PrepareData block");
}

void TParallelExecutionBlockV3BufferedOutput::OnFinishBundle()
{
    #if 0
    //TODO: enable only for greedy
    Finished_ = true;
    RowSeqNo = -1;
    ExecutionContext_ = {};
    #endif
}

void TParallelExecutionBlockV3BufferedOutput::Close()
{
    Finished_ = true;
    RowSeqNo = -1;
    ExecutionContext_ = {};
}

////////////////////////////////////////////////////////////////////////////////

TParallelExecutionBlockV3::TParallelExecutionBlockV3(NYT::IInvokerPtr invoker, std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks)
    : Invoker_(std::move(invoker))
    , AsyncExecutionBlocks_(std::move(asyncExecutionBlocks))
{
    Contexts_.resize(AsyncExecutionBlocks_.size());
    Inputs_.resize(AsyncExecutionBlocks_.size());
    Outputs_.resize(AsyncExecutionBlocks_.size());
}

void TParallelExecutionBlockV3::SetOutputs(std::vector<std::vector<IParallelExecutionBlockV3InputPtr>> foreignOutputs)
{
    ForeignOutputs_ = std::move(foreignOutputs);
}

void TParallelExecutionBlockV3::StartEpoch(const TBigRtExecutionContextArgs& contextArgs)
{
    Y_ABORT_IF(AsyncExecutionBlocks_.size() != Contexts_.size());
    //TODO: check ExecutionBlock GetOutputTags().size() == ForeignOutputs_.size()
    for (size_t i = 0; i < AsyncExecutionBlocks_.size(); ++i) {
        Contexts_[i] = CreateBigRtExecutionContext(contextArgs);
    }
}

NYT::TFuture<void> TParallelExecutionBlockV3::AsyncStartBundle()
{
    std::vector<NYT::TFuture<void>> futures;
    futures.reserve(AsyncExecutionBlocks_.size());
    for (size_t i = 0; i < AsyncExecutionBlocks_.size(); ++i) {
        futures.emplace_back(AsyncExecutionBlocks_[i]->AsyncStartBundle(Contexts_[i], Inputs_[i], Outputs_[i]));
    }
    return NYT::AllSucceeded(futures);
}

NYT::TFuture<void> TParallelExecutionBlockV3::AsyncFinishBundle()
{
    std::vector<NYT::TFuture<void>> futures;
    futures.reserve(AsyncExecutionBlocks_.size());
    for (size_t i = 0; i < AsyncExecutionBlocks_.size(); ++i) {
        futures.emplace_back(AsyncExecutionBlocks_[i]->AsyncFinishBundle());
    }
    return NYT::AllSucceeded(futures);
}

NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> TParallelExecutionBlockV3::FinishEpoch(NYT::TFuture<NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult> baseResultFuture)
{
    auto result = baseResultFuture.Apply(BIND([
        timers = NPrivate::TBigRtExecutionContextOps::GetTimers(Contexts_[0]),
        transactionWriterList = std::move(TransactionWriterList_),
        timersUpdates = std::move(TimersUpdates_),
        onCommitCallbackList = std::move(OnCommitCallbackList_)
    ] (NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult prepareResult) mutable {
        return NBigRT::TStatelessShardProcessor::TPrepareForAsyncWriteResult{
            .AsyncWriter = [
                timers,
                asyncWriter = prepareResult.AsyncWriter,
                timersUpdates = std::move(timersUpdates),
                transactionWriterList = std::move(transactionWriterList)
            ] (NYT::NApi::ITransactionPtr tx, NYTEx::ITransactionContextPtr context) {
                if (asyncWriter) {
                    asyncWriter(tx, context);
                }
                if (timers) {
                    timers->Commit(tx, timersUpdates);
                }
                for (const auto& writer : transactionWriterList) {
                    writer(tx, context);
                }
            },
            .OnCommitCallback = [
                onCommitCallback = prepareResult.OnCommitCallback,
                onCommitCallbackList = std::move(onCommitCallbackList)
            ] (const NBigRT::TStatelessShardProcessor::TCommitContext& context) {
                if (onCommitCallback) {
                    onCommitCallback(context);
                }

                NPrivate::TProcessorCommitContext rorenContext = {
                    .Success = context.Success,
                    .TransactionCommitResult = context.TransactionCommitResult,
                };
                for (const auto& callback : onCommitCallbackList) {
                    callback(rorenContext);
                }

            },
        };
    }));
    for (auto& context : Contexts_) {
        context = {};
    }
    return result;
}

THashMap<TString, TCreateBaseStateManagerFunction> TParallelExecutionBlockV3::GetStateManagerFunctions() const noexcept
{
    THashMap<TString, TCreateBaseStateManagerFunction> result;
    for (size_t i = 0; i < AsyncExecutionBlocks_.size(); ++i) {
        result.insert(AsyncExecutionBlocks_[i]->GetStateManagerFunctions().begin(), AsyncExecutionBlocks_[i]->GetStateManagerFunctions().end());
    }
    return result;
}

void TParallelExecutionBlockV3::GatherWrites()
{
    for (auto& executionContext : Contexts_) {
        Y_UNUSED(executionContext);
        auto transactionWriterList = NPrivate::TBigRtExecutionContextOps::GetTransactionWriterList(executionContext);
        TransactionWriterList_.reserve(TransactionWriterList_.size() + transactionWriterList.size());
        for (auto& writer : transactionWriterList) {
            TransactionWriterList_.push_back(std::move(writer));
        }
        auto onCommitCallbackList = NPrivate::TBigRtExecutionContextOps::GetOnCommitCallbackList(executionContext);
        OnCommitCallbackList_.reserve(OnCommitCallbackList_.size() + onCommitCallbackList.size());
        for (auto& callback : onCommitCallbackList) {
            OnCommitCallbackList_.push_back(std::move(callback));
        }
        auto timersUpdates = NPrivate::TBigRtExecutionContextOps::GetTimersHashMap(executionContext);
        for (auto& [timerKey, timer] : timersUpdates) {
            TimersUpdates_.insert_or_assign(timerKey, std::move(timer));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NRoren::NPrivate
