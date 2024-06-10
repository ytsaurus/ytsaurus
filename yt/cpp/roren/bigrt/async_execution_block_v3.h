#pragma once

#include "sync_execution_block_v3.h"

#include <deque>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAsyncDeque
{
public:
    TAsyncDeque()
        : ReadyEvent_(NYT::NewPromise<void>())
    {
    }

    [[nodiscard]] NYT::TFuture<void> GetReady()
    {
        auto guard = Guard(Lock_);
        if (!Deque_.empty()) {
            return NYT::VoidFuture;
        }
        NeedNotify_ = true;
        return ReadyEvent_.ToFuture();
    }

    [[nodiscard]] std::optional<T> TryPop() noexcept
    {
        auto guard = Guard(Lock_);
        if (Deque_.empty()) {
            return std::nullopt;
        }
        std::optional<T> result = std::move(Deque_.front());
        Deque_.pop_front();
        if (ReadyEvent_.IsSet() && Deque_.empty()) {
            ReadyEvent_ = NYT::NewPromise<void>();
            NeedNotify_ = false;
        }
        return result;
    }

    void Push(T value)
    {
        auto guard = Guard(Lock_);
        Deque_.push_back(std::move(value));
        if (NeedNotify_) {
            ReadyEvent_.Set();
            NeedNotify_ = false;
        }
    }

private:
    TSpinLock Lock_;
    NYT::TPromise<void> ReadyEvent_;
    std::atomic<bool> NeedNotify_ = false;
    std::deque<T> Deque_;
};  // class TAsyncDeque

////////////////////////////////////////////////////////////////////////////////

class IAsyncExecutionBlockV3Input
    : public TThrRefBase
{
public:
    [[nodiscard]] virtual std::optional<NBigRT::TStatelessShardProcessor::TRowWithMeta> PopData(int fiberIndex) = 0;
    virtual void PushData(NBigRT::TStatelessShardProcessor::TRowWithMeta value) = 0;
    [[nodiscard]] virtual NYT::TFuture<void> GetDataReady() = 0;

    [[nodiscard]] virtual std::optional<std::pair<TString, std::vector<TTimer>>> PopTimer(int fiberIndex) = 0;
    virtual void PushTimer(std::pair<TString, std::vector<TTimer>> value) = 0;
    [[nodiscard]] virtual NYT::TFuture<void> GetTimerReady() = 0;
};  // class IAsyncExecutionBlockV3Input

using IAsyncExecutionBlockV3InputPtr = TIntrusivePtr<IAsyncExecutionBlockV3Input>;

////////////////////////////////////////////////////////////////////////////////

class TAsyncExecutionBlockV3Input
    : public IAsyncExecutionBlockV3Input
{
public:
    [[nodiscard]] std::optional<NBigRT::TStatelessShardProcessor::TRowWithMeta> PopData(int fiberIndex);
    void PushData(NBigRT::TStatelessShardProcessor::TRowWithMeta value);
    [[nodiscard]] NYT::TFuture<void> GetDataReady();

    [[nodiscard]] std::optional<std::pair<TString, std::vector<TTimer>>> PopTimer(int fiberIndex);
    void PushTimer(std::pair<TString, std::vector<TTimer>> value);
    [[nodiscard]] NYT::TFuture<void> GetTimerReady();

private:
    TAsyncDeque<NBigRT::TStatelessShardProcessor::TRowWithMeta> Data_;
    TAsyncDeque<std::pair<TString, std::vector<TTimer>>> Timers_;
};  // class TAsyncExecutionBlockV3Input

////////////////////////////////////////////////////////////////////////////////

class IAsyncExecutionBlockV3
    : public TThrRefBase
{
public:
    virtual NYT::TFuture<void> AsyncStartBundle(IBigRtExecutionContextPtr context, IAsyncExecutionBlockV3InputPtr input, std::vector<IRowWithMetaOutputPtr> outputs) = 0;
    virtual void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta data) = 0;
    virtual void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) = 0;
    virtual NYT::TFuture<void> AsyncFinishBundle() = 0;

    //virtual const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& GetTimersCallbacks() const noexcept = 0;
    virtual const THashMap<TString, TCreateBaseStateManagerFunction>& GetStateManagerFunctions() const noexcept = 0;

    //virtual TRowVtable GetInputVtable() const noexcept = 0;
    //virtual TRowVtable GetOutputVtable() const noexcept = 0;
};  // class IAsyncExecutionBlockV3

using IAsyncExecutionBlockV3Ptr = TIntrusivePtr<IAsyncExecutionBlockV3>;

////////////////////////////////////////////////////////////////////////////////

class TAsyncExecutionBlockV3
    : public IAsyncExecutionBlockV3
{
public:
    TAsyncExecutionBlockV3(int fiberIndex, NYT::IInvokerPtr invoker, TSyncExecutionBlockV3Ptr executionBlock, NYT::TCancelableContextPtr cancelableContext);
    ~TAsyncExecutionBlockV3();
    NYT::TFuture<void> AsyncStartBundle(IBigRtExecutionContextPtr context, IAsyncExecutionBlockV3InputPtr input, std::vector<IRowWithMetaOutputPtr> outputs) override;
    void AsyncDo(NBigRT::TStatelessShardProcessor::TRowWithMeta data) override;
    void AsyncOnTimer(const TString& callbackId, std::vector<TTimer> readyTimers) override;
    NYT::TFuture<void> AsyncFinishBundle() override;

    const THashMap<TString, NPrivate::TStatefulTimerParDoWrapperPtr>& GetTimersCallbacks() const noexcept;
    const THashMap<TString, TCreateBaseStateManagerFunction>& GetStateManagerFunctions() const noexcept override;

    TRowVtable GetInputVtable() const noexcept;
    std::vector<TDynamicTypeTag> GetOutputTags() const noexcept;

private:
    void Loop() noexcept;

private:
    const int FiberIndex_;
    const NYT::IInvokerPtr Invoker_;
    const TSyncExecutionBlockV3Ptr ExecutionBlock_;
    const NYT::TCancelableContextPtr CancelableContext_;

    NYT::TPromise<void> NeedFinish_;
    NYT::TPromise<void> IsFiberTerminated_;

    IAsyncExecutionBlockV3InputPtr Input_;
};  // class TSyncExecutionBlockV3

using TAsyncExecutionBlockV3Ptr = TIntrusivePtr<TAsyncExecutionBlockV3>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
