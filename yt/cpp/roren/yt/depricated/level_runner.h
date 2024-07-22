#pragma once

#include "yt_graph.h"

#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/system/spinlock.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TLevelRunner
    : public ::NYT::TRefCounted
{
public:
    TLevelRunner(
        NYT::IClientBasePtr tx,
        std::shared_ptr<IYtGraph> ytGraph,
        i32 concurrencyLimit);

    void RunOperations(const std::vector<IYtGraph::TOperationNodeId>& level, const TStartOperationContext& context);

private:
    class TState
        : public ::NYT::TRefCounted
    {
    public:
        explicit TState(i32 concurrencyLimit, const std::vector<IYtGraph::TOperationNodeId>& level);

        void WaitOperations();
        void SignalCompletion();
        void SignalError();

        bool HasError() const;

        void RegisterRunningOperation(IYtGraph::TOperationNodeId operationNodeId, ::NYT::IOperationPtr operation);
        void UnregisterRunningOperation(IYtGraph::TOperationNodeId operationNodeId);
        void AbortRunningOperations();

    public:
        const std::vector<IYtGraph::TOperationNodeId>& Level;

        i32 AvailableToRun;
        ssize_t Completed = 0;
        ssize_t Cursor = 0;

    private:
        ::NThreading::TPromise<void> Promise_;
        ::NThreading::TFuture<void> MainFuture_;

        bool HasError_ = false;

        THashMap<IYtGraph::TOperationNodeId, ::NYT::IOperationPtr> RunningOperations_;
    };

private:
    void StartAllAvailable(const TStartOperationContext& context);

    NThreading::TFuture<void> StartOperation(
        IYtGraph::TOperationNodeId operationNodeId,
        const TStartOperationContext& context);

private:
    TAdaptiveLock Lock_;

    NYT::IClientBasePtr Tx_;
    std::shared_ptr<IYtGraph> YtGraph_;
    i32 ConcurrencyLimit_;

    ::NYT::TIntrusivePtr<TState> State_;
};

DECLARE_REFCOUNTED_TYPE(TLevelRunner)

////////////////////////////////////////////////////////////////////////////////

TLevelRunnerPtr MakeLevelRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

