#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/roren/yt/yt_graph.h>

#include <util/system/spinlock.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TOperationRunner
    : public ::NYT::TRefCounted
{
public:
    TOperationRunner(
        NYT::IClientBasePtr tx,
        std::shared_ptr<IYtGraph> ytGraph,
        i32 concurrencyLimit);

    void RunOperations(const std::vector<IYtGraph::TOperationNodeId>& level, const TStartOperationContext& context);

private:
    struct TState
        : public ::NYT::TRefCounted
    {
        explicit TState(i32 concurrencyLimit, const std::vector<IYtGraph::TOperationNodeId>& level);

        const std::vector<IYtGraph::TOperationNodeId>& Level;

        ::NThreading::TPromise<void> Promise;
        ::NThreading::TFuture<void> MainFuture;

        i32 AvailableToRun;
        ssize_t Completed = 0;
        ssize_t Cursor = 0;

        bool HasError = false;
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

DECLARE_REFCOUNTED_TYPE(TOperationRunner)

////////////////////////////////////////////////////////////////////////////////

TOperationRunnerPtr MakeOperationRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

