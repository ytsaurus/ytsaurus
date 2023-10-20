#pragma once

#include <yt/cpp/roren/yt/yt_graph_v2.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <util/system/spinlock.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TDependencyRunner
    : public ::NYT::TRefCounted
{
public:
    TDependencyRunner(
        NYT::IClientBasePtr tx,
        std::shared_ptr<TYtGraphV2> ytGraph,
        i32 concurrencyLimit);

    void RunOperations(const TStartOperationContext& context);

private:
    struct TState
    {
        explicit TState(i32 concurrencyLimit);

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

    void Init();
    void CompleteNext(IYtGraph::TOperationNodeId operationNodeId);

private:
    TAdaptiveLock Lock_;

    NYT::IClientBasePtr Tx_;
    std::shared_ptr<TYtGraphV2> YtGraph_;
    i32 ConcurrencyLimit_;

    THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>> NextOperationMapping_;
    THashMap<IYtGraph::TOperationNodeId, ssize_t> DependencyCountMapping_;

    std::vector<IYtGraph::TOperationNodeId> OperationsAvailableToRun_;

    TState State_;
};

using TDependencyRunnerPtr = ::NYT::TIntrusivePtr<TDependencyRunner>;

////////////////////////////////////////////////////////////////////////////////

TDependencyRunnerPtr MakeDependencyRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

