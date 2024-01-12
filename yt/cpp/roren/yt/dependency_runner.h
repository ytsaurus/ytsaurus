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
        NYT::IClientBasePtr client,
        std::shared_ptr<TYtGraphV2> ytGraph,
        i32 concurrencyLimit);

    void RunOperations(const TStartOperationContext& context);

private:
    class TState
    {
    public:
        explicit TState(i32 concurrencyLimit);

        void WaitOperations();
        void TryRethrow();
        void SignalCompletion();
        void SignalError(IYtGraph::TOperationNodeId operationNodeId);

        bool HasError() const;
        IYtGraph::TOperationNodeId GetErrorNodeId() const;

    public:
        i32 AvailableToRun;
        ssize_t Completed = 0;
        ssize_t Cursor = 0;

    private:
        ::NThreading::TPromise<void> Promise_;
        ::NThreading::TFuture<void> MainFuture_;

        std::optional<IYtGraph::TOperationNodeId> ErrorNodeId_;
    };

private:
    void StartAllAvailable(NYT::ITransactionPtr tx, const TStartOperationContext& context);

    NThreading::TFuture<void> StartOperation(
        NYT::ITransactionPtr tx,
        IYtGraph::TOperationNodeId operationNodeId,
        const TStartOperationContext& context);

    void Init();
    void CompleteNext(IYtGraph::TOperationNodeId operationNodeId);

private:
    TAdaptiveLock Lock_;

    NYT::IClientBasePtr Client_;
    std::shared_ptr<TYtGraphV2> YtGraph_;

    THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>> NextOperationMapping_;
    THashMap<IYtGraph::TOperationNodeId, ssize_t> DependencyCountMapping_;

    std::vector<IYtGraph::TOperationNodeId> OperationsAvailableToRun_;

    TState State_;
};

using TDependencyRunnerPtr = ::NYT::TIntrusivePtr<TDependencyRunner>;

////////////////////////////////////////////////////////////////////////////////

TDependencyRunnerPtr MakeDependencyRunner(
    NYT::IClientBasePtr client,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

