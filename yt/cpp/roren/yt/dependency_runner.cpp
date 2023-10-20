#include "dependency_runner.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <cstddef>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

TDependencyRunner::TState::TState(i32 concurrencyLimit)
    : Promise(::NThreading::NewPromise())
    , MainFuture(Promise)
    , AvailableToRun(concurrencyLimit)
{ }

////////////////////////////////////////////////////////////////////////////////

TDependencyRunner::TDependencyRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit)
    : Tx_(tx)
    , YtGraph_(ytGraph)
    , ConcurrencyLimit_(concurrencyLimit)
    , State_(concurrencyLimit)
{
    Init();
}

void TDependencyRunner::RunOperations(const TStartOperationContext& context)
{
    if (OperationsAvailableToRun_.empty()) {
        return;
    }

    {
        auto guard = Guard(Lock_);
        StartAllAvailable(context);
    }

    State_.MainFuture.GetValueSync();
}

void TDependencyRunner::StartAllAvailable(const TStartOperationContext& context)
{
    for (
        ;
        State_.Cursor < std::ssize(OperationsAvailableToRun_) && State_.AvailableToRun > 0;
        ++State_.Cursor, --State_.AvailableToRun
    ) {
        StartOperation(OperationsAvailableToRun_[State_.Cursor], context).Subscribe(
            [
                self = ::NYT::MakeStrong(this),
                id = State_.Cursor,
                context
            ] (const ::NThreading::TFuture<void>& operationFuture) {
                auto guard = Guard(self->Lock_);

                auto& state = self->State_;

                if (state.HasError) {
                    return;
                }

                try {
                    operationFuture.TryRethrow();

                    ++state.AvailableToRun;
                    ++state.Completed;

                    self->CompleteNext(id);

                    if (state.Completed == std::ssize(self->OperationsAvailableToRun_)) {
                        state.Promise.SetValue();
                    } else {
                        self->StartAllAvailable(context);
                    }
                } catch (...) {
                    state.HasError = true;
                    state.Promise.TrySetException(std::current_exception());
                }
            });
    }
}

::NThreading::TFuture<void> TDependencyRunner::StartOperation(
    IYtGraph::TOperationNodeId operationNodeId,
    const TStartOperationContext& context)
{
    auto operation = YtGraph_->StartOperation(Tx_, operationNodeId, context);
    YT_LOG_DEBUG("Operation was started (OperationId: %v)", operation->GetId());

    return operation->Watch();
}

void TDependencyRunner::Init()
{
    NextOperationMapping_ = YtGraph_->GetNextOperationMapping();

    for (const auto& [id, _] : NextOperationMapping_) {
        DependencyCountMapping_[id] = 0;
    }

    for (const auto& [id, nextOperations] : NextOperationMapping_) {
        for (const auto& nextId : nextOperations) {
            DependencyCountMapping_[nextId] += 1;
        }
    }

    for (const auto& [id, count] : DependencyCountMapping_) {
        if (count == 0) {
            OperationsAvailableToRun_.push_back(id);
        }
    }
}

void TDependencyRunner::CompleteNext(IYtGraph::TOperationNodeId operationNodeId)
{
    const auto& nextOperations = NextOperationMapping_[operationNodeId];
    for (const auto& nextId : nextOperations) {
        DependencyCountMapping_[nextId] -= 1;

        if (DependencyCountMapping_[nextId] == 0) {
            OperationsAvailableToRun_.push_back(nextId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TDependencyRunnerPtr MakeDependencyRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit)
{
    return ::NYT::New<TDependencyRunner>(tx, ytGraph, concurrencyLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

