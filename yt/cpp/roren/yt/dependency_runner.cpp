#include "dependency_runner.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/memory/ref_counted.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

TDependencyRunner::TState::TState(i32 concurrencyLimit)
    : AvailableToRun(concurrencyLimit)
    , Promise_(::NThreading::NewPromise())
    , MainFuture_(Promise_)
{ }

void TDependencyRunner::TState::WaitOperations()
{
    MainFuture_.Wait();
}

void TDependencyRunner::TState::TryRethrow()
{
    MainFuture_.TryRethrow();
}

void TDependencyRunner::TState::SignalCompletion()
{
    Promise_.SetValue();
}

void TDependencyRunner::TState::SignalError(IYtGraph::TOperationNodeId operationNodeId)
{
    ErrorNodeId_ = operationNodeId;
    Promise_.TrySetException(std::current_exception());
}

bool TDependencyRunner::TState::HasError() const
{
    return ErrorNodeId_.has_value();
}

IYtGraph::TOperationNodeId TDependencyRunner::TState::GetErrorNodeId() const
{
    return ErrorNodeId_.value();
}

////////////////////////////////////////////////////////////////////////////////

TDependencyRunner::TDependencyRunner(
    NYT::IClientBasePtr client,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit)
    : Client_(std::move(client))
    , YtGraph_(ytGraph)
    , State_(concurrencyLimit)
{
    Init();
}

void TDependencyRunner::RunOperations(const TStartOperationContext& context)
{
    if (OperationsAvailableToRun_.empty()) {
        return;
    }

    YtGraph_->CreateWorkingDir(Client_);

    auto tx = Client_->StartTransaction();

    {
        auto guard = Guard(Lock_);
        StartAllAvailable(tx, context);
    }

    State_.WaitOperations();

    if (!State_.HasError()) {
        YtGraph_->ClearIntermediateTables();

        tx->Commit();
    } else {
        YtGraph_->LeaveIntermediateTables(Client_, tx, State_.GetErrorNodeId());

        tx->Abort();

        State_.TryRethrow();
    }
}

void TDependencyRunner::StartAllAvailable(ITransactionPtr tx, const TStartOperationContext& context)
{
    for (
        ;
        State_.Cursor < std::ssize(OperationsAvailableToRun_) && State_.AvailableToRun > 0;
        ++State_.Cursor, --State_.AvailableToRun
    ) {
        StartOperation(tx, OperationsAvailableToRun_[State_.Cursor], context).Subscribe(
            [
                self = ::NYT::MakeStrong(this),
                tx,
                operationNodeId = State_.Cursor,
                context
            ] (const ::NThreading::TFuture<void>& operationFuture) {
                auto guard = Guard(self->Lock_);

                auto& state = self->State_;

                if (state.HasError()) {
                    return;
                }

                try {
                    operationFuture.TryRethrow();

                    ++state.AvailableToRun;
                    ++state.Completed;

                    self->CompleteNext(operationNodeId);

                    if (state.Completed == std::ssize(self->OperationsAvailableToRun_)) {
                        state.SignalCompletion();
                    } else {
                        self->StartAllAvailable(tx, context);
                    }
                } catch (...) {
                    state.SignalError(operationNodeId);
                }
            });
    }
}

::NThreading::TFuture<void> TDependencyRunner::StartOperation(
    ITransactionPtr tx,
    IYtGraph::TOperationNodeId operationNodeId,
    const TStartOperationContext& context)
{
    auto operation = YtGraph_->StartOperation(tx, operationNodeId, context);
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
    NYT::IClientBasePtr client,
    std::shared_ptr<TYtGraphV2> ytGraph,
    i32 concurrencyLimit)
{
    return ::NYT::New<TDependencyRunner>(client, ytGraph, concurrencyLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

