#include "level_runner.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <cstddef>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

TLevelRunner::TState::TState(i32 concurrencyLimit, const std::vector<IYtGraph::TOperationNodeId>& level)
    : Level(level)
    , AvailableToRun(concurrencyLimit)
    , Promise_(::NThreading::NewPromise())
    , MainFuture_(Promise_)
{ }

void TLevelRunner::TState::WaitOperations()
{
    MainFuture_.GetValueSync();
}

void TLevelRunner::TState::SignalCompletion()
{
    Promise_.SetValue();
}

void TLevelRunner::TState::SignalError()
{
    HasError_ = true;
    Promise_.TrySetException(std::current_exception());
}

bool TLevelRunner::TState::HasError() const
{
    return HasError_;
}

void TLevelRunner::TState::RegisterRunningOperation(
    IYtGraph::TOperationNodeId operationNodeId,
    ::NYT::IOperationPtr operation)
{
    RunningOperations_[operationNodeId] = operation;
}

void TLevelRunner::TState::UnregisterRunningOperation(IYtGraph::TOperationNodeId operationNodeId)
{
    RunningOperations_.erase(operationNodeId);
}

void TLevelRunner::TState::AbortRunningOperations()
{
    for (const auto& [_, operation] : RunningOperations_) {
        try {
            operation->AbortOperation();
        } catch (const ::NYT::TErrorResponse&) {
        }
    }

}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TLevelRunner)

TLevelRunner::TLevelRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit)
    : Tx_(tx)
    , YtGraph_(ytGraph)
    , ConcurrencyLimit_(concurrencyLimit)
{ }

void TLevelRunner::RunOperations(
    const std::vector<IYtGraph::TOperationNodeId>& level,
    const TStartOperationContext& context)
{
    if (level.empty()) {
        return;
    }

    State_ = ::NYT::New<TState>(ConcurrencyLimit_, level);

    {
        auto guard = Guard(Lock_);
        StartAllAvailable(context);
    }

    State_->WaitOperations();
}

void TLevelRunner::StartAllAvailable(const TStartOperationContext& context)
{
    for (
        ;
        State_->Cursor < std::ssize(State_->Level) && State_->AvailableToRun > 0;
        ++State_->Cursor, --State_->AvailableToRun
    ) {
        StartOperation(State_->Level[State_->Cursor], context).Subscribe(
            [
                self = ::NYT::MakeStrong(this),
                state = State_,
                operationNodeId = State_->Cursor,
                context
            ] (const ::NThreading::TFuture<void>& operationFuture) {
                auto guard = Guard(self->Lock_);

                if (state->HasError()) {
                    return;
                }

                state->UnregisterRunningOperation(operationNodeId);

                try {
                    operationFuture.TryRethrow();

                    ++state->AvailableToRun;
                    ++state->Completed;

                    if (state->Completed == std::ssize(state->Level)) {
                        state->SignalCompletion();
                    } else {
                        self->StartAllAvailable(context);
                    }
                } catch (...) {
                    state->AbortRunningOperations();
                    state->SignalError();
                }
            });
    }
}

::NThreading::TFuture<void> TLevelRunner::StartOperation(
    IYtGraph::TOperationNodeId operationNodeId,
    const TStartOperationContext& context)
{
    auto operation = YtGraph_->StartOperation(Tx_, operationNodeId, context);
    YT_LOG_DEBUG("Operation was started (OperationId: %v)", operation->GetId());

    State_->RegisterRunningOperation(operationNodeId, operation);

    return operation->Watch();
}

////////////////////////////////////////////////////////////////////////////////

TLevelRunnerPtr MakeLevelRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit)
{
    return ::NYT::New<TLevelRunner>(tx, ytGraph, concurrencyLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
