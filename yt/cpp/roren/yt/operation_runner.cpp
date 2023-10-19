#include "operation_runner.h"

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/memory/ref_counted.h>

#include <cstddef>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;

TOperationRunner::TState::TState(i32 concurrencyLimit, const std::vector<IYtGraph::TOperationNodeId>& level)
    : Level(level)
    , Promise(::NThreading::NewPromise())
    , MainFuture(Promise)
    , AvailableToRun(concurrencyLimit)
{ }

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TOperationRunner)

TOperationRunner::TOperationRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit)
    : Tx_(tx)
    , YtGraph_(ytGraph)
    , ConcurrencyLimit_(concurrencyLimit)
{ }

void TOperationRunner::RunOperations(
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

    State_->MainFuture.GetValueSync();
}

void TOperationRunner::StartAllAvailable(const TStartOperationContext& context)
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
                context
            ] (const ::NThreading::TFuture<void>& operationFuture) {
                auto guard = Guard(self->Lock_);

                if (state->HasError) {
                    return;
                }

                try {
                    operationFuture.TryRethrow();

                    ++state->AvailableToRun;
                    ++state->Completed;

                    if (state->Completed == std::ssize(state->Level)) {
                        state->Promise.SetValue();
                    } else {
                        self->StartAllAvailable(context);
                    }
                } catch (...) {
                    state->HasError = true;
                    state->Promise.TrySetException(std::current_exception());
                }
            });
    }
}

::NThreading::TFuture<void> TOperationRunner::StartOperation(
    IYtGraph::TOperationNodeId operationNodeId,
    const TStartOperationContext& context)
{
    auto operation = YtGraph_->StartOperation(Tx_, operationNodeId, context);
    YT_LOG_DEBUG("Operation was started (OperationId: %v)", operation->GetId());

    return operation->Watch();
}

////////////////////////////////////////////////////////////////////////////////

TOperationRunnerPtr MakeOperationRunner(
    NYT::IClientBasePtr tx,
    std::shared_ptr<IYtGraph> ytGraph,
    i32 concurrencyLimit)
{
    return ::NYT::New<TOperationRunner>(tx, ytGraph, concurrencyLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

