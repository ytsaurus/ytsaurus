#include "process_function_test_harness.h"

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

TProcessFunctionTestHarness::TProcessFunctionTestHarness(
    TTestStateEnvironment& env,
    IProcessFunctionBasePtr function,
    IRuntimeContextPtr context)
    : Env_(env)
    , Function_(std::move(function))
    , Batch_(WrapAsBatch(Function_))
    // Recover the sync phase by cross-cast: the function is constructed directly, not via the registry.
    , SyncFunction_(dynamic_cast<ISyncProcessFunction*>(Function_.Get()))
    , Context_(std::move(context))
    , Output_(New<TRecordingOutputCollector>())
{ }

void TProcessFunctionTestHarness::RunEpoch(const IInputContextPtr& input)
{
    EnsureInitialized();

    Env_.PreloadEpoch(input);

    Output_ = New<TRecordingOutputCollector>();
    Batch_->Process(input, Output_, Context_);

    Env_.CommitEpoch([&] (const IRetryableTransactionPtr& transaction) {
        if (SyncFunction_) {
            SyncFunction_->Sync(transaction, Context_);
        }
    });
}

void TProcessFunctionTestHarness::RunEpoch(
    std::vector<TInputMessageConstPtr> messages,
    std::vector<TInputTimerConstPtr> timers,
    std::vector<TInputVisitConstPtr> visits)
{
    RunEpoch(New<TInputContext>(std::move(messages), std::move(timers), std::move(visits)));
}

const std::vector<TRecordingOutputCollector::TRecordedMessage>& TProcessFunctionTestHarness::GetMessages() const
{
    return Output_->GetMessages();
}

const std::vector<TRecordingOutputCollector::TRecordedTimer>& TProcessFunctionTestHarness::GetTimers() const
{
    return Output_->GetTimers();
}

const IRuntimeContextPtr& TProcessFunctionTestHarness::GetContext() const
{
    return Context_;
}

void TProcessFunctionTestHarness::EnsureInitialized()
{
    if (Initialized_) {
        return;
    }
    // Lazy, so the test can tweak the environment before the first epoch.
    Function_->Init(Env_.GetInitContext());
    Initialized_ = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
