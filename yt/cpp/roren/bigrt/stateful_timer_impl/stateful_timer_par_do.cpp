#include "stateful_timer_par_do.h"

#include "../bigrt_execution_context.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TStatefulTimerParDoWrapper::TStatefulTimerParDoWrapper(IRawStatefulTimerParDoPtr underlying, TString stateManagerId, TBigRtStateManagerVtable stateManagerVtable, TBigRtStateConfig config)
    : Underlying_(std::move(underlying))
    , StateManagerId_(stateManagerId)
    , StateManagerVtable_(std::move(stateManagerVtable))
    , Config_(std::move(config))
    , FnAttributes_(Underlying_->GetFnAttributes())
{
    Initialize();
}

TStatefulTimerParDoWrapper::~TStatefulTimerParDoWrapper()
{
    Terminate_ = true;
    Y_ABORT_IF(!NYT::NConcurrency::WaitFor(ReadyQueueExecutor_).IsOK());
}

void TStatefulTimerParDoWrapper::SetAttribute(const TString& key, const std::any& value)
{
    Underlying_->SetAttribute(key, value);
}

const std::any* TStatefulTimerParDoWrapper::GetAttribute(const TString& key) const
{
    return Underlying_->GetAttribute(key);
}

void TStatefulTimerParDoWrapper::Initialize()
{
    auto inputTags = Underlying_->GetInputTags();
    Y_ABORT_UNLESS(inputTags.size() == 1);
    RowVtable_ = inputTags[0].GetRowVtable();
    Y_ABORT_UNLESS(RowVtable_.KeyOffset != TRowVtable::NotKv);
    KeyVTable_ = RowVtable_.KeyVtableFactory();
    KeyRawCoder_ = KeyVTable_.RawCoderFactory();
}

void TStatefulTimerParDoWrapper::Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs)
{
    BigRtContext_ = context->As<IBigRtExecutionContext>();
    auto cluster = TBigRtExecutionContextOps::GetMainCluster(BigRtContext_);
    Client_ = TBigRtExecutionContextOps::ResolveYtClient(BigRtContext_, cluster);
    StateManager_ = TBigRtExecutionContextOps::ResolveStateManager(BigRtContext_, StateManagerId_);
    RawStateStore_ = StateManagerVtable_.CreateStateStore();

    Underlying_->Start(context, RawStateStore_, outputs);

    if (Epoch_ == 0) {
        StateManager_->StartNewEpoch(Epoch_);
    }
}

void TStatefulTimerParDoWrapper::Do(const void* rows, int count)
{
    auto curRow = static_cast<const char*>(rows);

    for (int i = 0; i < count; ++i, curRow += RowVtable_.DataSize) {
        TRawRowHolder row(RowVtable_);
        row.CopyFrom(curRow);
        const auto* key = row.GetKeyOfKV();
        auto request = THashableRequest(StateManagerVtable_.KeyToRequest(StateManager_.Get(), key));
        {
            auto guard = Guard(Lock_);
            auto it = Requested_.find(request);
            if (it != Requested_.end()) {
                it->second.Rows.push_back(std::move(row));
            } else {
                Batch_[request].Rows.push_back(std::move(row));
            }
        }
    }

    if (std::ssize(Batch_) > Config_.GetRequestStateBatchSize()) {
        Flush();
    }
}

void TStatefulTimerParDoWrapper::OnTimer(std::vector<TTimer> timers)
{
    for (auto& timer : timers) {
        TRawRowHolder key(KeyVTable_);
        KeyRawCoder_->DecodeRow(timer.GetKey().GetKey(), key.GetData());
        auto request = THashableRequest(StateManagerVtable_.KeyToRequest(StateManager_.Get(), key.GetData()));
        {
            auto guard = Guard(Lock_);
            auto it = Requested_.find(request);
            if (it != Requested_.end()) {
                it->second.Timers.emplace_back(std::move(key), std::move(timer));
            } else {
                Batch_[request].Timers.emplace_back(std::move(key), std::move(timer));
            }
        }
    }

    if (std::ssize(Batch_) > Config_.GetRequestStateBatchSize()) {
        Flush();
    }
}

void TStatefulTimerParDoWrapper::Flush()
{
    while (!RequestsQueue_.empty()) {
        auto future = RequestsQueue_.front();
        if (!future.IsSet()) {
            break;
        }
        future.Get().ThrowOnError();
        RequestsQueue_.pop();
    }

    TVector<NBigRT::TBaseStateRequestPtr> requestList;
    {
        auto guard = Guard(Lock_);
        // First of all we check all incoming keys and make requests for state manager
        for (auto& [request, batch] : Batch_) {
            Requested_[request] = std::move(batch);
            requestList.emplace_back(request.Get());
        }
        Batch_.clear();
    }

    // Make request to load states and update our map with immediately available values.
    auto [ready, inProgress] = StateManager_->LoadStates(std::move(requestList), Client_);
    auto processRequests = BIND([pThis = TIntrusivePtr(this)] (TResolvedRequestList&& readyRequests) {
        pThis->EnqueueReadyRequests(std::move(readyRequests));
    }).AsyncVia(NYT::GetCurrentInvoker());
    EnqueueReadyRequests(std::move(ready));
    RequestsQueue_.emplace(inProgress.ApplyUnique(processRequests));
}

TStatefulTimerParDoWrapper::TBatch TStatefulTimerParDoWrapper::PopPending(const NBigRT::TBaseStateRequestPtr& request)
{
    auto guard = Guard(Lock_);
    auto it = Requested_.find(request);
    if (it == Requested_.end()) {
        return {};
    }
    TBatch result = std::move(it->second);
    Requested_.erase(it);
    return result;
}

void TStatefulTimerParDoWrapper::ProcessReadyRequests(const TResolvedRequestList& readyRequests)
{
    UpdateStateMap(readyRequests);
    for (const auto& request : readyRequests) {
        auto batch  = PopPending(request);
        for (auto& row : batch.Rows) {
            ProcessRow(std::move(row));
        }
        for (auto& key_timer : batch.Timers) {
            const auto& [key, timer] = key_timer;
            const bool isTimerChanged = TBigRtExecutionContextOps::IsTimerChanged(BigRtContext_, timer.GetKey());
            if (TBigRtExecutionContextOps::GetTimers(BigRtContext_)->IsValidForExecute(timer, isTimerChanged)) {
                BigRtContext_->DeleteTimer(timer.GetKey());
                ProcessTimer(key, timer);
            }
        }
    }
}

void TStatefulTimerParDoWrapper::EnqueueReadyRequests(TResolvedRequestList readyRequests)
{
    auto guard = Guard(Lock_);
    const bool execute = ReadyQueue_.empty();
    ReadyQueue_.push_back(std::move(readyRequests));
    if (execute && !Terminate_) {
        ReadyQueueExecutor_ = BIND([this]() {
            this->ExecuteReadyQueue();
        }).AsyncVia(NYT::GetCurrentInvoker()).Run();
    }
}

void TStatefulTimerParDoWrapper::ExecuteReadyQueue()
{
    auto guard = Guard(Lock_);
    while (!Terminate_ && !ReadyQueue_.empty()) {
        Lock_.Release();
        ProcessReadyRequests(ReadyQueue_.front());
        Lock_.Acquire();
        ReadyQueue_.pop_front();
    }
}

void TStatefulTimerParDoWrapper::Finish()
{
    Flush();

    while (!RequestsQueue_.empty()) {
        // N.B. `WaitFor` is nonatomic, it can hang if object passed to it is updated during waiting
        // so we make a copy of future here.
        auto future = RequestsQueue_.front();
        NYT::NConcurrency::WaitFor(future).ThrowOnError();
        RequestsQueue_.pop();
    }

    NYT::NConcurrency::WaitFor(ReadyQueueExecutor_).ThrowOnError();

    Underlying_->Finish();

    auto writer = StateManager_->WriteStates();
    auto committingEpoch = Epoch_;
    StateManager_->StartNewEpoch(++Epoch_);
    TBigRtExecutionContextOps::AddTransactionWriter(BigRtContext_, std::move(writer));
    TBigRtExecutionContextOps::AddOnCommitCallback(BigRtContext_, BIND([
        stateManager = StateManager_,
        committingEpoch = committingEpoch
    ] (const TProcessorCommitContext& context) {
        if (context.Success) {
            auto timestamp = context.TransactionCommitResult.CommitTimestamps.Timestamps[0].second;
            stateManager->ReleaseEpoch(committingEpoch, timestamp);
        }
    }));
}

std::vector<TDynamicTypeTag> TStatefulTimerParDoWrapper::GetInputTags() const
{
    return Underlying_->GetInputTags();
}

std::vector<TDynamicTypeTag> TStatefulTimerParDoWrapper::GetOutputTags() const
{
    return Underlying_->GetOutputTags();
}

TStatefulTimerParDoWrapper::TDefaultFactoryFunc TStatefulTimerParDoWrapper::GetDefaultFactory() const
{
    return [] () -> IRawParDoPtr {
        return ::MakeIntrusive<TStatefulTimerParDoWrapper>();
    };
}

void TStatefulTimerParDoWrapper::Save(IOutputStream* stream) const
{
    SaveSerializable(stream, Underlying_);
    ::Save(stream, FnAttributes_);
    ::Save(stream, StateManagerId_);
    ::Save(stream, StateManagerVtable_);
    ::Save(stream, Config_);
}

void TStatefulTimerParDoWrapper::Load(IInputStream* stream)
{
    LoadSerializable(stream, Underlying_);
    ::Load(stream, FnAttributes_);
    ::Load(stream, StateManagerId_);
    ::Load(stream, StateManagerVtable_);
    ::Load(stream, Config_);
    Initialize();
}

void TStatefulTimerParDoWrapper::ProcessRow(NRoren::NPrivate::TRawRowHolder row)
{
    Underlying_->Do(row.GetData(), 1);
}

void TStatefulTimerParDoWrapper::ProcessTimer(NRoren::NPrivate::TRawRowHolder key, const NRoren::TTimer& timer)
{
    Underlying_->OnTimer(key.GetData(), timer);
}

void TStatefulTimerParDoWrapper::UpdateStateMap(const std::vector<NBigRT::TBaseStateRequestPtr>& requestList)
{
    for (const auto& baseRequest : requestList) {
        StateManagerVtable_.StateStorePut(RawStateStore_.Get(), baseRequest.Get());
    }
}

const TFnAttributes& TStatefulTimerParDoWrapper::GetFnAttributes() const
{
    return FnAttributes_;
}


////////////////////////////////////////////////////////////////////////////////

TStatefulTimerParDoWrapperPtr CreateStatefulTimerParDo(
    IRawStatefulTimerParDoPtr rawStatefulTimerParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable stateManagerVtable,
    TBigRtStateConfig stateConfig)
{
    return ::MakeIntrusive<TStatefulTimerParDoWrapper>(
        std::move(rawStatefulTimerParDo),
        std::move(stateManagerId),
        std::move(stateManagerVtable),
        std::move(stateConfig)
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
