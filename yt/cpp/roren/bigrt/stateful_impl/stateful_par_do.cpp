#include "stateful_par_do.h"

#include "../bigrt_execution_context.h"

#include <yt/cpp/roren/bigrt/proto/config.pb.h>
#include <yt/cpp/roren/interface/private/raw_pipeline.h>

#include <yt/cpp/roren/interface/private/raw_transform.h>
#include <yt/cpp/roren/interface/private/raw_state_store.h>

#include <bigrt/lib/processing/state_manager/base/manager.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRawRowVector
{
public:
    explicit TRawRowVector(NPrivate::TRowVtable rowVtable);

    void PushBack(const void* row);

    void* operator[](size_t idx)
    {
        return Buffer_ + (idx * RowVtable_.DataSize);
    }

private:
    NPrivate::TRowVtable RowVtable_;

    char* Buffer_;
    size_t Size_;
    size_t Capacity_;
};

class TStatefulParDoWrapper
    : public NPrivate::IRawParDo
{
private:
    using TResolvedRequestList = TVector<NBigRT::TBaseStateRequestPtr>;
    using TRawRowVector = std::vector<NPrivate::TRawRowHolder>;

public:
    TStatefulParDoWrapper() = default;

    TStatefulParDoWrapper(NPrivate::IRawStatefulParDoPtr underlying, TString stateManagerId, TBigRtStateManagerVtable stateManagerVtable, TBigRtStateConfig config)
        : Underlying_(std::move(underlying))
        , StateManagerId_(stateManagerId)
        , StateManagerVtable_(std::move(stateManagerVtable))
        , Config_(std::move(config))
    {
        if (auto name = NPrivate::GetAttribute(*Underlying_, NameTag)) {
            auto wrappedName = *name + " (wrapped)";
            NPrivate::SetAttribute(*this, NameTag, wrappedName);
        }
    }

    void Start(const IExecutionContextPtr& context, const std::vector<NPrivate::IRawOutputPtr>& outputs) override
    {
        if (!Initialized_) {
            auto inputTags = Underlying_->GetInputTags();
            Y_VERIFY(inputTags.size() == 1);
            RowVtable_ = inputTags[0].GetRowVtable();

            Initialized_ = true;
        }
        BigRtContext_ = context->As<IBigRtExecutionContext>();
        auto cluster = NPrivate::GetMainCluster(BigRtContext_);
        Client_ = NPrivate::ResolveYtClient(BigRtContext_, cluster);
        StateManager_ = NPrivate::ResolveStateManager(BigRtContext_, StateManagerId_);
        RawStateStore_ = StateManagerVtable_.CreateStateStore();

        Underlying_->Start(context, RawStateStore_, outputs);

        if (Epoch_ == 0) {
            StateManager_->StartNewEpoch(Epoch_);
        }

        Y_VERIFY(RowVtable_.KeyOffset != TRowVtable::NotKv);
    }

    void Do(const void* rows, int count) override
    {
        auto curRow = static_cast<const char*>(rows);

        for (int i = 0; i < count; ++i, curRow += RowVtable_.DataSize) {
            RowBatch_.emplace_back(RowVtable_);
            RowBatch_.back().CopyFrom(curRow);
        }

        if (std::ssize(RowBatch_) > Config_.GetRequestStateBatchSize()) {
            Flush();
        }
    }

    void Flush()
    {
        if (RowBatch_.empty()) {
            return;
        }

        // First of all we check all incoming keys and make requests for state manager
        TVector<NBigRT::TBaseStateRequestPtr> requestList;
        for (const auto& row : RowBatch_) {
            const auto* key = row.GetKeyOfKV();
            auto request = StateManagerVtable_.KeyToRequest(StateManager_.Get(), key);
            requestList.push_back(std::move(request));
        }

        // Make request to load states and update our map with immediately available values.
        auto&& [ready, inProgress] = StateManager_->LoadStates(std::move(requestList), Client_);
        UpdateStateMap(ready);

        RequestQueue_.emplace(inProgress, std::move(RowBatch_));
        RowBatch_.clear();
        if (RequestQueue_.size() == 1) {
            RequestQueue_.front().first.Apply(
                BIND([pThis = TIntrusivePtr(this)] (const TResolvedRequestList&) {
                    pThis->ProcessRequestQueue();
                }).AsyncVia(NYT::GetCurrentInvoker())
                // TODO: какой тут должен быть правильный invoker ???
            );
        }
    }

    void Finish() override
    {
        Flush();

        while (!RequestQueue_.empty()) {
            auto& future = RequestQueue_.front().first;

            NYT::NConcurrency::WaitUntilSet(future.AsVoid());

            ProcessRequestQueue();
        }

        auto writer = StateManager_->WriteStates();
        auto commitingEpoch = Epoch_;
        StateManager_->StartNewEpoch(++Epoch_);
        AddTransactionWriter(BigRtContext_, std::move(writer));
        AddOnCommitCallback(BigRtContext_, BIND([
            stateManager = StateManager_,
            commitingEpoch = commitingEpoch
        ] (const TProcessorCommitContext& context) {
            if (context.Success) {
                auto timestamp = context.TransactionCommitResult.CommitTimestamps.Timestamps[0].second;
                stateManager->ReleaseEpoch(commitingEpoch, timestamp);
            }
        }));
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return Underlying_->GetInputTags();
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return Underlying_->GetOutputTags();
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> NPrivate::IRawParDoPtr {
            return ::MakeIntrusive<TStatefulParDoWrapper>();
        };
    }

    void SaveState(IOutputStream& stream) const override
    {
        NPrivate::SaveSerializable(&stream, Underlying_);
        ::Save(&stream, StateManagerId_);
        ::Save(&stream, StateManagerVtable_);
        ::Save(&stream, Config_);
    }

    void LoadState(IInputStream& stream) override
    {
        NPrivate::LoadSerializable(&stream, Underlying_);
        ::Load(&stream, StateManagerId_);
        ::Load(&stream, StateManagerVtable_);
        ::Load(&stream, Config_);
    }

private:
    void ProcessRequestQueue()
    {
        while (!RequestQueue_.empty()) {
            auto&& [future, rows] = RequestQueue_.front();
            if (!future.IsSet()) {
                break;
            }

            auto resolvedList = future.GetUnique()
                .ValueOrThrow();

            UpdateStateMap(resolvedList);

            for (const auto& rowHolder : rows) {
                Underlying_->Do(rowHolder.GetData(), 1);
            }

            RequestQueue_.pop();
        }
    }

    void UpdateStateMap(const TVector<NBigRT::TBaseStateRequestPtr>& requestList)
    {
        for (const auto& baseRequest : requestList) {
            StateManagerVtable_.StateStorePut(RawStateStore_.Get(), baseRequest.Get());
        }
    }

private:
    NPrivate::IRawStatefulParDoPtr Underlying_;
    TString StateManagerId_;
    TBigRtStateManagerVtable StateManagerVtable_;
    TBigRtStateConfig Config_;

    std::vector<NPrivate::TRawRowHolder> RowBatch_;

    NBigRT::TBaseStateManagerPtr StateManager_;
    NPrivate::TRowVtable RowVtable_;

    NYT::NApi::IClientPtr Client_;
    IRawStateStorePtr RawStateStore_ = nullptr;

    std::queue<std::pair<NYT::TFuture<TResolvedRequestList>, TRawRowVector>> RequestQueue_;

    IBigRtExecutionContextPtr BigRtContext_;

    bool Initialized_ = false;

    ui64 Epoch_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

NPrivate::IRawParDoPtr CreateStatefulParDo(
    IRawStatefulParDoPtr rawStatefulParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable stateManagerVtable,
    TBigRtStateConfig stateConfig)
{
    return ::MakeIntrusive<TStatefulParDoWrapper>(
        std::move(rawStatefulParDo),
        std::move(stateManagerId),
        std::move(stateManagerVtable),
        std::move(stateConfig)
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

void TSerializer<NRoren::NPrivate::TBigRtStateManagerVtable>::Save(IOutputStream* output, const NRoren::NPrivate::TBigRtStateManagerVtable& vtable)
{
    ::Save(output, reinterpret_cast<ui64>(vtable.CreateStateStore));
    ::Save(output, reinterpret_cast<ui64>(vtable.KeyToRequest));
    ::Save(output, reinterpret_cast<ui64>(vtable.StateStorePut));
}

void TSerializer<NRoren::NPrivate::TBigRtStateManagerVtable>::Load(IInputStream* input, NRoren::NPrivate::TBigRtStateManagerVtable& vtable)
{
    using namespace NRoren::NPrivate;

    ::Load(input, *reinterpret_cast<ui64*>(&vtable.CreateStateStore));
    ::Load(input, *reinterpret_cast<ui64*>(&vtable.KeyToRequest));
    ::Load(input, *reinterpret_cast<ui64*>(&vtable.StateStorePut));
}
