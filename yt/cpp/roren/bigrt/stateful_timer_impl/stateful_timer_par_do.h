#pragma once

#include "../fwd.h"

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/private/fwd.h>
#include <yt/cpp/roren/interface/private/raw_pipeline.h>
#include <yt/cpp/roren/interface/private/raw_transform.h>
#include <yt/cpp/roren/interface/private/raw_state_store.h>
#include <yt/cpp/roren/bigrt/proto/config.pb.h>
#include <yt/cpp/roren/bigrt/stateful_impl/stateful_par_do.h>
#include <yt/cpp/roren/library/timers/timers.h>
#include <bigrt/lib/processing/state_manager/base/manager.h>

#include <yt/yt/core/actions/future.h>

namespace NRoren::NPrivate {

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

class TStatefulTimerParDoWrapper
    : public NPrivate::IRawParDo
{
private:
    using TResolvedRequestList = TVector<NBigRT::TBaseStateRequestPtr>;
    using TRawRowVector = std::vector<NPrivate::TRawRowHolder>;

public:
    TStatefulTimerParDoWrapper() = default;
    ~TStatefulTimerParDoWrapper();

    TStatefulTimerParDoWrapper(IRawStatefulTimerParDoPtr underlying, TString stateManagerId, TBigRtStateManagerVtable stateManagerVtable, TBigRtStateConfig config);

    const TFnAttributes& GetFnAttributes() const override;
    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override;
    void Do(const void* rows, int count) override;
    void OnTimer(std::vector<TTimer> timer);
    void Flush();
    void Finish() override;
    std::vector<TDynamicTypeTag> GetInputTags() const override;
    std::vector<TDynamicTypeTag> GetOutputTags() const override;
    TDefaultFactoryFunc GetDefaultFactory() const override;
    void Save(IOutputStream* stream) const override;
    void Load(IInputStream* stream) override;

private:
    struct TBatch
    {
        std::deque<TRawRowHolder> Rows;
        std::deque<std::tuple<TRawRowHolder, TTimer>> Timers;
    };

    void SetAttribute(const TString& key, const std::any& value) override;
    const std::any* GetAttribute(const TString& key) const override;
    void ProcessRow(NRoren::NPrivate::TRawRowHolder row);
    void ProcessTimer(NRoren::NPrivate::TRawRowHolder key, const NRoren::TTimer& timer);
    void UpdateStateMap(const std::vector<NBigRT::TBaseStateRequestPtr>& requestList);
    void Initialize();
    TBatch PopPending(const NBigRT::TBaseStateRequestPtr& request);
    void EnqueueReadyRequests(TResolvedRequestList readyRequests);
    void ProcessReadyRequests(const TResolvedRequestList& readyRequests);
    void ExecuteReadyQueue();

private:
    IRawStatefulTimerParDoPtr Underlying_;
    TString StateManagerId_;
    TBigRtStateManagerVtable StateManagerVtable_;
    TBigRtStateConfig Config_;
    TFnAttributes FnAttributes_;

    TRowVtable RowVtable_;
    TRowVtable KeyVTable_;
    IRawCoderPtr KeyRawCoder_;

    IBigRtExecutionContextPtr BigRtContext_;
    NYT::NApi::IClientPtr Client_;
    NBigRT::TBaseStateManagerPtr StateManager_;
    IRawStateStorePtr RawStateStore_;

    using THashableRequest = NBigRT::TBaseStateRequest::THashable;
    THashMap<THashableRequest, TBatch> Batch_;
    THashMap<THashableRequest, TBatch> Requested_;
    std::queue<NYT::TFuture<void>> RequestsQueue_;
    std::deque<TResolvedRequestList> ReadyQueue_;
    NYT::TFuture<void> ReadyQueueExecutor_ = NYT::VoidFuture;

    ui64 Epoch_ = 0;

    NYT::NThreading::TSpinLock Lock_;
    std::atomic<bool> Terminate_ = false;
};

////////////////////////////////////////////////////////////////////////////////

using TStatefulTimerParDoWrapperPtr = TIntrusivePtr<TStatefulTimerParDoWrapper>;

TStatefulTimerParDoWrapperPtr CreateStatefulTimerParDo(
    IRawStatefulTimerParDoPtr rawStatefulTimerParDo,
    TString stateManagerId,
    TBigRtStateManagerVtable stateManagerVtable,
    TBigRtStateConfig stateConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
