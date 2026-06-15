#include "task_node_progress.h"

namespace NYql {

using namespace NProgressMerger;

//////////////////////////////////////////////////////////////////////////////

TNodeProgress::TNodeProgress(const TOperationProgress& p)
    : TNodeProgressBase(p)
{}

TNodeProgress::TNodeProgress(const NProto::TTaskProgress::TNodeProgress& p)
    : TNodeProgressBase(
        TOperationProgress(p.GetCategory(), p.GetId(), ConvertState(p.GetState()), {}, ProtoToAlerts(p)),
        TInstant::MilliSeconds(p.GetStartedAt()),
        p.HasFinishedAt()
            ? TInstant::MilliSeconds(p.GetFinishedAt())
            : TInstant::Max(),
        ProtoToStages(p))
{
    if (!Stages_.empty()) {
        Progress_.Stage = Stages_.back();
    }
    Progress_.RemoteId = p.GetRemoteId();
    if (p.HasCounters()) {
        Progress_.Counters.ConstructInPlace();
        Progress_.Counters->Completed = p.GetCounters().GetCompleted();
        Progress_.Counters->Running = p.GetCounters().GetRunning();
        Progress_.Counters->Total = p.GetCounters().GetTotal();
        Progress_.Counters->Aborted = p.GetCounters().GetAborted();
        Progress_.Counters->Failed = p.GetCounters().GetFailed();
        Progress_.Counters->Lost = p.GetCounters().GetLost();
        Progress_.Counters->Pending = p.GetCounters().GetPending();
        THashMap<TString, i64> customCounters;
        for (auto& [k, v] : p.GetCustomCounters()) {
            customCounters.emplace(k, v);
        }
        Progress_.Counters->Custom = customCounters;
    }
    if (p.HasBlockStatus()) {
        Progress_.BlockStatus = ConvertBlockStatus(p.GetBlockStatus());
    }
}

bool TNodeProgress::MergeWith(const NProto::TTaskProgress::TNodeProgress& p)
{
    bool dirty = false;

    // (1) remote id
    if (!p.GetRemoteId().empty() && p.GetRemoteId() != Progress_.RemoteId) {
        Progress_.RemoteId = p.GetRemoteId();
        dirty = true;
    }

    // (2) state
    auto newState = ConvertState(p.GetState());
    if (newState != Progress_.State) {
        Progress_.State = newState;
        dirty = true;
    }

    // (3) counters
    if (p.HasCounters()) {
        THashMap<TString, i64> customCounters;
        for (auto& [k, v] : p.GetCustomCounters()) {
            customCounters.emplace(k, v);
        }
        TOperationProgress::TCounters newCounters {
                p.GetCounters().GetCompleted(),
                p.GetCounters().GetRunning(),
                p.GetCounters().GetTotal(),
                p.GetCounters().GetAborted(),
                p.GetCounters().GetFailed(),
                p.GetCounters().GetLost(),
                p.GetCounters().GetPending()
                , std::move(customCounters)
        };

        auto& oldCounters = Progress_.Counters;
        if (!oldCounters.Defined() || newCounters != *oldCounters) {
            oldCounters = newCounters;
            dirty = true;
        }
    }

    // (4) finished time
    if (p.HasFinishedAt()) {
        auto newFinishedAt = TInstant::MilliSeconds(p.GetFinishedAt());
        if (FinishedAt_ != newFinishedAt) {
            FinishedAt_ = newFinishedAt;
            dirty = true;
        }
    }

    // (5) progress
    auto stages = ProtoToStages(p);
    if (stages != Stages_) {
        Stages_ = stages;
        Progress_.Stage = Stages_.back();
        dirty = true;
    }

    // (6) block status
    if (p.HasBlockStatus()) {
        auto newBlockStatus = ConvertBlockStatus(p.GetBlockStatus());
        auto& oldBlockStatus = Progress_.BlockStatus;
        if (!oldBlockStatus.Defined() || newBlockStatus != *oldBlockStatus) {
            oldBlockStatus = newBlockStatus;
            dirty = true;
        }
    }

    // (7) alerts
    auto newAlerts = ProtoToAlerts(p);
    if (newAlerts != Progress_.Alerts) {
        Progress_.Alerts = newAlerts;
        dirty = true;
    }

    SetDirty(dirty);
    return dirty;
}

void TNodeProgress::FlushTo(NProto::TTaskProgress::TNodeProgress* proto)
{
    proto->SetId(Progress_.Id);
    proto->SetCategory(Progress_.Category);
    proto->SetState(ConvertState(Progress_.State));
    proto->SetRemoteId(Progress_.RemoteId);
    if (Progress_.Counters) {
        auto mut = proto->MutableCounters();
        mut->SetCompleted(Progress_.Counters->Completed);
        mut->SetRunning(Progress_.Counters->Running);
        mut->SetTotal(Progress_.Counters->Total);
        mut->SetAborted(Progress_.Counters->Aborted);
        mut->SetFailed(Progress_.Counters->Failed);
        mut->SetLost(Progress_.Counters->Lost);
        mut->SetPending(Progress_.Counters->Pending);
        auto& custom = *proto->MutableCustomCounters();
        for (auto& [k, v] : Progress_.Counters->Custom) {
            custom[k] = v;
        }
    }

    proto->SetStartedAt(StartedAt_.MilliSeconds());
    StagesToProto(Stages_, proto);
    if (FinishedAt_ != TInstant::Max()) {
        proto->SetFinishedAt(FinishedAt_.MilliSeconds());
    }
    if (Progress_.BlockStatus) {
        proto->SetBlockStatus(ConvertBlockStatus(*Progress_.BlockStatus));
    }
    AlertsToProto(Progress_.Alerts, proto);
    SetDirty(false);
}

NProto::TTaskProgress::ENodeState TNodeProgress::ConvertState(EState s)
{
    switch (s) {
    case EState::Started: return NProto::TTaskProgress::STARTED;
    case EState::InProgress: return NProto::TTaskProgress::IN_PROGRESS;
    case EState::Finished: return NProto::TTaskProgress::FINISHED;
    case EState::Failed: return NProto::TTaskProgress::FAILED;
    case EState::Aborted: return NProto::TTaskProgress::ABORTED;
    default:
        ythrow yexception() << "unknown task progress state: "
                            << static_cast<int>(s);
    }
}

TNodeProgress::EState TNodeProgress::ConvertState(NProto::TTaskProgress::ENodeState s)
{
    switch (s) {
    case NProto::TTaskProgress::STARTED: return EState::Started;
    case NProto::TTaskProgress::IN_PROGRESS: return EState::InProgress;
    case NProto::TTaskProgress::FINISHED: return EState::Finished;
    case NProto::TTaskProgress::FAILED: return EState::Failed;
    case NProto::TTaskProgress::ABORTED: return EState::Aborted;
    default:
        ythrow yexception() << "unknown task progress state: "
                            << NProto::TTaskProgress::ENodeState_Name(s);
    }
}

NProto::TTaskProgress::ENodeBlockStatus TNodeProgress::ConvertBlockStatus(TOperationProgress::EOpBlockStatus s)
{
    switch (s) {
    case TOperationProgress::EOpBlockStatus::None: return NProto::TTaskProgress::NONE;
    case TOperationProgress::EOpBlockStatus::Partial: return NProto::TTaskProgress::PARTIAL;
    case TOperationProgress::EOpBlockStatus::Full: return NProto::TTaskProgress::FULL;
    }
    Y_UNREACHABLE();
}

TOperationProgress::EOpBlockStatus TNodeProgress::ConvertBlockStatus(NProto::TTaskProgress::ENodeBlockStatus s)
{
    switch (s) {
    case NProto::TTaskProgress::NONE: return TOperationProgress::EOpBlockStatus::None;
    case NProto::TTaskProgress::PARTIAL: return TOperationProgress::EOpBlockStatus::Partial;
    case NProto::TTaskProgress::FULL: return TOperationProgress::EOpBlockStatus::Full;
    }
    Y_UNREACHABLE();
}

TVector<TOperationProgress::TStage> TNodeProgress::ProtoToStages(const NProto::TTaskProgress::TNodeProgress& proto)
{
    TVector<TOperationProgress::TStage> stages;
    for (size_t i = 0; i < proto.StagesSize(); ++i) {
        auto stage = proto.GetStages(i);
        stages.emplace_back(stage.GetName(), TInstant::MilliSeconds(stage.GetStartedAt()));
    }
    return stages;
}

void TNodeProgress::StagesToProto(
    const TVector<TOperationProgress::TStage>& stages,
    NProto::TTaskProgress::TNodeProgress* proto)
{
    proto->ClearStages();
    for (auto& el : stages) {
        NProto::TTaskProgress::TNodeProgress::TStage* stage = proto->AddStages();
        stage->SetName(el.first);
        stage->SetStartedAt(el.second.MilliSeconds());
    }
}

TVector<TOperationProgress::TAlert> TNodeProgress::ProtoToAlerts(const NProto::TTaskProgress::TNodeProgress& proto)
{
    TVector<TOperationProgress::TAlert> alerts;
    for (size_t i = 0; i < proto.AlertsSize(); i++) {
        auto alert = proto.GetAlerts(i);
        alerts.push_back(TOperationProgress::TAlert(alert.GetType(), alert.GetMessage()));
    }
    return alerts;
}

void TNodeProgress::AlertsToProto(
    const TVector<TOperationProgress::TAlert>& alerts,
    NProto::TTaskProgress::TNodeProgress* proto)
{
    proto->ClearAlerts();
    for (const auto& el : alerts) {
        auto* alert = proto->AddAlerts();
        alert->SetType(el.Type);
        alert->SetMessage(el.Message);
    }
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYql
