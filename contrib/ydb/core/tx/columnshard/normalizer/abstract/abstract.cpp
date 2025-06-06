#include "abstract.h"
#include <contrib/ydb/core/tx/columnshard/columnshard_private_events.h>
#include <contrib/ydb/core/tx/columnshard/columnshard_schema.h>

#include <contrib/ydb/core/protos/config.pb.h>

namespace NKikimr::NOlap {

TNormalizationController::INormalizerComponent::TPtr TNormalizationController::RegisterNormalizer(INormalizerComponent::TPtr normalizer) {
    AFL_VERIFY(normalizer);
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalizer_register")("description", normalizer->DebugString());
    Counters.emplace_back(normalizer->GetClassName());
    Normalizers.emplace_back(normalizer);
    return normalizer;
}

const TNormalizationController::INormalizerComponent::TPtr& TNormalizationController::GetNormalizer() const {
    Y_ABORT_UNLESS(Normalizers.size());
    return Normalizers.front();
}

const TNormalizerCounters& TNormalizationController::GetCounters() const {
    Y_ABORT_UNLESS(Counters.size());
    return Counters.front();
}

bool TNormalizationController::TNormalizationController::IsNormalizationFinished() const {
    AFL_VERIFY(Counters.size() == Normalizers.size());
    return Normalizers.empty();
}

bool TNormalizationController::SwitchNormalizer() {
    if (IsNormalizationFinished()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalization_finished");
        return false;
    }
    AFL_VERIFY(!GetNormalizer()->HasActiveTasks());
    GetCounters().OnNormalizerFinish();
    Normalizers.pop_front();
    Counters.pop_front();
    if (Normalizers.size()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalizer_switched")("description", Normalizers.front()->DebugString())(
            "id", Normalizers.front()->GetEnumSequentialId());
    } else {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalization_finished");
    }
    return !IsNormalizationFinished();
}

void TTrivialNormalizerTask::Start(const TNormalizationController& /* controller */, const TNormalizationContext& nCtx) {
    TActorContext::AsActorContext().Send(nCtx.GetShardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(Changes));
}

void TNormalizationController::AddNormalizerEvent(NIceDb::TNiceDb& db, const TString& eventType, const TString& eventDescription) const {
    NColumnShard::Schema::AddNormalizerEvent(db, GetNormalizer()->GetUniqueId(), eventType, eventDescription);
}

void TNormalizationController::OnNormalizerFinished(NIceDb::TNiceDb& db) const {
    if (auto seqId = GetNormalizer()->GetSequentialId()) {
        NColumnShard::Schema::SaveSpecialValue(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, *seqId);
    }
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalizer_finished")("description", GetNormalizer()->DebugString())(
        "id", GetNormalizer()->GetSequentialId());
    NColumnShard::Schema::FinishNormalizer(
        db, GetNormalizer()->GetClassName(), GetNormalizer()->GetUniqueDescription(), GetNormalizer()->GetUniqueId());
}

void TNormalizationController::InitNormalizers(const TInitContext& ctx) {
    Counters.clear();
    Normalizers.clear();
    if (HasAppData()) {
        for (auto&& i : AppDataVerified().ColumnShardConfig.GetRepairs()) {
            AFL_VERIFY(i.GetDescription())("error", "repair normalization have to has unique description");
            if (FinishedNormalizers.contains(TNormalizerFullId(i.GetClassName(), i.GetDescription()))) {
                AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("warning", "repair already processed")("description", i.GetDescription());
            } else {
                auto component = INormalizerComponent::TFactory::MakeHolder(i.GetClassName(), ctx);
                AFL_VERIFY(component)("class_name", i.GetClassName());
                auto normalizer = RegisterNormalizer(std::shared_ptr<INormalizerComponent>(component.Release()));
                normalizer->SetIsRepair(true).SetIsDryRun(i.GetDryRun()).SetUniqueDescription(i.GetDescription());
            }
        }
    }

    // We want to rerun all normalizers in case of binary rollback
    if (LastSavedNormalizerId && (ui32)ENormalizerSequentialId::MAX <= *LastSavedNormalizerId) {
        LastSavedNormalizerId = {};
    }

    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "normalization_start")("last_saved_id", LastSavedNormalizerId);
    auto normalizers = GetEnumAllValues<ENormalizerSequentialId>();
    auto lastRegisteredNormalizer = ENormalizerSequentialId::Granules;
    for (auto nType : normalizers) {
        if (LastSavedNormalizerId && (ui32)nType <= *LastSavedNormalizerId) {
            continue;
        }
        if (nType == ENormalizerSequentialId::MAX) {
            continue;
        }
        if (::ToString(nType).StartsWith("Deprecated")) {
            continue;
        }
        auto component = INormalizerComponent::TFactory::MakeHolder(::ToString(nType), ctx);
        AFL_VERIFY(component)("class_name", ::ToString(nType));
        auto normalizer = RegisterNormalizer(std::shared_ptr<INormalizerComponent>(component.Release()));
        AFL_VERIFY(normalizer->GetEnumSequentialIdVerified() == nType);
        AFL_VERIFY(lastRegisteredNormalizer <= nType)("current", ToString(nType))("last", ToString(lastRegisteredNormalizer));
        lastRegisteredNormalizer = nType;
    }

    for (auto&& i : Normalizers) {
        auto it = StartedNormalizers.find(i->GetNormalizerFullId());
        if (it != StartedNormalizers.end()) {
            i->SetUniqueId(it->second);
        }
        if (!i->GetUniqueDescription()) {
            i->SetUniqueDescription(i->GetClassName());
        }
    }
}

bool TNormalizationController::InitControllerState(NIceDb::TNiceDb& db) {
    {
        auto rowset = db.Table<NColumnShard::Schema::Normalizers>().Select();
        if (!rowset.IsReady()) {
            return false;
        }
        std::set<TNormalizerFullId> finished;
        std::map<TNormalizerFullId, TString> started;
        while (!rowset.EndOfSet()) {
            const TNormalizerFullId id(
                rowset.GetValue<NColumnShard::Schema::Normalizers::ClassName>(),
                rowset.GetValue<NColumnShard::Schema::Normalizers::Description>());
            if (!rowset.HaveValue<NColumnShard::Schema::Normalizers::Finish>()) {
                started.emplace(id, rowset.GetValue<NColumnShard::Schema::Normalizers::Identifier>());
            } else {
                finished.emplace(id);
            }
            if (!rowset.Next()) {
                return false;
            }
        }
        FinishedNormalizers = finished;
        StartedNormalizers = started;
    }

    std::optional<ui64> lastNormalizerId;
    if (!NColumnShard::Schema::GetSpecialValueOpt(db, NColumnShard::Schema::EValueIds::LastNormalizerSequentialId, lastNormalizerId)) {
        return false;
    }
    LastSavedNormalizerId = lastNormalizerId.value_or(0);
    return true;
}

NKikimr::TConclusion<std::vector<NKikimr::NOlap::INormalizerTask::TPtr>> TNormalizationController::INormalizerComponent::Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "normalizer_init")("last", controller.GetLastSavedNormalizerId())
        ("seq_id", GetSequentialId())("type", GetEnumSequentialId());
    auto result = DoInit(controller, txc);
    if (!result.IsSuccess()) {
        return result;
    }
    NIceDb::TNiceDb db(txc.DB);
    if (!controller.StartedNormalizers.contains(GetNormalizerFullId())) {
        NColumnShard::Schema::StartNormalizer(db, GetClassName(), GetUniqueDescription(), UniqueId);
    }
    AtomicSet(ActiveTasksCount, result.GetResult().size());
    return result;
}

}
