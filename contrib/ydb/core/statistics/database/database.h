#pragma once

#include <contrib/ydb/core/scheme/scheme_pathid.h>
#include <contrib/ydb/core/protos/statistics.pb.h>
#include <contrib/ydb/core/statistics/events.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NStat {

NActors::IActor* CreateStatisticsTableCreator(std::unique_ptr<NActors::IEventBase> event, const TString& database);

NActors::IActor* CreateSaveStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId, std::vector<TStatisticsItem>&& items);

void DispatchLoadStatisticsQuery(
    const NActors::TActorId& replyActorId, ui64 queryId,
    const TString& database, const TPathId& pathId, EStatType statType, ui32 columnTag);

NActors::IActor* CreateDeleteStatisticsQuery(const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId);

};
