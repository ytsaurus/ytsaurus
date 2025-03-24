#pragma once

#include <contrib/ydb/core/scheme/scheme_tabledefs.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

struct TDbWatcherCallback : public TThrRefBase {
    virtual void OnDatabaseRemoved(const TString& database, TPathId pathId) = 0;
};

NActors::IActor* CreateDbWatcherActor(TIntrusivePtr<TDbWatcherCallback> callback);

}
}
