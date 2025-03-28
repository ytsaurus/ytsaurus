#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql {

NActors::IActor* CreateDummyLock(
    const TString& lockName,
    const TString& lockAttributesYson);

} // namespace NYql

