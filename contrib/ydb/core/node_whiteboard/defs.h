#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/node_whiteboard/defs.h
#include <contrib/ydb/library/actors/core/defs.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/services/services.pb.h>
#include <contrib/ydb/core/debug/valgrind_check.h>
#include <util/generic/array_ref.h>
#include <util/generic/string.h>

namespace NKikimr {
    // actorlib is organic part of kikimr so we emulate global import by this directive
    using namespace NActors;
}
