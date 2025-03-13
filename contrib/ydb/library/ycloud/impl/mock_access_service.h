#pragma once
#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud {

NActors::IActor* CreateMockAccessServiceWithCache(); // for compatibility with older code

}
