#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateTenantResolver(const TActorId& parent, ui64 rid, const TPathId& pathId);

}
