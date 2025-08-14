#pragma once

#include <contrib/ydb/core/base/defs.h>

namespace NKikimr::NReplication::NController {

IActor* CreateSecretResolver(const TActorId& parent, ui64 rid, const TPathId& pathId, const TString& secretName, const ui64 cookie);

}
