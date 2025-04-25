#pragma once

#include <yql/essentials/sql/v1/complete/name/name_service.h>

namespace NSQLComplete {

    THolder<INameService> MakeDeadlinedNameService(
        INameService::TPtr origin, TDuration timeout);

    THolder<INameService> MakeFallbackNameService(
        INameService::TPtr primary, INameService::TPtr standby);

} // namespace NSQLComplete
