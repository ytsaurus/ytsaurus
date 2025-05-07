#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeFallbackNameService(INameService::TPtr primary, INameService::TPtr standby);

    INameService::TPtr MakeEmptyNameService();

    INameService::TPtr MakeSwallowingNameService(INameService::TPtr origin);

} // namespace NSQLComplete
