#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeLoggingNameService(INameService::TPtr origin);

} // namespace NSQLComplete
