#pragma once

#include "ranking.h"
#include "name_set.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeStaticNameService();

    INameService::TPtr MakeStaticNameService(TNameSet names, IRanking::TPtr ranking);

} // namespace NSQLComplete
