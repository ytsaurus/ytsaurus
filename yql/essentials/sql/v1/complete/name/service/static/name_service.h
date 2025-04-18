#pragma once

#include "name_set.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>

namespace NSQLComplete {

    INameService::TPtr MakeStaticNameService();

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking);

} // namespace NSQLComplete
