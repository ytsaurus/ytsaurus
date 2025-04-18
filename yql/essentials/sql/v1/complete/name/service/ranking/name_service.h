#pragma once

#include "ranking.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    INameService::TPtr MakeRankingNameService(INameService::TPtr source, IRanking::TPtr ranking);

} // namespace NSQLComplete
