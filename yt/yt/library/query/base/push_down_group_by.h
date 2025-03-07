#pragma once

#include "public.h"
#include "ast.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

void TryPushDownGroupBy(const TQueryPtr& query, const NAst::TQuery& ast, const NLogging::TLogger& Logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

