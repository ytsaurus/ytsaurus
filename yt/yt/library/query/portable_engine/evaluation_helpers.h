#pragma once

#include "public.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

namespace NYT::NQueryClient::NPortable {

////////////////////////////////////////////////////////////////////////////////

TCGExpressionImage CreateExpressionImage(TExpressionProgram program);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
