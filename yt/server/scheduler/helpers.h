#pragma once

#include "public.h"

#include <core/yson/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
