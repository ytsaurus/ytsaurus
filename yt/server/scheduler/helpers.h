#pragma once

#include "public.h"

#include <core/yson/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue);
Stroka TrimCommandForBriefSpec(const Stroka& command);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
