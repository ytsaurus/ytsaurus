#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void BuildOperationAttributes(TOperationPtr operation, NYTree::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYTree::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYTree::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
