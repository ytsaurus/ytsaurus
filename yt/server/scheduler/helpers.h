#pragma once

#include "public.h"

#include <core/yson/public.h>

#include <core/ytree/public.h>

#include <ytlib/object_client/public.h>
#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/hive/public.h>
#include <ytlib/hive/cluster_directory.h>

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

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode);

////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const TRefCountedJobResultPtr& result);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
