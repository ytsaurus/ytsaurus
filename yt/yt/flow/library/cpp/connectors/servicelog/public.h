#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TServiceLogRange);

DECLARE_REFCOUNTED_STRUCT(TTableFetcherSpec);

DECLARE_REFCOUNTED_STRUCT(TTableJoinerSpec);

DECLARE_REFCOUNTED_STRUCT(TServiceLogSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicServiceLogSpec);
DECLARE_REFCOUNTED_STRUCT(TParsedServiceLogDynamicPartitionSpec);

DECLARE_REFCOUNTED_STRUCT(TServiceLogControllerState);
DECLARE_REFCOUNTED_CLASS(TServiceLogSourceController);
DECLARE_REFCOUNTED_CLASS(TServiceLogSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
